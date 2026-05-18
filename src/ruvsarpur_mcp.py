#!/usr/bin/env python
# coding=utf-8
"""
MCP server that exposes ruvsarpur to AI agents (e.g. OpenClaw, Claude).

Read-only operations import helpers from ruvsarpur.py directly so the
agent gets structured JSON instead of having to parse CLI stdout.
Downloads are long-running, so they are spawned as background subprocesses
of ruvsarpur.py and tracked by job id; the agent polls for progress
rather than blocking on ffmpeg.

Run:
    python ruvsarpur_mcp.py            # stdio transport (for local clients)

Requires:
    pip install "mcp[cli]"
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP

# Reuse ruvsarpur's own helpers. Importing the module runs no side effects
# beyond setting colorama lambdas, but we don't want its argparse to fire,
# so we make sure __name__ != '__main__' (it won't be when imported).
import ruvsarpur as ruv

mcp = FastMCP("ruvsarpur")

SCRIPT_PATH = Path(__file__).resolve().parent / "ruvsarpur.py"

# Server-managed state files, kept next to ruvsarpur's own logs so they
# move with --portable.
WATCHLIST_FILE = "mcp_watchlist.json"
JOB_HISTORY_FILE = "mcp_jobs.json"


# ---------------------------------------------------------------------------
# Schedule / config helpers (read side)
# ---------------------------------------------------------------------------

def _config_files(portable: bool) -> tuple[str, str]:
    return (
        ruv.createFullConfigFileName(portable, ruv.PREV_LOG_FILE),
        ruv.createFullConfigFileName(portable, ruv.TV_SCHEDULE_LOG_FILE),
    )


def _load_schedule(portable: bool) -> dict:
    _, tv_file = _config_files(portable)
    schedule = ruv.getExistingTvSchedule(tv_file)
    return schedule or {}


def _summarize(item: dict) -> dict:
    """Trim a schedule item down to fields useful for an agent."""
    keep = (
        "pid", "sid", "title", "series_title", "original-title",
        "ep_num", "ep_total", "showtime", "desc", "is_movie", "is_docu",
        "duration", "subtitles",
    )
    return {k: item.get(k) for k in keep if k in item}


def _state_path(name: str, portable: bool) -> str:
    return ruv.createFullConfigFileName(portable, name)


def _load_json(path: str, default: Any) -> Any:
    p = Path(path)
    if not p.is_file():
        return default
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _build_schedule_index(schedule: dict) -> dict[str, dict]:
    """pid -> summarized show, for cheap title/sid lookups."""
    out: dict[str, dict] = {}
    for key, item in schedule.items():
        if key == "date" or not isinstance(item, dict):
            continue
        pid = item.get("pid")
        if pid:
            out[pid] = _summarize(item)
    return out


def _fake_args(**overrides) -> argparse.Namespace:
    """Build the args namespace that ruvsarpur's search helper expects."""
    defaults = dict(
        sid=None, pid=None, find=None, new=False, includeenglishsubs=False,
        originaltitle=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------

@mcp.tool()
def schedule_status(portable: bool = False) -> dict:
    """Return last-refresh date and number of entries in the local schedule."""
    schedule = _load_schedule(portable)
    if not schedule:
        return {"loaded": False, "entries": 0, "last_refresh": None}
    last = schedule.get("date")
    return {
        "loaded": True,
        "entries": max(0, len(schedule) - 1),  # minus the 'date' key
        "last_refresh": last.isoformat() if hasattr(last, "isoformat") else last,
    }


@mcp.tool()
def search_shows(
    query: Optional[str] = None,
    sid: Optional[str] = None,
    pid: Optional[str] = None,
    only_new: bool = False,
    include_english_subs: bool = False,
    limit: int = 50,
    portable: bool = False,
) -> dict:
    """Search the locally-cached TV schedule.

    Provide one of: query (fuzzy title match), sid, or pid. Returns trimmed
    metadata suitable for an agent to reason over.
    """
    schedule = _load_schedule(portable)
    if not schedule:
        return {
            "results": [],
            "note": "No local schedule. Call refresh_schedule first.",
        }

    args = _fake_args(
        sid=[sid] if sid else None,
        pid=[pid] if pid else None,
        find=query,
        new=only_new,
        includeenglishsubs=include_english_subs,
    )
    matches = ruv.searchForItemsInTvSchedule(args, schedule)
    matches = sorted(matches, key=lambda x: x.get("showtime", ""), reverse=True)
    return {
        "count": len(matches),
        "truncated": len(matches) > limit,
        "results": [_summarize(m) for m in matches[:limit]],
    }


@mcp.tool()
def get_show(pid: Optional[str] = None, sid: Optional[str] = None,
             portable: bool = False) -> dict:
    """Return the full schedule entry for one show by pid (episode) or sid (series)."""
    if not pid and not sid:
        return {"error": "Provide pid or sid."}
    schedule = _load_schedule(portable)
    for key, item in schedule.items():
        if key == "date" or not isinstance(item, dict):
            continue
        if pid and item.get("pid") == pid:
            return {"item": item}
        if sid and item.get("sid") == sid:
            return {"item": item}
    return {"error": "Not found."}


@mcp.tool()
def list_downloaded(portable: bool = False, limit: int = 200) -> dict:
    """Return pids already recorded locally, enriched with titles from the schedule.

    Order follows prevrecorded.log (oldest first). Use `limit` to cap output.
    """
    rec_file, _ = _config_files(portable)
    pids = ruv.getPreviouslyRecordedShows(rec_file)
    index = _build_schedule_index(_load_schedule(portable))
    items = []
    for pid in pids:
        meta = index.get(pid)
        items.append({"pid": pid, **(meta or {"title": None})})
    truncated = len(items) > limit
    return {
        "count": len(items),
        "truncated": truncated,
        "log_path": rec_file,
        "items": items[-limit:] if truncated else items,
    }


@mcp.tool()
def get_config(portable: bool = False) -> dict:
    """Return where ruvsarpur keeps its config / log files and which qualities are known."""
    rec_file, tv_file = _config_files(portable)
    return {
        "version": getattr(ruv, "__version__", None),
        "prev_recorded_log": rec_file,
        "tv_schedule_log": tv_file,
        "qualities": list(ruv.QUALITY_BITRATE.keys()),
        "default_quality": "Normal",
    }


# ---------------------------------------------------------------------------
# Background job runner (write side)
# ---------------------------------------------------------------------------

class Job:
    __slots__ = ("id", "kind", "cmd", "meta", "proc", "status", "log",
                 "started_at", "ended_at", "portable")

    def __init__(self, kind: str, cmd: list[str], meta: dict, portable: bool):
        self.id = uuid.uuid4().hex[:8]
        self.kind = kind  # "download" | "refresh" | "watchlist"
        self.cmd = cmd
        self.meta = meta  # pid/sid/title/quality/output for downloads
        self.proc: Optional[subprocess.Popen] = None
        self.status = "pending"
        self.log: deque[str] = deque(maxlen=500)
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None
        self.portable = portable

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "meta": self.meta,
            "cmd": self.cmd,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "return_code": self.proc.returncode if self.proc else None,
            "tail": list(self.log)[-20:],
        }


_JOBS: dict[str, Job] = {}
_JOBS_LOCK = threading.Lock()

# Only one refresh may run at a time, and downloads must not run while a
# refresh is rewriting tvschedule.json underneath them. Held for the
# lifetime of a refresh job; downloads check it before starting.
_REFRESH_LOCK = threading.Lock()


def _refresh_in_progress() -> bool:
    if _REFRESH_LOCK.acquire(blocking=False):
        _REFRESH_LOCK.release()
        return False
    return True


def _persist_jobs(portable: bool) -> None:
    """Append a snapshot of finished/cancelled jobs to disk."""
    try:
        path = _state_path(JOB_HISTORY_FILE, portable)
        with _JOBS_LOCK:
            snapshot = [j.to_dict() for j in _JOBS.values()
                        if j.status in ("succeeded", "failed", "cancelled")]
        _save_json(path, snapshot)
    except Exception:
        pass  # persistence is best-effort


def _hydrate_jobs_from_disk(portable: bool) -> None:
    """Load previous-run history so list_jobs/recent_downloads survive restarts."""
    path = _state_path(JOB_HISTORY_FILE, portable)
    history = _load_json(path, [])
    with _JOBS_LOCK:
        for entry in history:
            if not isinstance(entry, dict) or "id" not in entry:
                continue
            if entry["id"] in _JOBS:
                continue
            j = Job(entry.get("kind", "download"), entry.get("cmd", []),
                    entry.get("meta", {}), portable)
            j.id = entry["id"]
            j.status = entry.get("status", "succeeded")
            j.started_at = entry.get("started_at")
            j.ended_at = entry.get("ended_at")
            for line in entry.get("tail", []):
                j.log.append(line)
            _JOBS[j.id] = j


def _spawn_job(kind: str, cmd: list[str], meta: dict, portable: bool) -> Job:
    job = Job(kind, cmd, meta, portable)
    with _JOBS_LOCK:
        _JOBS[job.id] = job

    def runner() -> None:
        # Refresh jobs serialize against each other and against any download
        # so the schedule file isn't read while it's being rewritten.
        if kind == "refresh":
            _REFRESH_LOCK.acquire()
        job.status = "running"
        job.started_at = time.time()
        try:
            job.proc = subprocess.Popen(
                cmd,
                cwd=str(SCRIPT_PATH.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert job.proc.stdout is not None
            for line in job.proc.stdout:
                job.log.append(line.rstrip())
            job.proc.wait()
            job.status = "succeeded" if job.proc.returncode == 0 else "failed"
        except Exception as ex:
            job.log.append(f"[exception] {ex}")
            job.status = "failed"
        finally:
            job.ended_at = time.time()
            if job.kind == "watchlist" and job.meta.get("watchlist_entry"):
                try:
                    items = _watchlist_load(portable)
                    for entry in items:
                        if entry.get("id") == job.meta["watchlist_entry"]:
                            entry["status"] = ("downloaded"
                                               if job.status == "succeeded"
                                               else "failed")
                            entry["finished_at"] = job.ended_at
                            break
                    _watchlist_save(items, portable)
                except Exception:
                    pass
            _persist_jobs(portable)
            if kind == "refresh":
                try:
                    _REFRESH_LOCK.release()
                except RuntimeError:
                    pass

    threading.Thread(target=runner, daemon=True).start()
    return job


def _build_download_cmd(
    pid: Optional[str], sid: Optional[str], quality: str,
    output: Optional[str], plex: bool, original_title: bool,
    suffix: Optional[str], force: bool, portable: bool,
    ffmpeg: Optional[str],
    keep_partial: bool = False, check_local: bool = False,
    no_metadata: bool = False, no_video: bool = False,
    include_english_subs: bool = False,
) -> list[str]:
    cmd: list[str] = [sys.executable, str(SCRIPT_PATH)]
    if pid:
        cmd += ["--pid", pid]
    if sid:
        cmd += ["--sid", sid]
    cmd += ["--quality", quality]
    if output:
        cmd += ["--output", output]
    if plex:
        cmd += ["--plex"]
    if original_title:
        cmd += ["--originaltitle"]
    if suffix:
        cmd += ["--suffix", suffix]
    if force:
        cmd += ["--force"]
    if portable:
        cmd += ["--portable"]
    if ffmpeg:
        cmd += ["--ffmpeg", ffmpeg]
    if keep_partial:
        cmd += ["--keeppartial"]
    if check_local:
        cmd += ["--checklocal"]
    if no_metadata:
        cmd += ["--nometadata"]
    if no_video:
        cmd += ["--novideo"]
    if include_english_subs:
        cmd += ["--includeenglishsubs"]
    return cmd


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------

@mcp.tool()
def refresh_schedule(incremental: bool = True, force: bool = False,
                     portable: bool = False) -> dict:
    """Refresh the TV schedule. Starts a background job; poll get_job_status.

    Refuses to start while another refresh is already running.
    """
    if _refresh_in_progress():
        return {"error": "A refresh is already in progress.",
                "refresh_in_progress": True}
    cmd = [sys.executable, str(SCRIPT_PATH), "--refresh", "--list"]
    if incremental:
        cmd.append("--incremental")
    if force:
        cmd.append("--force")
    if portable:
        cmd.append("--portable")
    meta = {"incremental": incremental, "force": force}
    job = _spawn_job("refresh", cmd, meta, portable)
    return {"job_id": job.id, "status": job.status}


@mcp.tool()
def start_download(
    pid: Optional[str] = None,
    sid: Optional[str] = None,
    quality: str = "Normal",
    output: Optional[str] = None,
    plex: bool = False,
    original_title: bool = False,
    suffix: Optional[str] = None,
    force: bool = False,
    portable: bool = False,
    ffmpeg: Optional[str] = None,
    keep_partial: bool = False,
    check_local: bool = False,
    no_metadata: bool = False,
    no_video: bool = False,
    include_english_subs: bool = False,
) -> dict:
    """Download one or more episodes. Returns a job id immediately.

    Provide pid (single episode) or sid (whole series). quality is one of
    Normal, HD720, HD1080. Refuses to start while a schedule refresh is in
    progress (the schedule file would be rewritten underneath the download).
    """
    if not pid and not sid:
        return {"error": "Provide pid or sid."}
    if quality not in ruv.QUALITY_BITRATE:
        return {
            "error": f"Unknown quality. Choose one of: {list(ruv.QUALITY_BITRATE)}",
        }
    if _refresh_in_progress():
        return {"error": "A schedule refresh is in progress; try again "
                         "after it finishes.",
                "refresh_in_progress": True}
    cmd = _build_download_cmd(
        pid, sid, quality, output, plex, original_title, suffix,
        force, portable, ffmpeg,
        keep_partial=keep_partial, check_local=check_local,
        no_metadata=no_metadata, no_video=no_video,
        include_english_subs=include_english_subs,
    )
    # Best-effort title resolution from the cached schedule for nicer history.
    index = _build_schedule_index(_load_schedule(portable))
    title = None
    if pid and pid in index:
        title = index[pid].get("title")
    meta = {"pid": pid, "sid": sid, "title": title, "quality": quality,
            "output": output, "plex": plex}
    job = _spawn_job("download", cmd, meta, portable)
    return {"job_id": job.id, "status": job.status, "cmd": cmd}


@mcp.tool()
def get_job_status(job_id: str) -> dict:
    """Return current status, return code, and recent log lines for a job."""
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if not job:
        return {"error": "Unknown job_id."}
    return job.to_dict()


@mcp.tool()
def list_jobs() -> dict:
    """Return all jobs the server is tracking (running and finished)."""
    with _JOBS_LOCK:
        return {"jobs": [j.to_dict() for j in _JOBS.values()]}


@mcp.tool()
def cancel_job(job_id: str) -> dict:
    """Terminate a running job (kills the spawned ruvsarpur subprocess)."""
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if not job:
        return {"error": "Unknown job_id."}
    if job.proc and job.proc.poll() is None:
        job.proc.terminate()
        try:
            job.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            job.proc.kill()
        job.status = "cancelled"
        _persist_jobs(job.portable)
    return job.to_dict()


@mcp.tool()
def recent_downloads(limit: int = 20, portable: bool = False) -> dict:
    """Most recent download/refresh jobs the server has tracked, newest first.

    Includes title, pid/sid, status, return code, output path and timestamps.
    Survives server restarts via mcp_jobs.json next to ruvsarpur's other logs.
    """
    _hydrate_jobs_from_disk(portable)
    with _JOBS_LOCK:
        jobs = [j.to_dict() for j in _JOBS.values()]
    jobs.sort(key=lambda j: j.get("ended_at") or j.get("started_at") or 0,
              reverse=True)
    return {"count": len(jobs), "items": jobs[:limit]}


# ---------------------------------------------------------------------------
# Watchlist (a server-side queue; ruvsarpur itself has no concept of one)
# ---------------------------------------------------------------------------

def _watchlist_load(portable: bool) -> list[dict]:
    return _load_json(_state_path(WATCHLIST_FILE, portable), [])


def _watchlist_save(items: list[dict], portable: bool) -> None:
    _save_json(_state_path(WATCHLIST_FILE, portable), items)


@mcp.tool()
def add_to_watchlist(
    pid: Optional[str] = None,
    sid: Optional[str] = None,
    quality: str = "Normal",
    output: Optional[str] = None,
    plex: bool = False,
    original_title: bool = False,
    suffix: Optional[str] = None,
    note: Optional[str] = None,
    portable: bool = False,
) -> dict:
    """Queue a show for later download without starting one now.

    Provide pid (episode) or sid (whole series). The agent can later call
    `download_watchlist()` to kick off everything still pending, or the
    user can hand off to the bare ruvsarpur CLI using the same options.
    """
    if not pid and not sid:
        return {"error": "Provide pid or sid."}
    if quality not in ruv.QUALITY_BITRATE:
        return {
            "error": f"Unknown quality. Choose one of: {list(ruv.QUALITY_BITRATE)}",
        }
    items = _watchlist_load(portable)
    if any((pid and i.get("pid") == pid) or (sid and i.get("sid") == sid)
           for i in items):
        return {"ok": True, "duplicate": True, "watchlist_size": len(items)}
    # Best-effort title resolution.
    index = _build_schedule_index(_load_schedule(portable))
    title = None
    if pid and pid in index:
        title = index[pid].get("title")
    entry = {
        "id": uuid.uuid4().hex[:8],
        "pid": pid,
        "sid": sid,
        "title": title,
        "quality": quality,
        "output": output,
        "plex": plex,
        "original_title": original_title,
        "suffix": suffix,
        "note": note,
        "added_at": time.time(),
        "status": "pending",
    }
    items.append(entry)
    _watchlist_save(items, portable)
    return {"ok": True, "entry": entry, "watchlist_size": len(items)}


@mcp.tool()
def list_watchlist(status: Optional[str] = None, portable: bool = False) -> dict:
    """Return the current watchlist. Optionally filter by status."""
    items = _watchlist_load(portable)
    if status:
        items = [i for i in items if i.get("status") == status]
    return {"count": len(items), "items": items}


@mcp.tool()
def remove_from_watchlist(entry_id: str, portable: bool = False) -> dict:
    """Drop one entry from the watchlist by its id."""
    items = _watchlist_load(portable)
    before = len(items)
    items = [i for i in items if i.get("id") != entry_id]
    _watchlist_save(items, portable)
    return {"removed": before - len(items), "watchlist_size": len(items)}


@mcp.tool()
def clear_watchlist(status: Optional[str] = "downloaded",
                    portable: bool = False) -> dict:
    """Drop all entries with the given status (default: only completed ones).
    Pass status=None to clear everything.
    """
    items = _watchlist_load(portable)
    before = len(items)
    if status is None:
        items = []
    else:
        items = [i for i in items if i.get("status") != status]
    _watchlist_save(items, portable)
    return {"removed": before - len(items), "watchlist_size": len(items)}


@mcp.tool()
def download_watchlist(
    portable: bool = False,
    ffmpeg: Optional[str] = None,
    force: bool = False,
) -> dict:
    """Start a download job for every pending watchlist entry.

    Each entry becomes its own background job (so they can be cancelled
    individually). Entries are marked as 'started' immediately; once the
    matching job ends with status 'succeeded' the entry's status is updated
    to 'downloaded' (poll list_watchlist after jobs finish).
    """
    if _refresh_in_progress():
        return {"error": "A schedule refresh is in progress; try again "
                         "after it finishes.",
                "refresh_in_progress": True, "started": 0, "jobs": []}
    items = _watchlist_load(portable)
    started: list[dict] = []
    for entry in items:
        if entry.get("status") != "pending":
            continue
        cmd = _build_download_cmd(
            entry.get("pid"), entry.get("sid"), entry.get("quality", "Normal"),
            entry.get("output"), entry.get("plex", False),
            entry.get("original_title", False), entry.get("suffix"),
            force, portable, ffmpeg,
        )
        meta = {
            "watchlist_entry": entry["id"],
            "pid": entry.get("pid"),
            "sid": entry.get("sid"),
            "title": entry.get("title"),
            "quality": entry.get("quality"),
            "output": entry.get("output"),
        }
        job = _spawn_job("watchlist", cmd, meta, portable)
        entry["status"] = "started"
        entry["last_job_id"] = job.id
        started.append({"entry_id": entry["id"], "job_id": job.id,
                        "pid": entry.get("pid"), "sid": entry.get("sid")})
    _watchlist_save(items, portable)
    return {"started": len(started), "jobs": started}


@mcp.tool()
def convert_vtt_to_srt(vtt_path: str, srt_path: Optional[str] = None) -> dict:
    """Convert a WebVTT/VTT subtitle file to SRT via webvtttosrt.py."""
    converter = SCRIPT_PATH.parent / "webvtttosrt.py"
    cmd = [sys.executable, str(converter), "--input", vtt_path]
    if srt_path:
        cmd += ["--output", srt_path]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "return_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "output_path": srt_path,
    }


if __name__ == "__main__":
    # Best-effort: load previous job history from the default (non-portable)
    # location so list_jobs/recent_downloads aren't empty on startup.
    try:
        _hydrate_jobs_from_disk(portable=False)
    except Exception:
        pass
    mcp.run()
