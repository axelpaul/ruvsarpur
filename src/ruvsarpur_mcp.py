#!/usr/bin/env python
# coding=utf-8
"""
MCP (Model Context Protocol) server for ruvsarpur.

Lets an AI agent search the RÚV catalog, download shows, manage monitors
(series that are downloaded automatically), change download paths and read
statistics.

Usage:
  python ruvsarpur_mcp.py                       # stdio server (Claude Desktop, Claude Code, ...)
  python ruvsarpur_mcp.py --transport streamable-http --port 8765
  python ruvsarpur_mcp.py --run-monitors        # run all enabled monitors once and exit (cron)

Requires: pip install -r requirements.txt -r requirements-mcp.txt
"""

import argparse
import functools
import json
import os
import sys
from typing import Literal, Optional

# The ruvsarpur modules live next to this file
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _parse_cli():
  parser = argparse.ArgumentParser(description="MCP server for ruvsarpur")
  parser.add_argument("--config-dir", help="Directory for settings, catalog and download registry "
                                           "(default ~/.ruvsarpur or $RUVSARPUR_CONFIG_DIR)")
  parser.add_argument("--transport", choices=["stdio", "streamable-http", "sse"], default="stdio")
  parser.add_argument("--host", default="127.0.0.1", help="Host for the HTTP transports (default 127.0.0.1)")
  parser.add_argument("--port", type=int, default=8765, help="Port for the HTTP transports (default 8765)")
  parser.add_argument("--run-monitors", action="store_true",
                      help="Run all enabled monitors once, wait for the downloads and exit")
  parser.add_argument("--no-scheduler", action="store_true",
                      help="Do not run monitors automatically even if monitor_interval_minutes is set")
  return parser.parse_args()


CLI = _parse_cli() if __name__ == "__main__" else argparse.Namespace(
  config_dir=None, transport="stdio", host="127.0.0.1", port=8765, run_monitors=False, no_scheduler=False)

# Must be set before ruvsarpur is imported, it reads the config dir at import time
if CLI.config_dir:
  os.environ["RUVSARPUR_CONFIG_DIR"] = os.path.abspath(os.path.expanduser(CLI.config_dir))

import ruvmanager  # noqa: E402
from ruvmanager import ManagerError, RuvManager  # noqa: E402

manager = RuvManager()


def run_monitors_once():
  """Command line mode for cron / Windows Task Scheduler."""
  try:
    started = manager.run_monitors(source="cli")
  except ManagerError as ex:
    print(f"Nothing to do: {ex}")
    return 0
  job_ids = [j["job_id"] for j in started["jobs"]]
  for job in started["jobs"]:
    print(f"Queued {job['label']} ({job['job_id']})")
  manager.wait_for(job_ids)
  exit_code = 0
  for job_id in job_ids:
    job = manager.get_job(job_id, log_lines=0)
    downloaded = job.get("downloaded") or []
    print(f"{job['label']}: {job['status']}, {len(downloaded)} new download(s)")
    for entry in downloaded:
      print(f"  + {entry.get('title')} -> {entry.get('file')}")
    for error in job.get("errors") or []:
      print(f"  ! {error}")
    if job["status"] == "failed":
      exit_code = 1
  return exit_code


if __name__ == "__main__" and CLI.run_monitors:
  sys.exit(run_monitors_once())


# Only needed when running as a server
try:
  from mcp.server.mcpserver import MCPServer as _Server   # mcp >= 2
  from mcp.server.mcpserver.exceptions import ToolError
  MCP_V2 = True
except ImportError:
  try:
    from mcp.server.fastmcp import FastMCP as _Server    # mcp 1.x
    from mcp.server.fastmcp.exceptions import ToolError
    MCP_V2 = False
  except ImportError:
    sys.stderr.write("The 'mcp' package is required: pip install -r requirements-mcp.txt\n")
    raise

INSTRUCTIONS = """\
Manages ruvsarpur, a downloader for TV shows and movies from the Icelandic broadcaster RÚV (ruv.is).

Identifiers: every episode has a program id (pid) and belongs to a series with a series id (sid).
The catalog is a local cache of what RÚV currently offers, refresh it with refresh_catalog if it is stale.

Typical flows:
- Find something: search -> get_series / get_episode.
- One-time download: download(pids=[...]) or download(sids=[...]) returns a job_id immediately;
  downloads take minutes, poll get_job(job_id). Use dry_run=true first when downloading by query.
- Follow a show: add_monitor(sids=[...] or query=...), then run_monitors (or set
  monitor_interval_minutes with update_settings so it happens automatically).
- Paths: set_download_path changes the default folder or one monitor's folder. Existing files are not moved.
- Already downloaded episodes are skipped; use force=true or unmark_downloaded to fetch them again.
"""

mcp = _Server("ruvsarpur", instructions=INSTRUCTIONS)

Quality = Literal["Normal", "HD720", "HD1080"]
Kind = Literal["movie", "tvshow", "sport", "documentary"]


def tool(fn):
  """Registers a tool and turns ManagerErrors into error results the agent can read
  (other exceptions are hidden by the MCP SDK)."""
  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    try:
      return fn(*args, **kwargs)
    except ManagerError as ex:
      raise ToolError(str(ex)) from None
  return mcp.tool()(wrapper)


# ---------------------------------------------------------------- catalog

@tool
def search(query: str, include_english_subs: bool = False, new_only: bool = False,
           search_online: bool = True, limit: int = 20, episodes_per_series: int = 5) -> dict:
  """Search the RÚV catalog by title (Icelandic or original/foreign title, fuzzy match).

  Results are grouped by series with episode counts, how many are already downloaded and
  which monitors follow the series. search_online also asks RÚV for series missing from the
  local catalog. new_only limits results to series that just aired their first episode."""
  return manager.search(query, include_english_subs=include_english_subs, new_only=new_only,
                        online=search_online, limit=limit, episodes_per_series=episodes_per_series)


@tool
def browse_catalog(kind: Optional[Kind] = None, category: Optional[str] = None,
                   aired_within_days: Optional[int] = None, new_only: bool = False,
                   not_downloaded_only: bool = False, include_english_subs: bool = False,
                   limit: int = 30, episodes_per_series: int = 3) -> dict:
  """Browse the local catalog with filters: kind (movie, tvshow, sport, documentary), category
  name (see list_categories, partial match), recently aired, newly started series or only
  things not downloaded yet. Newest first, grouped by series."""
  return manager.browse(kind=kind, category=category, aired_within_days=aired_within_days,
                        new_only=new_only, not_downloaded_only=not_downloaded_only,
                        include_english_subs=include_english_subs, limit=limit,
                        episodes_per_series=episodes_per_series)


@tool
def list_categories() -> dict:
  """List the RÚV categories in the catalog with the number of series and episodes in each."""
  return manager.list_categories()


@tool
def get_series(sid: str, include_descriptions: bool = False) -> dict:
  """Get a series (or movie) by series id: description, IMDB match, poster and every available
  episode with its pid and whether it has been downloaded. Looks it up on RÚV if it is not in
  the local catalog."""
  return manager.get_series(sid, include_descriptions=include_descriptions)


@tool
def get_episode(pid: str) -> dict:
  """Get full details for one episode by program id, including description, subtitles and the
  file path it would be saved to with the current settings."""
  return manager.get_episode(pid)


@tool
def refresh_catalog(full: bool = False, wait_seconds: int = 0) -> dict:
  """Refresh the local catalog from RÚV. Incremental (default) only loads changed series and is
  fast. full=true reloads everything and can take several minutes. Runs as a job, pass
  wait_seconds to wait for it (max 600), otherwise poll get_job."""
  return manager.submit_refresh(full=full, wait_seconds=max(0, min(wait_seconds, 600)))


# -------------------------------------------------------------- downloads

@tool
def download(pids: Optional[list[str]] = None, sids: Optional[list[str]] = None,
             query: Optional[str] = None, output_path: Optional[str] = None,
             quality: Optional[Quality] = None, plex: Optional[bool] = None,
             original_title: Optional[bool] = None, include_english_subs: Optional[bool] = None,
             new_only: bool = False, force: bool = False, check_local: bool = False,
             metadata_only: bool = False, suffix: Optional[str] = None,
             dry_run: bool = False, allow_many: bool = False) -> dict:
  """One-time download of episodes. Give exactly one of pids (episodes), sids (every available
  episode of those series) or query (title search).

  Returns a job_id right away, the download runs in the background: poll get_job. Episodes
  already downloaded are skipped unless force=true. Options left empty use the settings
  (see get_settings): output_path, quality (Normal/HD720/HD1080), plex folder layout,
  original_title in file names. check_local marks files that already exist on disk as
  downloaded instead of downloading them. metadata_only fetches only posters/subtitles.
  dry_run=true lists what would be downloaded and where without downloading. Query downloads
  that would fetch many episodes are refused unless allow_many=true."""
  return manager.download(pids=pids, sids=sids, query=query, dry_run=dry_run, allow_many=allow_many,
                          output_path=output_path, quality=quality, plex=plex,
                          original_title=original_title, include_english_subs=include_english_subs,
                          new_only=new_only, force=force, check_local=check_local,
                          metadata_only=metadata_only, suffix=suffix)


@tool
def get_job(job_id: str, log_lines: int = 30) -> dict:
  """Status of a download, refresh or registry job: progress of the current episode, files
  downloaded so far, errors and the last lines of output."""
  return manager.get_job(job_id, log_lines=max(0, min(log_lines, 400)))


@tool
def list_jobs(status: Optional[Literal["active", "queued", "waiting", "running", "completed",
                                       "failed", "cancelled", "nothing_found"]] = None,
              limit: int = 20) -> dict:
  """List recent jobs, newest first. status='active' shows queued and running jobs."""
  return manager.list_jobs(status=status, limit=limit)


@tool
def cancel_job(job_id: str) -> dict:
  """Cancel a queued job, or stop a running download (a partial file may be left behind)."""
  return manager.cancel_job(job_id)


@tool
def get_download_history(limit: int = 25, query: Optional[str] = None,
                         monitor_id: Optional[str] = None, since_days: Optional[int] = None) -> dict:
  """Downloads made through this server, newest first, with file paths and sizes. Filter by
  title text, monitor or age in days."""
  return manager.get_history(limit=limit, query=query, monitor_id=monitor_id, since_days=since_days)


@tool
def mark_downloaded(pids: list[str]) -> dict:
  """Mark episodes as already downloaded so they are skipped by downloads and monitors."""
  return manager.update_registry(mark=pids)


@tool
def unmark_downloaded(pids: list[str]) -> dict:
  """Remove episodes from the downloaded registry so they will be downloaded again."""
  return manager.update_registry(unmark=pids)


# --------------------------------------------------------------- monitors

@tool
def list_monitors(include_pending_count: bool = True) -> dict:
  """List monitors (followed series / searches) with their settings, effective download path,
  last run result and how many episodes are waiting to be downloaded."""
  return manager.list_monitors(include_pending=include_pending_count)


@tool
def add_monitor(sids: Optional[list[str]] = None, query: Optional[str] = None,
                name: Optional[str] = None, output_path: Optional[str] = None,
                quality: Optional[Quality] = None, plex: Optional[bool] = None,
                original_title: Optional[bool] = None, include_english_subs: Optional[bool] = None,
                enabled: bool = True, notes: Optional[str] = None) -> dict:
  """Start following a show so new episodes are downloaded whenever monitors run.

  Use sids for one exact series run (season), or query to follow a title across new
  seasons and reruns (fuzzy title match, check it with search first). Options left empty
  follow the global settings, so changing the default download path also moves where this
  monitor saves, unless output_path is set here."""
  return manager.add_monitor(name=name, query=query, sids=sids, enabled=enabled, notes=notes,
                             output_path=output_path, quality=quality, plex=plex,
                             original_title=original_title, include_english_subs=include_english_subs)


@tool
def update_monitor(monitor_id: str, name: Optional[str] = None, sids: Optional[list[str]] = None,
                   query: Optional[str] = None, output_path: Optional[str] = None,
                   quality: Optional[Literal["Normal", "HD720", "HD1080", ""]] = None,
                   plex: Optional[bool] = None, original_title: Optional[bool] = None,
                   include_english_subs: Optional[bool] = None, enabled: Optional[bool] = None,
                   notes: Optional[str] = None) -> dict:
  """Change a monitor. Only the fields you pass change. Pass an empty string for output_path or
  quality to go back to the global setting. Setting sids replaces query and vice versa."""
  return manager.update_monitor(monitor_id, name=name, sids=sids, query=query, output_path=output_path,
                                quality=quality, plex=plex, original_title=original_title,
                                include_english_subs=include_english_subs, enabled=enabled, notes=notes)


@tool
def remove_monitor(monitor_id: str) -> dict:
  """Stop following a show. Downloaded files are kept."""
  return manager.remove_monitor(monitor_id)


@tool
def check_monitors(monitor_ids: Optional[list[str]] = None, search_online: bool = True) -> dict:
  """Preview, without downloading, which episodes each monitor would download next and where."""
  return manager.check_monitors(monitor_ids=monitor_ids, online=search_online)


@tool
def run_monitors(monitor_ids: Optional[list[str]] = None, refresh_catalog_first: Optional[bool] = None) -> dict:
  """Download new episodes for all enabled monitors (or the given ones, even if disabled).
  Refreshes the catalog first by default. Queues one job per monitor and returns their ids."""
  return manager.run_monitors(monitor_ids=monitor_ids, refresh=refresh_catalog_first)


# --------------------------------------------------------------- settings

@tool
def get_settings() -> dict:
  """Current settings: default download path, quality, plex mode, ffmpeg path, automatic
  monitor interval and more."""
  return {"settings": manager.get_settings(), "config_dir": manager.config_dir,
          "qualities": {k: f"{v['code']} kbps" for k, v in ruvmanager.ruvsarpur.QUALITY_BITRATE.items()}}


@tool
def update_settings(download_path: Optional[str] = None, quality: Optional[Quality] = None,
                    plex: Optional[bool] = None, original_title: Optional[bool] = None,
                    include_english_subs: Optional[bool] = None, no_metadata: Optional[bool] = None,
                    keep_partial: Optional[bool] = None, ffmpeg_path: Optional[str] = None,
                    imdb_folder: Optional[str] = None, monitor_interval_minutes: Optional[int] = None,
                    refresh_before_monitors: Optional[bool] = None,
                    max_query_matches: Optional[int] = None) -> dict:
  """Change global settings, only the fields you pass change.

  monitor_interval_minutes > 0 makes this server run all enabled monitors on that interval
  while it is running (0 turns it off). ffmpeg_path / imdb_folder accept an empty string to
  clear them. imdb_folder is a folder with IMDB's title.basics.tsv for better title matching."""
  settings = manager.update_settings(
    download_path=download_path, quality=quality, plex=plex, original_title=original_title,
    include_english_subs=include_english_subs, no_metadata=no_metadata, keep_partial=keep_partial,
    ffmpeg_path=ffmpeg_path, imdb_folder=imdb_folder, monitor_interval_minutes=monitor_interval_minutes,
    refresh_before_monitors=refresh_before_monitors, max_query_matches=max_query_matches)
  return {"settings": settings}


@tool
def set_download_path(path: str, monitor_id: Optional[str] = None, create: bool = True) -> dict:
  """Change where downloads are saved. Without monitor_id this changes the default path used by
  one-time downloads and every monitor without its own path; with monitor_id only that monitor
  changes. Files already downloaded are not moved."""
  return manager.set_download_path(path, monitor_id=monitor_id, create=create)


# ------------------------------------------------------------------ stats

@tool
def get_stats(include_library_size: bool = False) -> dict:
  """Overview: catalog size and age, downloads (totals, last 24h/7d/30d, top series), monitors,
  running and queued jobs, free disk space at the download path and whether ffmpeg is found.
  include_library_size walks the download folder to total up video files (slow on big libraries)."""
  return manager.stats(include_library_size=include_library_size)


def main():
  if not CLI.no_scheduler:
    manager.start_scheduler()
  try:
    if CLI.transport == "stdio":
      mcp.run()
    elif MCP_V2:
      mcp.run(CLI.transport, host=CLI.host, port=CLI.port)
    else:
      mcp.settings.host = CLI.host
      mcp.settings.port = CLI.port
      mcp.run(CLI.transport)
  finally:
    manager.shutdown()


if __name__ == "__main__":
  main()
