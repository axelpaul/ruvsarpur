#!/usr/bin/env python
# coding=utf-8
"""
Management layer for ruvsarpur.

Wraps the ruvsarpur.py command line tool with a programmatic API that is used
by the MCP server (ruvsarpur_mcp.py), but has no dependency on MCP itself.

It provides:
  * Persistent settings (default download path, quality, plex mode, ...)
  * Monitors: saved series ids or search terms whose new episodes are
    downloaded automatically when the monitors are run
  * A serial job queue that runs downloads through ruvsarpur.py in a
    subprocess, with progress tracking, logs and cancellation
  * Download history and statistics
  * Read only catalog queries (search, series and episode details)

All state lives in the ruvsarpur config directory (~/.ruvsarpur by default,
override with the RUVSARPUR_CONFIG_DIR environment variable) next to the files
the command line tool already uses.
"""

import argparse
import collections
import contextlib
import copy
import datetime
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import ruvsarpur

SCRIPT_PATH = str(Path(__file__).resolve().parent / "ruvsarpur.py")

SETTINGS_FILE = "mcp-settings.json"
MONITORS_FILE = "monitors.json"
HISTORY_FILE = "download-history.json"
JOBS_FILE = "mcp-jobs.json"
LOCK_FILE = "ruvsarpur.lock"

QUALITIES = list(ruvsarpur.QUALITY_BITRATE.keys())

DEFAULT_SETTINGS = {
  # Where downloads go when neither the request nor the monitor specifies a path
  "download_path": os.path.join(os.path.expanduser("~"), "Videos", "RUV"),
  "quality": "HD1080",
  "plex": False,
  "original_title": False,
  "include_english_subs": False,
  "no_metadata": False,
  "keep_partial": False,
  "ffmpeg_path": None,
  "imdb_folder": None,
  # When > 0 the MCP server runs all enabled monitors on this interval while it is running
  "monitor_interval_minutes": 0,
  # Refresh the catalog (incrementally) before monitors are run
  "refresh_before_monitors": True,
  # A query download that would fetch more than this many new episodes must be confirmed
  "max_query_matches": 25,
}

# Settings the caller is allowed to change and their expected types
SETTING_TYPES = {
  "download_path": (str,),
  "quality": (str,),
  "plex": (bool,),
  "original_title": (bool,),
  "include_english_subs": (bool,),
  "no_metadata": (bool,),
  "keep_partial": (bool,),
  "ffmpeg_path": (str, type(None)),
  "imdb_folder": (str, type(None)),
  "monitor_interval_minutes": (int,),
  "refresh_before_monitors": (bool,),
  "max_query_matches": (int,),
}

# Options a monitor may override, None means "inherit from settings"
MONITOR_OPTIONS = ["output_path", "quality", "plex", "original_title", "include_english_subs"]

ACTIVE_STATES = ("queued", "waiting", "running")
FINISHED_STATES = ("completed", "failed", "cancelled", "nothing_found")

RE_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RE_FOUND = re.compile(r"^Found (\d+) show")
RE_ITEM = re.compile(r"^(\d+) of (\d+): (.*?) \| Estimated: (\d+) MB")
RE_PERCENT = re.compile(r"Downloading:\s*\|[=\-]*\|\s*([\d.]+)%")
RE_COMPLETE = re.compile(r"Complete -> (.+)$")
RE_ALREADY = re.compile(r"already recorded \(pid=([^)]+)\)")
RE_FOUND_LOCAL = re.compile(r"found locally and marked as already recorded \(pid=([^)]+)\)")
RE_NOTHING = re.compile(r"^Nothing found to download")
RE_EXCEPTION = re.compile(r"^[A-Za-z_][\w.]*(Error|Exception|Interrupt)\b.*")


class ManagerError(Exception):
  """An error caused by invalid input or state, safe to show to the caller."""


def _now():
  return datetime.datetime.now().astimezone()


def _iso(dt=None):
  return (dt or _now()).isoformat(timespec="seconds")


def _parse_iso(value):
  try:
    return datetime.datetime.fromisoformat(value)
  except (TypeError, ValueError):
    return None


def _norm_path(path):
  return os.path.abspath(os.path.expanduser(os.path.expandvars(str(path))))


def _clean_query(query):
  # The query ends up inside a JSON string in a GraphQL url, quotes and
  # backslashes would break it
  query = re.sub(r'["\\\n\r\t]', " ", str(query or "")).strip()
  return re.sub(r"\s+", " ", query)


def _clean_ids(ids, name):
  if ids is None:
    return None
  if isinstance(ids, (str, int)):
    ids = [ids]
  cleaned = []
  for value in ids:
    value = str(value).strip()
    if not value:
      continue
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", value):
      raise ManagerError(f"Invalid {name} '{value}', ids contain only letters, digits, '-' and '_'")
    cleaned.append(value)
  return cleaned or None


def _slugify(text):
  text = str(text).lower()
  for a, b in (("á", "a"), ("ð", "d"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
               ("ý", "y"), ("þ", "th"), ("æ", "ae"), ("ö", "o")):
    text = text.replace(a, b)
  text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
  return text[:40] or "monitor"


def _human_bytes(num):
  if num is None:
    return None
  for unit in ("B", "KB", "MB", "GB", "TB"):
    if abs(num) < 1024 or unit == "TB":
      return f"{num:.1f} {unit}" if unit != "B" else f"{int(num)} B"
    num /= 1024.0


# Serialises swapping sys.stdout so concurrent callers restore it correctly.
_quiet_lock = threading.Lock()
_quiet_depth = 0
_quiet_saved = None


@contextlib.contextmanager
def quiet():
  """Sends anything ruvsarpur prints to stderr.

  ruvsarpur.py prints progress bars and messages to stdout, which would corrupt
  the MCP stdio protocol stream.
  """
  global _quiet_depth, _quiet_saved
  with _quiet_lock:
    if _quiet_depth == 0:
      _quiet_saved = sys.stdout
      sys.stdout = sys.stderr
    _quiet_depth += 1
  try:
    yield
  finally:
    with _quiet_lock:
      _quiet_depth -= 1
      if _quiet_depth == 0:
        sys.stdout = _quiet_saved


class FileLock:
  """Cross-process lock so MCP jobs and cron runs never download at the same time.

  ruvsarpur.py rewrites the download registry and the catalog from memory, so
  two concurrent runs would overwrite each other's changes.
  """

  def __init__(self, path):
    self.path = path
    self._fh = None

  def try_acquire(self):
    os.makedirs(os.path.dirname(self.path), exist_ok=True)
    fh = open(self.path, "a+")
    try:
      if os.name == "nt":
        import msvcrt
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
      else:
        import fcntl
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
      fh.close()
      return False
    self._fh = fh
    return True

  def release(self):
    if self._fh is None:
      return
    try:
      if os.name == "nt":
        import msvcrt
        self._fh.seek(0)
        msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
      else:
        import fcntl
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
    finally:
      self._fh.close()
      self._fh = None


class Job:
  def __init__(self, kind, params, label, monitor_id=None, run_id=None):
    self.id = uuid.uuid4().hex[:10]
    self.kind = kind
    self.params = params
    self.label = label
    self.monitor_id = monitor_id
    self.run_id = run_id
    self.status = "queued"
    self.status_detail = None
    self.created_at = _iso()
    self.started_at = None
    self.finished_at = None
    self.returncode = None
    self.items_found = None
    self.current_index = None
    self.current_title = None
    self.current_estimated_mb = None
    self.current_percent = None
    self.downloaded = []      # history entries for new downloads
    self.skipped_pids = []    # already downloaded
    self.completed_files = []
    self.errors = []
    self.result = None        # free form result for non-download jobs
    self.log = collections.deque(maxlen=400)
    self.cancel_requested = False
    self.proc = None
    self.done = threading.Event()

  def to_dict(self, log_lines=0):
    data = {
      "job_id": self.id,
      "kind": self.kind,
      "label": self.label,
      "status": self.status,
      "status_detail": self.status_detail,
      "monitor_id": self.monitor_id,
      "run_id": self.run_id,
      "params": self.params,
      "created_at": self.created_at,
      "started_at": self.started_at,
      "finished_at": self.finished_at,
    }
    if self.kind == "download":
      data["progress"] = {
        "items_found": self.items_found,
        "current_item": self.current_index,
        "current_title": self.current_title,
        "current_estimated_mb": self.current_estimated_mb,
        "current_percent": self.current_percent,
        "downloaded_count": len(self.downloaded),
        "skipped_already_downloaded": len(self.skipped_pids),
      }
      data["downloaded"] = self.downloaded
      data["returncode"] = self.returncode
    if self.result is not None:
      data["result"] = self.result
    if self.errors:
      data["errors"] = self.errors[-20:]
    if log_lines:
      data["log_tail"] = list(self.log)[-log_lines:]
    return data

  @classmethod
  def from_dict(cls, data):
    job = cls(data.get("kind"), data.get("params") or {}, data.get("label"),
              data.get("monitor_id"), data.get("run_id"))
    job.id = data.get("job_id", job.id)
    for attr in ("status", "status_detail", "created_at", "started_at", "finished_at", "returncode", "result"):
      setattr(job, attr, data.get(attr))
    progress = data.get("progress") or {}
    job.items_found = progress.get("items_found")
    job.downloaded = data.get("downloaded") or []
    job.errors = data.get("errors") or []
    job.done.set()
    return job


class RuvManager:
  def __init__(self, config_dir=None):
    self.config_dir = _norm_path(config_dir or ruvsarpur.LOG_DIR)
    os.makedirs(self.config_dir, exist_ok=True)
    self._lock = threading.RLock()
    self._jobs = collections.OrderedDict()
    self._queue = collections.deque()
    self._queue_cv = threading.Condition(self._lock)
    self._worker = None
    self._scheduler = None
    self._stop = threading.Event()
    self._schedule_cache = None
    self._schedule_mtime = None
    self._online_cache = {}   # pid -> episode, series fetched from RUV that are not in the catalog file
    self.run_lock = FileLock(self._path(LOCK_FILE))
    self._load_jobs()

  # ------------------------------------------------------------------ files

  def _path(self, name):
    return os.path.join(self.config_dir, name)

  def _read_json(self, name, default):
    try:
      with open(self._path(name), "r", encoding="utf-8") as fh:
        return json.load(fh)
    except FileNotFoundError:
      return copy.deepcopy(default)
    except (OSError, ValueError) as ex:
      raise ManagerError(f"Could not read {self._path(name)}: {ex}")

  def _write_json(self, name, data):
    path = self._path(name)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
      json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

  # --------------------------------------------------------------- settings

  def get_settings(self):
    with self._lock:
      stored = self._read_json(SETTINGS_FILE, {})
    settings = dict(DEFAULT_SETTINGS)
    settings.update({k: v for k, v in stored.items() if k in DEFAULT_SETTINGS})
    return settings

  def update_settings(self, **changes):
    changes = {k: v for k, v in changes.items() if v is not None}
    clear = [k for k, v in changes.items() if v == "" and k in ("ffmpeg_path", "imdb_folder")]
    for key in clear:
      changes[key] = None
    for key, value in changes.items():
      if key not in SETTING_TYPES:
        raise ManagerError(f"Unknown setting '{key}'")
      if not isinstance(value, SETTING_TYPES[key]) or (isinstance(value, bool) and int in SETTING_TYPES[key]):
        raise ManagerError(f"Setting '{key}' has the wrong type")
    if "quality" in changes and changes["quality"] not in QUALITIES:
      raise ManagerError(f"Quality must be one of {QUALITIES}")
    if "monitor_interval_minutes" in changes and changes["monitor_interval_minutes"] < 0:
      raise ManagerError("monitor_interval_minutes must be 0 (off) or a positive number of minutes")
    if "max_query_matches" in changes and changes["max_query_matches"] < 1:
      raise ManagerError("max_query_matches must be at least 1")
    if "download_path" in changes and not str(changes["download_path"]).strip():
      raise ManagerError("download_path must not be empty")
    for key in ("download_path", "ffmpeg_path", "imdb_folder"):
      if changes.get(key):
        changes[key] = _norm_path(changes[key])
    if changes.get("ffmpeg_path") and not os.path.isfile(changes["ffmpeg_path"]):
      raise ManagerError(f"ffmpeg executable not found at {changes['ffmpeg_path']}")
    with self._lock:
      stored = self._read_json(SETTINGS_FILE, {})
      stored.update(changes)
      self._write_json(SETTINGS_FILE, stored)
    return self.get_settings()

  def set_download_path(self, path, monitor_id=None, create=True):
    path = _norm_path(path)
    if os.path.exists(path) and not os.path.isdir(path):
      raise ManagerError(f"{path} exists and is not a directory")
    if create:
      os.makedirs(path, exist_ok=True)
    if monitor_id:
      monitor = self.update_monitor(monitor_id, output_path=path)
      return {"scope": "monitor", "monitor_id": monitor_id, "download_path": path,
              "exists": os.path.isdir(path), "monitor": monitor}
    self.update_settings(download_path=path)
    with self._lock:
      inheriting = [m["id"] for m in self._monitors() if not m.get("output_path")]
    return {"scope": "default", "download_path": path, "exists": os.path.isdir(path),
            "monitors_using_default_path": inheriting}

  # ---------------------------------------------------------------- catalog

  def schedule_file(self):
    return self._path(ruvsarpur.TV_SCHEDULE_LOG_FILE)

  def registry_file(self):
    return self._path(ruvsarpur.PREV_LOG_FILE)

  def _schedule(self):
    """The catalog file merged with any series fetched online, keyed by pid."""
    path = self.schedule_file()
    try:
      mtime = os.path.getmtime(path)
    except OSError:
      mtime = None
    with self._lock:
      if mtime != self._schedule_mtime:
        data = {}
        if mtime is not None:
          try:
            with open(path, "r", encoding="utf-8") as fh:
              data = json.load(fh)
          except (OSError, ValueError):
            data = {}
        self._schedule_cache = data
        self._schedule_mtime = mtime
      merged = dict(self._online_cache)
      merged.update(self._schedule_cache or {})
      return merged

  def _episodes(self, schedule=None):
    schedule = schedule if schedule is not None else self._schedule()
    return {k: v for k, v in schedule.items() if isinstance(v, dict) and "pid" in v}

  def catalog_date(self):
    with self._lock:
      self._schedule()
      return (self._schedule_cache or {}).get("date")

  def recorded_pids(self):
    return set(ruvsarpur.getPreviouslyRecordedShows(self.registry_file()))

  def _fetch_series_online(self, sid):
    with quiet():
      try:
        episodes = ruvsarpur.getVodSeriesSchedule(sid, None, {}, None)
      except Exception:
        episodes = {}
    if episodes:
      with self._lock:
        self._online_cache.update(episodes)
    return episodes

  def _search_online(self, query):
    with quiet():
      try:
        results = ruvsarpur.getVodSearchResults(query) or []
      except Exception:
        results = []
    return [str(r["id"]) for r in results if isinstance(r, dict) and r.get("id")]

  def find_episodes(self, query=None, sids=None, pids=None, new_only=False,
                    include_english_subs=False, online=True):
    """Same matching rules as the command line tool, returns schedule items."""
    if query:
      query = _clean_query(query)
    schedule = self._schedule()
    if online and (query or sids):
      known_sids = {str(v.get("sid")) for v in self._episodes(schedule).values()}
      lookup = list(sids or [])
      if query:
        lookup.extend(self._search_online(query))
      fetched = False
      for sid in dict.fromkeys(lookup):
        if sid not in known_sids and self._fetch_series_online(sid):
          fetched = True
      if fetched:
        schedule = self._schedule()
    args = argparse.Namespace(sid=sids, pid=pids, find=query, new=new_only,
                              includeenglishsubs=include_english_subs, originaltitle=False)
    with quiet():
      items = ruvsarpur.searchForItemsInTvSchedule(args, self._episodes(schedule))
    return sorted(items, key=lambda i: (str(i.get("showtime") or ""), str(i.get("pid"))), reverse=True)

  @staticmethod
  def _kind(item):
    if item.get("is_docu"):
      return "documentary"
    if item.get("is_movie"):
      return "movie"
    if item.get("is_sport"):
      return "sport"
    return "tvshow"

  def episode_summary(self, item, recorded=None, description=False):
    recorded = recorded if recorded is not None else self.recorded_pids()
    data = {
      "pid": item.get("pid"),
      "sid": item.get("sid"),
      "title": item.get("title"),
      "series_title": item.get("series_title"),
      "original_title": item.get("original-title"),
      "episode_title": item.get("episode_title") or None,
      "episode": item.get("ep_num"),
      "episodes_total": item.get("ep_total"),
      "season": item.get("season_num"),
      "showtime": item.get("showtime"),
      "duration": item.get("duration_friendly"),
      "kind": self._kind(item),
      "categories": item.get("categories"),
      "has_subtitles": bool(item.get("subtitles")),
      "english_subtitled": bool(item.get("english_subtitled")),
      "downloaded": item.get("pid") in recorded,
    }
    if description:
      data["description"] = item.get("desc")
    return data

  def _group_by_series(self, items, recorded, episodes_per_series):
    groups = collections.OrderedDict()
    for item in items:
      sid = str(item.get("sid"))
      group = groups.get(sid)
      if group is None:
        group = groups[sid] = {
          "sid": sid,
          "series_title": item.get("series_title"),
          "original_title": item.get("original-title"),
          "kind": self._kind(item),
          "categories": item.get("categories"),
          "episodes_available": 0,
          "episodes_downloaded": 0,
          "latest_showtime": item.get("showtime"),
          "episodes": [],
        }
      group["episodes_available"] += 1
      if item.get("pid") in recorded:
        group["episodes_downloaded"] += 1
      if len(group["episodes"]) < episodes_per_series:
        group["episodes"].append(self.episode_summary(item, recorded))
    monitored = self._monitored_index()
    for group in groups.values():
      group["monitored_by"] = monitored.get(group["sid"], [])
    return list(groups.values())

  def _monitored_index(self):
    """sid -> monitor ids that explicitly follow that series."""
    index = collections.defaultdict(list)
    for monitor in self._monitors():
      for sid in monitor.get("sids") or []:
        index[str(sid)].append(monitor["id"])
    return index

  def search(self, query, include_english_subs=False, new_only=False, online=True,
             limit=20, episodes_per_series=5):
    query = _clean_query(query)
    if not query:
      raise ManagerError("query must not be empty")
    items = self.find_episodes(query=query, new_only=new_only,
                               include_english_subs=include_english_subs, online=online)
    recorded = self.recorded_pids()
    groups = self._group_by_series(items, recorded, episodes_per_series)
    return {
      "query": query,
      "catalog_date": self.catalog_date(),
      "series_found": len(groups),
      "episodes_found": len(items),
      "series": groups[:limit],
      "truncated": len(groups) > limit,
    }

  def browse(self, kind=None, category=None, aired_within_days=None, new_only=False,
             not_downloaded_only=False, include_english_subs=False, limit=30, episodes_per_series=3):
    if kind and kind not in ("movie", "tvshow", "sport", "documentary"):
      raise ManagerError("kind must be one of movie, tvshow, sport, documentary")
    items = self.find_episodes(new_only=new_only, include_english_subs=include_english_subs, online=False)
    recorded = self.recorded_pids()
    cutoff = None
    if aired_within_days:
      cutoff = (datetime.datetime.now() - datetime.timedelta(days=aired_within_days)).strftime("%Y-%m-%d")
    filtered = []
    for item in items:
      if kind and self._kind(item) != kind:
        continue
      if category and not any(category.lower() in str(c).lower() for c in item.get("categories") or []):
        continue
      if cutoff and str(item.get("showtime") or "")[:10] < cutoff:
        continue
      if not_downloaded_only and item.get("pid") in recorded:
        continue
      filtered.append(item)
    groups = self._group_by_series(filtered, recorded, episodes_per_series)
    return {
      "catalog_date": self.catalog_date(),
      "series_found": len(groups),
      "episodes_found": len(filtered),
      "series": groups[:limit],
      "truncated": len(groups) > limit,
    }

  def list_categories(self):
    counts = collections.Counter()
    series = collections.defaultdict(set)
    for item in self._episodes().values():
      for cat in item.get("categories") or []:
        counts[cat] += 1
        series[cat].add(item.get("sid"))
    return {
      "catalog_date": self.catalog_date(),
      "categories": [{"name": c, "series": len(series[c]), "episodes": n} for c, n in counts.most_common()],
    }

  def get_series(self, sid, online=True, include_descriptions=False):
    sid = _clean_ids(sid, "sid")[0]
    items = [i for i in self._episodes().values() if str(i.get("sid")) == sid]
    if not items and online:
      items = list(self._fetch_series_online(sid).values())
    if not items:
      raise ManagerError(f"Series {sid} was not found in the catalog or on RÚV")
    recorded = self.recorded_pids()

    def sort_key(item):
      try:
        num = int(item.get("ep_num") or 0)
      except ValueError:
        num = 0
      return (str(item.get("season_num") or ""), num, str(item.get("showtime") or ""))

    items.sort(key=sort_key)
    first = items[0]
    return {
      "sid": sid,
      "series_title": first.get("series_title"),
      "original_title": first.get("original-title"),
      "kind": self._kind(first),
      "categories": first.get("categories"),
      "description": first.get("series_sdesc") or first.get("series_desc"),
      "imdb": first.get("imdb"),
      "poster": first.get("portrait_image") or first.get("series_image"),
      "in_catalog_file": any(i.get("pid") in (self._schedule_cache or {}) for i in items),
      "episodes_available": len(items),
      "episodes_downloaded": sum(1 for i in items if i.get("pid") in recorded),
      "monitored_by": self._monitored_index().get(sid, []),
      "episodes": [self.episode_summary(i, recorded, include_descriptions) for i in items],
    }

  def get_episode(self, pid):
    pid = _clean_ids(pid, "pid")[0]
    item = self._episodes().get(pid)
    if item is None:
      raise ManagerError(f"Episode {pid} is not in the catalog, search for it or refresh the catalog first")
    data = self.episode_summary(item, description=True)
    data.update({
      "series_description": item.get("series_sdesc") or item.get("series_desc"),
      "subtitle_languages": [s.get("name") for s in item.get("subtitles") or []],
      "rating": item.get("rating"),
      "imdb": item.get("imdb"),
      "episode_image": item.get("episode_image"),
      "poster": item.get("portrait_image") or item.get("series_image"),
      "planned_file": self.planned_file(item, self.get_settings()),
    })
    return data

  def planned_file(self, item, options):
    with quiet():
      name = ruvsarpur.createLocalFileName(item, options.get("original_title"), options.get("plex"),
                                           options.get("suffix") or "")
    path = os.path.join(options.get("output_path") or options["download_path"], name)
    if not path.endswith(".mp4") and not options.get("no_metadata"):
      path += ".mp4"
    return path

  def refresh_catalog(self, full=False, job=None):
    """Downloads the RÚV catalog, must run while holding the run lock (i.e. as a job)."""
    settings = self.get_settings()
    path = self.schedule_file()
    with quiet():
      existing = ruvsarpur.getExistingTvSchedule(path)
      if job:
        job.status_detail = "loading IMDB titles" if settings.get("imdb_folder") else "downloading catalog"
      imdb_titles = ruvsarpur.loadImdbOriginalTitles(settings.get("imdb_folder")) if settings.get("imdb_folder") else {}
      imdb_cache_path = self._path(ruvsarpur.IMDB_CACHE_FILE)
      imdb_cache = ruvsarpur.getExistingJsonFile(imdb_cache_path) or {}
      incremental = not full and existing is not None
      schedule = {} if not incremental else existing
      if job:
        job.status_detail = "downloading catalog" + (" (incremental)" if incremental else " (full)")
      schedule = ruvsarpur.getVodSchedule(schedule, incremental and len(schedule) > 0, imdb_cache, imdb_titles)
      if len(schedule) > 1:
        ruvsarpur.saveCurrentTvSchedule(schedule, path)
      if imdb_cache:
        ruvsarpur.saveImdbCache(imdb_cache, imdb_cache_path)
    with self._lock:
      self._online_cache.clear()
    episodes = self._episodes()
    return {
      "mode": "incremental" if incremental else "full",
      "catalog_date": self.catalog_date(),
      "episodes": len(episodes),
      "series": len({e.get("sid") for e in episodes.values()}),
    }

  # --------------------------------------------------------------- monitors

  def _monitors(self):
    return self._read_json(MONITORS_FILE, {"monitors": []}).get("monitors", [])

  def _monitor_state(self):
    return self._read_json(MONITORS_FILE, {"monitors": []})

  def _save_monitors(self, monitors, **extra):
    state = self._monitor_state()
    state["monitors"] = monitors
    state.update(extra)
    self._write_json(MONITORS_FILE, state)

  def list_monitors(self, include_pending=False):
    with self._lock:
      monitors = self._monitors()
      state = self._monitor_state()
    settings = self.get_settings()
    result = []
    for monitor in monitors:
      entry = dict(monitor)
      entry["effective"] = self._monitor_options(monitor, settings)
      if include_pending:
        try:
          pending = self.monitor_pending(monitor, online=False)
          entry["pending_count"] = len(pending)
        except ManagerError:
          entry["pending_count"] = None
      result.append(entry)
    return {
      "monitors": result,
      "last_run_at": state.get("last_run_at"),
      "auto_run_interval_minutes": settings.get("monitor_interval_minutes"),
    }

  def get_monitor(self, monitor_id):
    for monitor in self._monitors():
      if monitor["id"] == monitor_id:
        return monitor
    raise ManagerError(f"No monitor with id '{monitor_id}', use list_monitors to see the ids")

  @staticmethod
  def _validate_monitor_options(options):
    if options.get("quality") is not None and options["quality"] not in QUALITIES:
      raise ManagerError(f"quality must be one of {QUALITIES}")
    if options.get("output_path"):
      options["output_path"] = _norm_path(options["output_path"])
    return options

  def add_monitor(self, name=None, query=None, sids=None, enabled=True, notes=None, **options):
    sids = _clean_ids(sids, "sid")
    query = _clean_query(query) if query else None
    if bool(query) == bool(sids):
      raise ManagerError("Provide exactly one of 'query' (a title search) or 'sids' (series ids)")
    options = self._validate_monitor_options({k: options.get(k) for k in MONITOR_OPTIONS})
    if not name:
      if query:
        name = query
      else:
        series = self._episodes()
        titles = [next((e.get("series_title") for e in series.values() if str(e.get("sid")) == s), None) for s in sids]
        name = ", ".join(t for t in titles if t) or "Series " + ", ".join(sids)
    with self._lock:
      monitors = self._monitors()
      for existing in monitors:
        if (query and existing.get("query", "") and existing["query"].lower() == query.lower()) or \
           (sids and sorted(existing.get("sids") or []) == sorted(sids)):
          raise ManagerError(f"Monitor '{existing['id']}' already covers this, update it instead")
      base = _slugify(name)
      monitor_id, n = base, 2
      ids = {m["id"] for m in monitors}
      while monitor_id in ids:
        monitor_id, n = f"{base}-{n}", n + 1
      monitor = {
        "id": monitor_id,
        "name": name,
        "query": query,
        "sids": sids,
        "enabled": bool(enabled),
        "notes": notes,
        **options,
        "created_at": _iso(),
        "last_run_at": None,
        "last_result": None,
      }
      monitors.append(monitor)
      self._save_monitors(monitors)
    return monitor

  def update_monitor(self, monitor_id, **changes):
    allowed = set(MONITOR_OPTIONS) | {"name", "query", "sids", "enabled", "notes"}
    unknown = set(changes) - allowed
    if unknown:
      raise ManagerError(f"Unknown monitor fields: {sorted(unknown)}")
    with self._lock:
      monitors = self._monitors()
      monitor = next((m for m in monitors if m["id"] == monitor_id), None)
      if monitor is None:
        raise ManagerError(f"No monitor with id '{monitor_id}', use list_monitors to see the ids")
      for key, value in changes.items():
        if value is None:
          continue
        if key in MONITOR_OPTIONS and value == "":
          monitor[key] = None   # empty string resets to the default setting
          continue
        if key == "sids":
          value = _clean_ids(value, "sid")
          if value:
            monitor["query"] = None
        if key == "query":
          value = _clean_query(value) or None
          if value:
            monitor["sids"] = None
        monitor[key] = value
      self._validate_monitor_options(monitor)
      if not monitor.get("query") and not monitor.get("sids"):
        raise ManagerError("A monitor needs either a query or sids")
      self._save_monitors(monitors)
    return monitor

  def remove_monitor(self, monitor_id):
    with self._lock:
      monitors = self._monitors()
      remaining = [m for m in monitors if m["id"] != monitor_id]
      if len(remaining) == len(monitors):
        raise ManagerError(f"No monitor with id '{monitor_id}'")
      self._save_monitors(remaining)
    return {"removed": monitor_id, "remaining": len(remaining)}

  def _monitor_options(self, monitor, settings):
    options = {}
    options["output_path"] = monitor.get("output_path") or settings["download_path"]
    for key in ("quality", "plex", "original_title", "include_english_subs"):
      options[key] = monitor.get(key) if monitor.get(key) is not None else settings[key]
    return options

  def monitor_pending(self, monitor, online=True):
    recorded = self.recorded_pids()
    options = self._monitor_options(monitor, self.get_settings())
    items = self.find_episodes(query=monitor.get("query"), sids=monitor.get("sids"),
                               include_english_subs=options["include_english_subs"], online=online)
    return [i for i in items if i.get("pid") not in recorded]

  def check_monitors(self, monitor_ids=None, online=True):
    settings = self.get_settings()
    report = []
    for monitor in self._monitors():
      if monitor_ids and monitor["id"] not in monitor_ids:
        continue
      pending = self.monitor_pending(monitor, online=online)
      options = dict(self._monitor_options(monitor, settings), download_path=settings["download_path"],
                     no_metadata=settings["no_metadata"])
      report.append({
        "monitor_id": monitor["id"],
        "name": monitor["name"],
        "enabled": monitor.get("enabled", True),
        "pending_count": len(pending),
        "pending": [dict(self.episode_summary(i, set()), planned_file=self.planned_file(i, options))
                    for i in pending[:25]],
      })
    return {"catalog_date": self.catalog_date(), "monitors": report}

  def run_monitors(self, monitor_ids=None, refresh=None, source="manual"):
    settings = self.get_settings()
    monitors = [m for m in self._monitors()
                if (m["id"] in monitor_ids if monitor_ids else m.get("enabled", True))]
    if monitor_ids:
      missing = set(monitor_ids) - {m["id"] for m in monitors}
      if missing:
        raise ManagerError(f"Unknown monitor ids: {sorted(missing)}")
    if not monitors:
      raise ManagerError("There are no enabled monitors to run, add one with add_monitor")
    run_id = "run-" + uuid.uuid4().hex[:8]
    jobs = []
    if refresh if refresh is not None else settings.get("refresh_before_monitors", True):
      jobs.append(self._submit(Job("refresh", {"full": False}, "Incremental catalog refresh before monitors",
                                   run_id=run_id)))
    for monitor in monitors:
      options = self._monitor_options(monitor, settings)
      params = {
        "query": monitor.get("query"),
        "sids": monitor.get("sids"),
        **options,
      }
      jobs.append(self._submit(Job("download", self._download_params(params, settings),
                                   f"Monitor: {monitor['name']}", monitor_id=monitor["id"], run_id=run_id)))
    with self._lock:
      self._save_monitors(self._monitors(), last_run_at=_iso(), last_run_source=source)
    return {"run_id": run_id, "jobs": [{"job_id": j.id, "label": j.label} for j in jobs]}

  def _record_monitor_result(self, job):
    if not job.monitor_id:
      return
    with self._lock:
      monitors = self._monitors()
      for monitor in monitors:
        if monitor["id"] == job.monitor_id:
          monitor["last_run_at"] = job.finished_at
          monitor["last_result"] = {
            "status": job.status,
            "job_id": job.id,
            "downloaded": len(job.downloaded),
            "titles": [d.get("title") for d in job.downloaded][:10],
            "errors": job.errors[-3:],
          }
      self._save_monitors(monitors)

  # -------------------------------------------------------------- downloads

  def _download_params(self, request, settings):
    params = {
      "pids": request.get("pids"),
      "sids": request.get("sids"),
      "query": request.get("query"),
      "output_path": _norm_path(request.get("output_path") or settings["download_path"]),
      "quality": request.get("quality") or settings["quality"],
      "plex": settings["plex"] if request.get("plex") is None else bool(request["plex"]),
      "original_title": settings["original_title"] if request.get("original_title") is None else bool(request["original_title"]),
      "include_english_subs": settings["include_english_subs"] if request.get("include_english_subs") is None else bool(request["include_english_subs"]),
      "new_only": bool(request.get("new_only")),
      "force": bool(request.get("force")),
      "check_local": bool(request.get("check_local")),
      "metadata_only": bool(request.get("metadata_only")),
      "suffix": request.get("suffix") or "",
      "no_metadata": settings["no_metadata"],
      "keep_partial": settings["keep_partial"],
    }
    if params["quality"] not in QUALITIES:
      raise ManagerError(f"quality must be one of {QUALITIES}")
    if params["suffix"] and not params["suffix"].startswith(" "):
      params["suffix"] = " " + params["suffix"]
    return {k: v for k, v in params.items() if v is not None}

  def download(self, pids=None, sids=None, query=None, dry_run=False, allow_many=False, **options):
    pids = _clean_ids(pids, "pid")
    sids = _clean_ids(sids, "sid")
    query = _clean_query(query) if query else None
    if sum(bool(x) for x in (pids, sids, query)) != 1:
      raise ManagerError("Provide exactly one of 'pids', 'sids' or 'query'")
    settings = self.get_settings()
    params = self._download_params(dict(options, pids=pids, sids=sids, query=query), settings)

    needs_preview = dry_run or (query and not params["force"] and not allow_many)
    if needs_preview:
      items = self.find_episodes(query=query, sids=sids, pids=pids, new_only=params["new_only"],
                                 include_english_subs=params["include_english_subs"], online=True)
      recorded = self.recorded_pids()
      to_fetch = [i for i in items if params["force"] or params["metadata_only"] or i.get("pid") not in recorded]
      if dry_run:
        plan_options = dict(params, download_path=settings["download_path"])
        return {
          "dry_run": True,
          "matched": len(items),
          "would_download": len(to_fetch),
          "already_downloaded": len(items) - len(to_fetch),
          "output_path": params["output_path"],
          "quality": params["quality"],
          "episodes": [dict(self.episode_summary(i, recorded), planned_file=self.planned_file(i, plan_options))
                       for i in items[:50]],
          "truncated": len(items) > 50,
        }
      limit = settings.get("max_query_matches", 25)
      if len(to_fetch) > limit:
        raise ManagerError(
          f"The query '{query}' would download {len(to_fetch)} episodes (limit {limit}). "
          "Run with dry_run=true to review, narrow the query, use sids/pids, or pass allow_many=true.")

    label = "Download " + (f"pids {', '.join(pids)}" if pids else f"series {', '.join(sids)}" if sids else f"'{query}'")
    job = self._submit(Job("download", params, label))
    return {"job_id": job.id, "status": job.status, "label": label, "output_path": params["output_path"],
            "queue_position": self._queue_position(job)}

  def _cli_args(self, params, settings):
    args = [sys.executable, "-u", SCRIPT_PATH, "-o", params["output_path"], "-q", params["quality"]]
    if params.get("pids"):
      args += ["--pid", *params["pids"]]
    elif params.get("sids"):
      args += ["--sid", *params["sids"]]
    elif params.get("query"):
      args += ["--find", params["query"]]
    flags = {
      "plex": "--plex", "original_title": "--originaltitle", "include_english_subs": "--includeenglishsubs",
      "new_only": "--new", "force": "--force", "check_local": "--checklocal", "metadata_only": "--novideo",
      "no_metadata": "--nometadata", "keep_partial": "--keeppartial",
    }
    for key, flag in flags.items():
      if params.get(key):
        args.append(flag)
    if params.get("suffix"):
      args += ["--suffix", params["suffix"]]
    if settings.get("ffmpeg_path"):
      args += ["--ffmpeg", settings["ffmpeg_path"]]
    if settings.get("imdb_folder"):
      args += ["--imdbfolder", settings["imdb_folder"]]
    return args

  def _run_download(self, job):
    settings = self.get_settings()
    params = job.params
    os.makedirs(params["output_path"], exist_ok=True)
    before = self.recorded_pids()
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1", NO_COLOR="1",
               ANSI_COLORS_DISABLED="1", RUVSARPUR_CONFIG_DIR=self.config_dir)
    popen_kwargs = {}
    if os.name == "nt":
      popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
      popen_kwargs["start_new_session"] = True
    args = self._cli_args(params, settings)
    job.log.append("$ " + subprocess.list2cmdline(args[1:]))
    job.proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                text=True, encoding="utf-8", errors="replace", env=env,
                                cwd=os.path.dirname(SCRIPT_PATH), **popen_kwargs)
    if job.cancel_requested:
      self._terminate(job.proc)
    nothing_found = False
    for raw in job.proc.stdout:
      line = RE_ANSI.sub("", raw).strip()
      if not line:
        continue
      self._parse_progress(job, line)
      if RE_NOTHING.search(line):
        nothing_found = True
      if not line.startswith("Downloading:") or "Complete" in line:
        job.log.append(line)
      elif not job.log or not job.log[-1].startswith("Downloading:"):
        job.log.append(line)
      else:
        job.log[-1] = line  # keep only the latest progress bar line
    job.returncode = job.proc.wait()
    job.proc = None

    after = self.recorded_pids()
    new_pids = [p for p in after if p not in before]
    self._record_history(job, new_pids)
    if job.cancel_requested:
      job.status = "cancelled"
    elif nothing_found:
      job.status = "nothing_found"
    elif job.returncode != 0:
      job.status = "failed"
      job.errors.append(f"ruvsarpur.py exited with code {job.returncode}")
    elif job.errors and not job.downloaded and not job.skipped_pids:
      job.status = "failed"
    else:
      job.status = "completed"

  def _parse_progress(self, job, line):
    m = RE_FOUND.search(line)
    if m:
      job.items_found = int(m.group(1))
      return
    m = RE_ITEM.search(line)
    if m:
      job.current_index = int(m.group(1))
      job.current_title = m.group(3)
      job.current_estimated_mb = int(m.group(4))
      job.current_percent = 0.0
      return
    m = RE_PERCENT.search(line)
    if m:
      try:
        job.current_percent = float(m.group(1))
      except ValueError:
        pass
    m = RE_COMPLETE.search(line)
    if m:
      job.completed_files.append(m.group(1).strip())
      return
    m = RE_ALREADY.search(line) or RE_FOUND_LOCAL.search(line)
    if m:
      job.skipped_pids.append(m.group(1))
      return
    if line.startswith("Error") or "not found on server" in line or line.startswith("Could not locate FFMPEG"):
      job.errors.append(line)
    elif RE_EXCEPTION.match(line) and line not in job.errors:
      job.errors.append(line)   # the final line of a Python traceback

  def _record_history(self, job, new_pids):
    if not new_pids:
      return
    episodes = self._episodes()
    settings = self.get_settings()
    options = dict(job.params, download_path=settings["download_path"])
    entries = []
    for pid in new_pids:
      item = episodes.get(pid) or {"pid": pid}
      file_path = None
      if item.get("title"):
        planned = self.planned_file(item, options)
        stem = planned[:-4] if planned.endswith(".mp4") else planned
        file_path = next((f for f in job.completed_files if f.startswith(stem)), None)
        if file_path is None and os.path.exists(planned):
          file_path = planned
      size = None
      if file_path and os.path.exists(file_path):
        size = os.path.getsize(file_path)
      entries.append({
        "pid": pid,
        "sid": item.get("sid"),
        "title": item.get("title"),
        "series_title": item.get("series_title"),
        "kind": self._kind(item) if item.get("title") else None,
        "file": file_path,
        "size_bytes": size,
        "quality": job.params.get("quality"),
        "downloaded_at": _iso(),
        "job_id": job.id,
        "monitor_id": job.monitor_id,
      })
    job.downloaded.extend(entries)
    with self._lock:
      history = self._read_json(HISTORY_FILE, [])
      history.extend(entries)
      self._write_json(HISTORY_FILE, history)

  def get_history(self, limit=25, query=None, monitor_id=None, since_days=None):
    history = self._read_json(HISTORY_FILE, [])
    if query:
      q = query.lower()
      history = [h for h in history if q in str(h.get("title", "")).lower()
                 or q in str(h.get("series_title", "")).lower()]
    if monitor_id:
      history = [h for h in history if h.get("monitor_id") == monitor_id]
    if since_days:
      cutoff = _now() - datetime.timedelta(days=since_days)
      history = [h for h in history if (_parse_iso(h.get("downloaded_at")) or cutoff) > cutoff]
    history = list(reversed(history))
    return {"total": len(history), "entries": history[:limit], "truncated": len(history) > limit}

  def update_registry(self, mark=None, unmark=None, wait_seconds=10):
    mark = _clean_ids(mark, "pid") or []
    unmark = _clean_ids(unmark, "pid") or []
    if not mark and not unmark:
      raise ManagerError("Provide pids to mark or unmark")
    job = self._submit(Job("registry", {"mark": mark, "unmark": unmark},
                           f"Registry update (+{len(mark)} / -{len(unmark)})"))
    job.done.wait(wait_seconds)
    return job.to_dict()

  def _run_registry(self, job):
    pids = ruvsarpur.getPreviouslyRecordedShows(self.registry_file())
    current = set(pids)
    added = [p for p in job.params["mark"] if p not in current]
    removed = [p for p in job.params["unmark"] if p in current]
    remove_set = set(removed)
    pids = [p for p in pids if p not in remove_set] + added
    os.makedirs(self.config_dir, exist_ok=True)
    with open(self.registry_file(), "w") as fh:
      for pid in pids:
        fh.write(f"{pid}\n")
    job.result = {"marked": added, "unmarked": removed,
                  "not_changed": [p for p in job.params["mark"] + job.params["unmark"]
                                  if p not in added and p not in removed]}
    job.status = "completed"

  # ------------------------------------------------------------------- jobs

  def _submit(self, job):
    with self._lock:
      self._jobs[job.id] = job
      self._queue.append(job)
      self._prune_jobs()
      self._queue_cv.notify_all()
    self.start_worker()
    return job

  def _queue_position(self, job):
    with self._lock:
      try:
        return list(self._queue).index(job) + 1
      except ValueError:
        return 0

  def submit_refresh(self, full=False, wait_seconds=0):
    job = self._submit(Job("refresh", {"full": bool(full)}, ("Full" if full else "Incremental") + " catalog refresh"))
    if wait_seconds:
      job.done.wait(wait_seconds)
    return job.to_dict()

  def get_job(self, job_id, log_lines=30):
    job = self._jobs.get(job_id)
    if job is None:
      raise ManagerError(f"No job with id '{job_id}'")
    data = job.to_dict(log_lines=log_lines)
    if job.status == "queued":
      data["queue_position"] = self._queue_position(job)
    return data

  def list_jobs(self, status=None, limit=20):
    with self._lock:
      jobs = list(self._jobs.values())
    if status == "active":
      jobs = [j for j in jobs if j.status in ACTIVE_STATES]
    elif status:
      jobs = [j for j in jobs if j.status == status]
    jobs = list(reversed(jobs))[:limit]
    summary = []
    for job in jobs:
      entry = {"job_id": job.id, "kind": job.kind, "label": job.label, "status": job.status,
               "created_at": job.created_at, "finished_at": job.finished_at}
      if job.kind == "download":
        entry["downloaded_count"] = len(job.downloaded)
        if job.status == "running":
          entry["current_title"] = job.current_title
          entry["current_percent"] = job.current_percent
      if job.errors:
        entry["error_count"] = len(job.errors)
      summary.append(entry)
    return {"jobs": summary}

  def cancel_job(self, job_id):
    with self._lock:
      job = self._jobs.get(job_id)
      if job is None:
        raise ManagerError(f"No job with id '{job_id}'")
      if job.status in FINISHED_STATES:
        return {"job_id": job_id, "status": job.status, "message": "Job had already finished"}
      job.cancel_requested = True
      if job in self._queue:
        self._queue.remove(job)
        job.status = "cancelled"
        job.finished_at = _iso()
        job.done.set()
        self._persist_jobs()
        return {"job_id": job_id, "status": "cancelled"}
      proc = job.proc
    if proc is not None:
      self._terminate(proc)
    return {"job_id": job_id, "status": "cancelling",
            "message": "Stopping the download. A partially downloaded file may remain in the output folder."}

  @staticmethod
  def _terminate(proc):
    try:
      if os.name == "nt":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
      else:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
          proc.wait(10)
        except subprocess.TimeoutExpired:
          os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, OSError):
      pass

  def start_worker(self):
    with self._lock:
      if self._worker is None or not self._worker.is_alive():
        self._worker = threading.Thread(target=self._worker_loop, name="ruvsarpur-jobs", daemon=True)
        self._worker.start()

  def _worker_loop(self):
    while not self._stop.is_set():
      with self._lock:
        while not self._queue and not self._stop.is_set():
          self._queue_cv.wait(1)
        if self._stop.is_set():
          return
        job = self._queue.popleft()
        job.status = "waiting"
      self._execute(job)

  def _execute(self, job):
    job.status_detail = "waiting for another ruvsarpur run to finish"
    while not self.run_lock.try_acquire():
      if job.cancel_requested or self._stop.is_set():
        job.status = "cancelled"
        job.finished_at = _iso()
        job.done.set()
        return
      time.sleep(2)
    try:
      if job.cancel_requested:
        job.status = "cancelled"
        return
      job.status = "running"
      job.status_detail = None
      job.started_at = _iso()
      if job.kind == "download":
        self._run_download(job)
      elif job.kind == "refresh":
        job.result = self.refresh_catalog(full=job.params.get("full"), job=job)
        job.status = "completed"
      elif job.kind == "registry":
        self._run_registry(job)
      else:
        raise ManagerError(f"Unknown job kind {job.kind}")
    except Exception as ex:
      job.status = "failed"
      job.errors.append(f"{type(ex).__name__}: {ex}")
    finally:
      self.run_lock.release()
      job.status_detail = None
      job.finished_at = _iso()
      job.proc = None
      try:
        self._record_monitor_result(job)
      except Exception as ex:
        job.errors.append(f"Could not save monitor result: {ex}")
      with self._lock:
        self._persist_jobs()
      job.done.set()

  def _prune_jobs(self, keep=200):
    finished = [j for j in self._jobs.values() if j.status in FINISHED_STATES]
    for job in finished[:max(0, len(self._jobs) - keep)]:
      del self._jobs[job.id]

  def _persist_jobs(self):
    finished = [j.to_dict() for j in self._jobs.values() if j.status in FINISHED_STATES][-100:]
    try:
      self._write_json(JOBS_FILE, finished)
    except OSError:
      pass

  def _load_jobs(self):
    try:
      for data in self._read_json(JOBS_FILE, []):
        job = Job.from_dict(data)
        self._jobs[job.id] = job
    except (ManagerError, AttributeError, TypeError):
      pass

  def wait_for(self, jobs, timeout=None):
    deadline = None if timeout is None else time.monotonic() + timeout
    for job_id in jobs:
      job = self._jobs.get(job_id)
      if job is None:
        continue
      remaining = None if deadline is None else max(0, deadline - time.monotonic())
      job.done.wait(remaining)

  # -------------------------------------------------------------- scheduler

  def start_scheduler(self):
    """Runs enabled monitors on the configured interval while the process lives."""
    if self._scheduler is not None:
      return
    self._scheduler = threading.Thread(target=self._scheduler_loop, name="ruvsarpur-scheduler", daemon=True)
    self._scheduler.start()

  def _scheduler_loop(self):
    while not self._stop.wait(30):
      try:
        interval = int(self.get_settings().get("monitor_interval_minutes") or 0)
        if interval <= 0:
          continue
        last = _parse_iso(self._monitor_state().get("last_run_at"))
        if last and _now() - last < datetime.timedelta(minutes=interval):
          continue
        with self._lock:
          busy = any(j.run_id and j.status in ACTIVE_STATES for j in self._jobs.values())
        if busy or not any(m.get("enabled", True) for m in self._monitors()):
          continue
        self.run_monitors(source="scheduler")
      except Exception as ex:
        print(f"ruvsarpur scheduler error: {ex}", file=sys.stderr)

  def shutdown(self):
    self._stop.set()
    with self._lock:
      running = [j for j in self._jobs.values() if j.proc is not None]
      self._queue_cv.notify_all()
    for job in running:
      self._terminate(job.proc)

  # ------------------------------------------------------------------ stats

  def environment(self):
    settings = self.get_settings()
    try:
      ffmpeg = ruvsarpur.findffmpeg(settings.get("ffmpeg_path"), os.path.dirname(SCRIPT_PATH))
      ffmpeg_error = None
    except Exception as ex:
      ffmpeg, ffmpeg_error = None, str(ex)
    return {
      "ruvsarpur_version": ruvsarpur.__version__,
      "python": sys.version.split()[0],
      "config_dir": self.config_dir,
      "ffmpeg": ffmpeg,
      "ffmpeg_error": ffmpeg_error,
    }

  def stats(self, include_library_size=False):
    settings = self.get_settings()
    episodes = self._episodes()
    recorded = self.recorded_pids()
    kinds = collections.Counter(self._kind(e) for e in episodes.values())
    new_series = {e.get("sid") for e in episodes.values()
                  if str(e.get("ep_num")) == "1" and str(e.get("ep_total") or "1") not in ("1", "None")}
    schedule_age = None
    try:
      schedule_age = round((time.time() - os.path.getmtime(self.schedule_file())) / 3600, 1)
    except OSError:
      pass

    history = self._read_json(HISTORY_FILE, [])
    now = _now()

    def count_since(delta):
      return sum(1 for h in history if (_parse_iso(h.get("downloaded_at")) or now - delta * 2) > now - delta)

    tracked_bytes = sum(h.get("size_bytes") or 0 for h in history)
    by_series = collections.Counter(h.get("series_title") or h.get("title") for h in history)

    path = settings["download_path"]
    storage = {"download_path": path, "exists": os.path.isdir(path)}
    probe = path
    while probe and not os.path.exists(probe):
      parent = os.path.dirname(probe)
      probe = parent if parent != probe else None
    if probe:
      usage = shutil.disk_usage(probe)
      storage.update(free_bytes=usage.free, free=_human_bytes(usage.free), total=_human_bytes(usage.total))
    if include_library_size and os.path.isdir(path):
      size, files = 0, 0
      for root, _, names in os.walk(path):
        for name in names:
          if name.lower().endswith(".mp4"):
            files += 1
            try:
              size += os.path.getsize(os.path.join(root, name))
            except OSError:
              pass
      storage.update(library_video_files=files, library_size=_human_bytes(size), library_size_bytes=size)

    monitors = self._monitors()
    with self._lock:
      jobs = list(self._jobs.values())
    running = next((j for j in jobs if j.status == "running"), None)
    return {
      "catalog": {
        "last_refresh_date": (self._schedule_cache or {}).get("date"),
        "file_age_hours": schedule_age,
        "episodes": len(episodes),
        "series": len({e.get("sid") for e in episodes.values()}),
        "by_kind": dict(kinds),
        "new_series_starting": len(new_series),
        "episodes_available_and_downloaded": sum(1 for p in episodes if p in recorded),
      },
      "downloads": {
        "registry_total": len(recorded),
        "history_entries": len(history),
        "last_24h": count_since(datetime.timedelta(days=1)),
        "last_7d": count_since(datetime.timedelta(days=7)),
        "last_30d": count_since(datetime.timedelta(days=30)),
        "tracked_size": _human_bytes(tracked_bytes),
        "tracked_size_bytes": tracked_bytes,
        "top_series": [{"title": t, "downloads": n} for t, n in by_series.most_common(5)],
        "last_download": history[-1] if history else None,
      },
      "monitors": {
        "total": len(monitors),
        "enabled": sum(1 for m in monitors if m.get("enabled", True)),
        "last_run_at": self._monitor_state().get("last_run_at"),
        "auto_run_interval_minutes": settings.get("monitor_interval_minutes"),
      },
      "jobs": {
        "queued": sum(1 for j in jobs if j.status in ("queued", "waiting")),
        "running": running.to_dict() if running else None,
        "failed_recently": sum(1 for j in jobs if j.status == "failed"),
      },
      "storage": storage,
      "environment": self.environment(),
    }
