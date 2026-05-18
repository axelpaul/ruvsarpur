# ruvsarpur MCP server

`ruvsarpur_mcp.py` wraps `ruvsarpur.py` behind the Model Context Protocol
so AI agents (OpenClaw, Claude Desktop, Claude Code, etc.) can search the
RÚV schedule and trigger downloads via typed tool calls instead of having
to parse CLI stdout.

## Install

```bash
pip install -r requirements.txt
pip install "mcp[cli]"
```

`ffmpeg` is still required for downloads — see the main README.

## Run

Stdio transport (the usual way an MCP client launches a server):

```bash
python src/ruvsarpur_mcp.py
```

## Example client config

Claude Desktop / Claude Code MCP config:

```json
{
  "mcpServers": {
    "ruvsarpur": {
      "command": "python",
      "args": ["/absolute/path/to/ruvsarpur/src/ruvsarpur_mcp.py"]
    }
  }
}
```

## Tools

Read:

| Tool | Purpose |
| --- | --- |
| `schedule_status` | Last refresh date and number of cached entries |
| `search_shows` | Fuzzy search by `query`, or filter by `sid`/`pid`, `only_new` |
| `get_show` | Full schedule entry for a single `pid` or `sid` |
| `list_downloaded` | Pids in `prevrecorded.log`, enriched with titles from the schedule |
| `recent_downloads` | Download/refresh jobs the server has run (persists across restarts) |
| `list_jobs` | All tracked jobs (running + finished) |
| `get_job_status` | Status + last log lines for a job |
| `get_config` | Where logs live, available qualities |

Write (return a `job_id` immediately for the long-running ones):

| Tool | Purpose |
| --- | --- |
| `refresh_schedule` | Pull a fresh schedule from RÚV (incremental by default) |
| `start_download` | Download by `pid` or `sid` with quality/plex/output options |
| `cancel_job` | Terminate a running job |
| `convert_vtt_to_srt` | Wraps `webvtttosrt.py` |

Watchlist (server-side queue — ruvsarpur itself has no concept of one):

| Tool | Purpose |
| --- | --- |
| `add_to_watchlist` | Queue a pid/sid for later with quality/output options |
| `list_watchlist` | Show queued / started / downloaded / failed entries |
| `remove_from_watchlist` | Drop one entry by its id |
| `clear_watchlist` | Drop entries by status (default: `downloaded`) |
| `download_watchlist` | Start a background job for every pending entry |

## What the agent can see about past activity

- `recent_downloads` returns the title, pid/sid, quality, output path,
  status (`succeeded`/`failed`/`cancelled`), return code, and last ~20 log
  lines for each job. It's persisted to `mcp_jobs.json` next to ruvsarpur's
  other logs (or to the working dir under `--portable`), so the agent can
  ask "what did we download last run?" even after the server is restarted.
- `list_downloaded` resolves every pid in `prevrecorded.log` against the
  current schedule, so the agent gets titles, not just numbers.
- Each watchlist entry tracks its own status and `last_job_id`, so the
  agent can correlate a queued item with the job that ran for it.

## Design notes

- Read tools import helpers from `ruvsarpur.py` and return trimmed JSON.
- Downloads/refresh/watchlist-runs shell out to `ruvsarpur.py` as a
  subprocess and stream stdout into an in-memory ring buffer keyed by job
  id. Agents poll `get_job_status` instead of blocking on ffmpeg.
- Job history is persisted to `mcp_jobs.json`; the watchlist lives in
  `mcp_watchlist.json`. Both files follow `--portable`.
- `start_download` has an `extra_args` escape hatch for any flag not
  surfaced as a parameter (e.g. `--checklocal`, `--keeppartial`).
