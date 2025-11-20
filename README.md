# Roblox Job Gateway

Minimal FastAPI service that polls a Discord channel for job embeds, queues parsed jobs, and exposes HTTP/WebSocket endpoints for Roblox clients and internal tools.

## Requirements

- Python 3.11+
- (Optional) Redis instance when running multiple replicas

Install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Environment Variables

| Name | Description | Default |
| ---- | ----------- | ------- |
| `SETTINGS_FILE` | Optional JSON file with fallback values | `app_settings.json` |
| `CHANNEL_ID` | Discord channel ID to poll | – |
| `DISCORD_TOKEN` | Discord user token for REST polling | – |
| `API_KEY` | **Required.** Shared secret; required on every HTTP/WebSocket call | – |
| `REDIS_URL` | Redis URL; enables Redis-backed FIFO queue | – |
| `PORT` | HTTP listen port | `8000` |
| `HEADLESS_MODE` | Set to `true`/`false` to signal headless mode | `true` |
| `SERVICE_BASE_URL` | Self-referential base URL for internal callbacks | `https://pixells-basic.onrender.com` |
| `DISCORD_POLL_SECONDS` | Base poll interval (seconds) | `0.25` |
| `BURST_POLL_SECONDS` | Burst interval when new jobs detected | `0.05` |

> **Note:** When `REDIS_URL` is not set an in-memory queue is used and only a single application instance should run.

## Running Locally


Launch with the API key exported (example PowerShell):

```powershell
$env:API_KEY = 'super-secret-value'
uvicorn app:app --host 0.0.0.0 --port $PORT
```

This exposes:

- `POST /job` – Enqueue a job (requires `X-API-KEY`).
- `GET /job/next` – Dequeue the next job (`X-API-KEY` required).
- `GET /health` – Returns `{ "ok": true }` (`X-API-KEY` required).
- `WS /ws` – WebSocket channel pushing jobs to connected Roblox clients (`X-API-KEY` header or `?api_key=` query required).

The service also runs a background Discord poller that parses embed fields into jobs, enqueues them, and mirrors them through the HTTP and WebSocket interfaces.

## Render Deployment Notes

1. Create a **Web Service**.
2. Set build command to install dependencies (e.g. `pip install -r requirements.txt`).
3. Run command: `uvicorn app:app --host 0.0.0.0 --port $PORT`.
4. Configure environment variables listed above (ensure `DISCORD_TOKEN`, `CHANNEL_ID`, `API_KEY`, and optionally `SERVICE_BASE_URL=https://pixells-basic.onrender.com`).
5. When scaling to multiple instances, configure `REDIS_URL` so all replicas share the Redis-backed queue. Without Redis, run exactly one instance.
