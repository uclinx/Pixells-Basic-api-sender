import asyncio
import json
import logging
import os
import re
import time
from collections import deque
from contextlib import suppress
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, validator


SETTINGS_FILE = os.getenv("SETTINGS_FILE", "app_settings.json")
DISCORD_API_BASE = "https://discord.com/api/v9"
REQUEST_TIMEOUT: Tuple[float, float] = (0.6, 2.0)
DISCORD_POLL_SECONDS = float(os.getenv("DISCORD_POLL_SECONDS", 0.25))
BURST_POLL_SECONDS = float(os.getenv("BURST_POLL_SECONDS", 0.05))
RECENT_JOB_IDS_LIMIT = 2048


def load_settings(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:  # pragma: no cover
        print(f"failed to load settings file {path}: {exc}")
        return {}


_settings_data = load_settings(SETTINGS_FILE)


def as_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def setting(name: str, default: Optional[str] = None) -> Optional[str]:
    env_value = os.getenv(name)
    if env_value:
        return env_value
    value = _settings_data.get(name)
    if value is None:
        return default
    return str(value)


CHANNEL_ID = setting("CHANNEL_ID")
DISCORD_TOKEN = setting("DISCORD_TOKEN")
API_KEY = setting("API_KEY")
REDIS_URL = setting("REDIS_URL")
PORT = int(setting("PORT", "8000"))
HEADLESS_MODE = as_bool(setting("HEADLESS_MODE", "true"), default=True)
DEFAULT_SERVICE_BASE = os.getenv("RENDER_EXTERNAL_URL", "https://pixells-basic.onrender.com")
SERVICE_BASE_URL = setting("SERVICE_BASE_URL", DEFAULT_SERVICE_BASE)
API_KEY_HEADER = "X-API-KEY"


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("roblox-job-gateway")


class JobPayload(BaseModel):
    job_id: str
    money: float
    name: str
    players: Any
    players_max: Any
    ts: int

    @validator("job_id")
    def validate_job_id(cls, value: str) -> str:  # noqa: D417
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("job_id required")
        return cleaned

    @validator("ts")
    def validate_timestamp(cls, value: int) -> int:  # noqa: D417
        if value <= 0:
            raise ValueError("ts must be positive")
        return value

    @validator("players", "players_max", pre=True)
    def normalize_players(cls, value: Any) -> Any:  # noqa: D417
        if isinstance(value, str):
            value = value.strip()
        if value in ("", None):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return value

    @validator("money", pre=True)
    def normalize_money(cls, value: Any) -> float:  # noqa: D417
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover
            raise ValueError("money must be numeric") from exc
        return numeric


class JobQueueManager:
    def __init__(self, redis_url: Optional[str]) -> None:
        self._queue: Queue = Queue()
        self._redis = None
        self._redis_key = "roblox_job_queue"
        if redis_url:
            try:
                import redis  # type: ignore

                self._redis = redis.from_url(redis_url, decode_responses=True)
                logger.info("redis queue backend enabled")
            except Exception as exc:  # pragma: no cover
                logger.warning("redis unavailable, using in-memory queue: %s", exc)
        else:
            logger.info("in-memory queue backend enabled")

    async def enqueue(self, job: Dict[str, Any]) -> None:
        if self._redis:
            payload = json.dumps(job, separators=(",", ":"))
            await asyncio.to_thread(self._redis.lpush, self._redis_key, payload)
        else:
            self._queue.put(job)

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        if self._redis:
            raw = await asyncio.to_thread(self._redis.rpop, self._redis_key)
            if raw:
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:  # pragma: no cover
                    logger.warning("discarded malformed job payload from redis")
                    return None
            return None
        try:
            return self._queue.get_nowait()
        except Empty:
            return None


queue_manager = JobQueueManager(REDIS_URL)
connected_clients: Set[WebSocket] = set()
clients_lock = asyncio.Lock()
broadcast_lock = asyncio.Lock()
recent_job_ids: deque[str] = deque(maxlen=RECENT_JOB_IDS_LIMIT)
recent_ids_lock = asyncio.Lock()
api_session = requests.Session()
api_session.headers.update({
    "Content-Type": "application/json",
    "Connection": "keep-alive",
})

default_discord_headers = {
    "User-Agent": "RobloxJobGateway/1.0",
}
if DISCORD_TOKEN:
    default_discord_headers["Authorization"] = DISCORD_TOKEN

if not API_KEY:
    raise RuntimeError("API_KEY must be configured for secure access.")

app = FastAPI(title="Roblox Job Gateway", docs_url=None, redoc_url=None)

discord_session = requests.Session()
discord_session.headers.update(default_discord_headers)
discord_task: Optional[asyncio.Task] = None


async def remember_job(job_id: str) -> bool:
    if not job_id:
        return False
    async with recent_ids_lock:
        if job_id in recent_job_ids:
            return False
        recent_job_ids.append(job_id)
        return True


async def broadcast_job(job: Dict[str, Any]) -> None:
    async with clients_lock:
        clients_snapshot = list(connected_clients)
    if not clients_snapshot:
        return
    payload = json.dumps(job, separators=(",", ":"))
    async with broadcast_lock:
        stale: List[WebSocket] = []
        for websocket in clients_snapshot:
            try:
                await websocket.send_text(payload)
            except Exception:  # pragma: no cover
                stale.append(websocket)
        if stale:
            async with clients_lock:
                for websocket in stale:
                    connected_clients.discard(websocket)


async def enqueue_job(job_obj: Dict[str, Any]) -> bool:
    job_id = job_obj.get("job_id", "")
    is_new = await remember_job(str(job_id))
    if not is_new:
        return False
    await queue_manager.enqueue(job_obj)
    logger.info("job enqueued")
    asyncio.create_task(broadcast_job(job_obj))
    return True


async def dequeue_job() -> Optional[Dict[str, Any]]:
    return await queue_manager.dequeue()


async def verify_api_key(request: Request) -> None:
    if not API_KEY:
        return
    provided = request.headers.get(API_KEY_HEADER)
    if provided != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health", dependencies=[Depends(verify_api_key)])
async def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/job", dependencies=[Depends(verify_api_key)])
async def add_job(payload: JobPayload) -> Dict[str, bool]:
    job_data = payload.dict()
    await enqueue_job(job_data)
    return {"ok": True}


@app.get("/job/next", dependencies=[Depends(verify_api_key)])
async def get_next_job() -> Response:
    job = await dequeue_job()
    if job is None:
        return Response(status_code=204)
    return JSONResponse(job)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    credential = (
        websocket.headers.get(API_KEY_HEADER)
        or websocket.query_params.get("api_key")
    )
    if credential != API_KEY:
        await websocket.close(code=1008)
        logger.warning("websocket rejected due to invalid API key")
        return
    await websocket.accept()
    logger.info("websocket client connected")
    async with clients_lock:
        connected_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with clients_lock:
            connected_clients.discard(websocket)


async def post_job_non_blocking(job: Dict[str, Any]) -> None:
    if not job:
        return
    base = SERVICE_BASE_URL.rstrip("/")
    url = f"{base}/job"

    def _do_post() -> None:
        try:
            headers = {API_KEY_HEADER: API_KEY} if API_KEY else {}
            api_session.post(url, json=job, timeout=REQUEST_TIMEOUT, headers=headers)
        except Exception as exc:  # pragma: no cover
            logger.debug("internal job post failed: %s", exc)

    await asyncio.to_thread(_do_post)


_money_pattern = re.compile(r"(\d+[\d,]*\.?\d*)")
_players_pattern = re.compile(r"(\d+)\s*/\s*(\d+)")
_jobid_cleanup = re.compile(r"[`\"']")


def extract_jobs_from_message(message: Dict[str, Any]) -> List[Dict[str, Any]]:
    embeds = message.get("embeds") or []
    jobs: List[Dict[str, Any]] = []
    for embed in embeds:
        fields = embed.get("fields") or []
        job_data: Dict[str, Any] = {}
        for field in fields:
            name = (field.get("name") or "").lower()
            value = field.get("value") or ""
            if "job" in name and "id" in name:
                job_data["job_id"] = _jobid_cleanup.sub("", value)
            elif "money" in name:
                match = _money_pattern.search(value)
                if match:
                    job_data["money"] = match.group(1)
            elif "players" in name:
                match = _players_pattern.search(value)
                if match:
                    job_data["players"] = match.group(1)
                    job_data["players_max"] = match.group(2)
            elif "name" in name:
                job_data["name"] = value.strip()
        if job_data.get("job_id") and job_data.get("money"):
            job_data.setdefault("players", 0)
            job_data.setdefault("players_max", 0)
            job_data.setdefault("name", "")
            job_data["ts"] = int(time.time())
            try:
                job_payload = JobPayload(**job_data)
                jobs.append(job_payload.dict())
            except Exception as exc:  # pragma: no cover
                logger.debug("discarded embed job due to validation: %s", exc)
    return jobs


async def discord_monitor() -> None:
    if not CHANNEL_ID or not DISCORD_TOKEN:
        logger.info("discord monitor disabled (missing CHANNEL_ID or DISCORD_TOKEN)")
        return

    url = f"{DISCORD_API_BASE}/channels/{CHANNEL_ID}/messages"
    last_seen: Optional[str] = None
    logger.info("discord monitor started")
    try:
        while True:
            params: Dict[str, Any] = {"limit": 50}
            if last_seen:
                params["after"] = last_seen

            def _poll() -> requests.Response:
                return discord_session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            try:
                response = await asyncio.to_thread(_poll)
            except Exception as exc:  # pragma: no cover
                logger.debug("discord poll failed: %s", exc)
                await asyncio.sleep(DISCORD_POLL_SECONDS)
                continue

            if response.status_code == 429:
                retry_after = DISCORD_POLL_SECONDS
                try:
                    data = response.json()
                    retry_after = float(data.get("retry_after", retry_after))
                except Exception:  # pragma: no cover
                    pass
                await asyncio.sleep(max(retry_after, DISCORD_POLL_SECONDS))
                continue

            if response.status_code >= 400:
                logger.warning("discord poll error %s: %s", response.status_code, response.text)
                await asyncio.sleep(DISCORD_POLL_SECONDS)
                continue

            try:
                payload = response.json()
            except Exception as exc:  # pragma: no cover
                logger.debug("discord poll json decode error: %s", exc)
                await asyncio.sleep(DISCORD_POLL_SECONDS)
                continue

            if not isinstance(payload, list) or not payload:
                await asyncio.sleep(DISCORD_POLL_SECONDS)
                continue

            try:
                payload.sort(key=lambda item: int(item.get("id", "0")))
            except Exception:  # pragma: no cover
                pass

            burst_mode = False
            for message in payload:
                message_id = message.get("id")
                if message_id:
                    last_seen = message_id
                jobs = extract_jobs_from_message(message)
                if jobs:
                    burst_mode = True
                for job in jobs:
                    enqueue_success = await enqueue_job(job)
                    if enqueue_success:
                        asyncio.create_task(post_job_non_blocking(job))

            await asyncio.sleep(BURST_POLL_SECONDS if burst_mode else DISCORD_POLL_SECONDS)
    except asyncio.CancelledError:  # pragma: no cover
        raise
    finally:
        logger.info("discord monitor stopped")


@app.on_event("startup")
async def on_startup() -> None:
    global discord_task
    logger.info("api server started")
    if HEADLESS_MODE:
        logger.info("headless mode: %s", HEADLESS_MODE)
    if CHANNEL_ID and DISCORD_TOKEN:
        discord_task = asyncio.create_task(discord_monitor())
    else:
        logger.warning("discord monitor not started (set CHANNEL_ID and DISCORD_TOKEN)")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global discord_task
    if discord_task:
        discord_task.cancel()
        with suppress(asyncio.CancelledError):  # pragma: no cover
            await discord_task
        discord_task = None
    discord_session.close()
    api_session.close()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
