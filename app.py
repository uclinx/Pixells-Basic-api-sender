import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import time
from collections import deque
from contextlib import suppress
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import aiohttp
import orjson
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import ORJSONResponse, Response
from pydantic import BaseModel, validator
from redis.asyncio import Redis


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
INTERNAL_REPOST_ENABLED = as_bool(setting("INTERNAL_REPOST_ENABLED", "false"), default=False)
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


class LatestJobStore:
    def __init__(self, redis_client: Optional[Redis], redis_key: str) -> None:
        self._redis = redis_client
        self._redis_key = redis_key
        self._lock = asyncio.Lock()
        self._queue: Deque[Dict[str, Any]] = deque()

    async def enqueue(self, job: Dict[str, Any]) -> None:
        # Try Redis first; on failure, fall back to in-memory queue to avoid job loss.
        if self._redis:
            payload = orjson.dumps(job).decode()
            try:
                await self._redis.lpush(self._redis_key, payload)
                return
            except Exception as exc:  # pragma: no cover
                logger.error("redis enqueue failed, falling back to memory queue: %s", exc)

        async with self._lock:
            self._queue.append(job.copy())

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        # Read from Redis if configured; if empty or on error, also check in-memory queue.
        if self._redis:
            try:
                raw = await self._redis.rpop(self._redis_key)
            except Exception as exc:  # pragma: no cover
                logger.error("redis dequeue failed, falling back to memory queue: %s", exc)
            else:
                if raw is not None:
                    try:
                        return orjson.loads(raw)
                    except orjson.JSONDecodeError:  # pragma: no cover
                        logger.warning("discarded malformed job payload from redis")
                # If raw is None or malformed, continue to in-memory queue.

        async with self._lock:
            if not self._queue:
                return None
            return self._queue.popleft()


redis_client: Optional[Redis] = None
job_store = LatestJobStore(None, "roblox_job_queue")
connected_clients: Set[WebSocket] = set()
clients_lock = asyncio.Lock()
broadcast_lock = asyncio.Lock()
recent_job_ids: deque[str] = deque(maxlen=RECENT_JOB_IDS_LIMIT)
recent_ids_lock = asyncio.Lock()

INTERNAL_REQUEST_HEADER = "X-Internal-Request"
local_rate_limits: Dict[str, Tuple[int, float]] = {}
local_rate_lock = asyncio.Lock()

default_discord_headers = {
    "User-Agent": "RobloxJobGateway/1.0",
}
if DISCORD_TOKEN:
    default_discord_headers["Authorization"] = DISCORD_TOKEN

if not API_KEY:
    raise RuntimeError("API_KEY must be configured for secure access.")

HMAC_SECRET = setting("HMAC_SECRET")
if not HMAC_SECRET:
    logger.warning("HMAC_SECRET missing; defaulting to API_KEY. Set HMAC_SECRET for stronger security.")
    HMAC_SECRET = API_KEY
HMAC_SECRET_BYTES = HMAC_SECRET.encode()

TIMESTAMP_HEADER = "X-TIMESTAMP"
SIGNATURE_HEADER = "X-SIGNATURE"
TIMESTAMP_TOLERANCE = int(setting("TIMESTAMP_TOLERANCE", "60"))
RATE_LIMIT_PER_MINUTE = int(setting("RATE_LIMIT_PER_MINUTE", "0"))
RATE_LIMIT_WINDOW_SECONDS = int(setting("RATE_LIMIT_WINDOW_SECONDS", "60"))

app = FastAPI(title="Roblox Job Gateway", docs_url=None, redoc_url=None)

discord_session: Optional[aiohttp.ClientSession] = None
internal_session: Optional[aiohttp.ClientSession] = None
discord_task: Optional[asyncio.Task] = None


def normalize_job_id(value: Any) -> str:
    """Normalise job_id values for consistent deduplication."""
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    cleaned = value.strip()
    if cleaned and cleaned[0] in {"`", '"', "'"} and cleaned[-1] == cleaned[0]:
        cleaned = cleaned[1:-1].strip()
    return cleaned


async def remember_job(job_id: str) -> bool:
    """Backwards-compatible helper for recording recently seen job_ids.

    New code should use enqueue_job for atomic enqueue+dedupe behaviour.
    """
    normalized = normalize_job_id(job_id)
    if not normalized:
        return False
    async with recent_ids_lock:
        if normalized in recent_job_ids:
            return False
        recent_job_ids.append(normalized)
        return True


def verify_signature(timestamp: str, signature: str) -> None:
    try:
        ts_value = int(timestamp)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid timestamp") from exc

    now = int(time.time())
    if abs(now - ts_value) > TIMESTAMP_TOLERANCE:
        raise HTTPException(status_code=400, detail="Timestamp outside tolerance")

    expected = hmac.new(HMAC_SECRET_BYTES, timestamp.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=403, detail="Invalid signature")


def _client_identifier(request: Request) -> str:
    """Return a stable identifier string for rate limiting.

    FIX: restored helper used by rate_limit_middleware that was accidentally
    dropped, to keep rate limiting and security behaviour intact.
    """
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.headers.get(INTERNAL_REQUEST_HEADER) == "1":
        return await call_next(request)

    if RATE_LIMIT_PER_MINUTE <= 0 or RATE_LIMIT_WINDOW_SECONDS <= 0:
        return await call_next(request)

    identifier = _client_identifier(request)

    if redis_client is not None:
        key = f"rate:{identifier}"
        try:
            count = await redis_client.incr(key)
            if count == 1:
                await redis_client.expire(key, RATE_LIMIT_WINDOW_SECONDS)
            if count > RATE_LIMIT_PER_MINUTE:
                ttl = await redis_client.ttl(key)
                retry_after = ttl if ttl and ttl > 0 else RATE_LIMIT_WINDOW_SECONDS
                return ORJSONResponse(
                    {"detail": "Too many requests"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            return await call_next(request)
        except Exception as exc:  # pragma: no cover
            logger.debug("redis rate limit fallback: %s", exc)

    async with local_rate_lock:
        now = time.time()
        count, reset = local_rate_limits.get(identifier, (0, now + RATE_LIMIT_WINDOW_SECONDS))
        if now >= reset:
            local_rate_limits[identifier] = (1, now + RATE_LIMIT_WINDOW_SECONDS)
        else:
            if count + 1 > RATE_LIMIT_PER_MINUTE:
                retry_after = int(max(1, reset - now))
                return ORJSONResponse(
                    {"detail": "Too many requests"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            local_rate_limits[identifier] = (count + 1, reset)

    return await call_next(request)


async def broadcast_job(job: Dict[str, Any]) -> None:
    async with clients_lock:
        clients_snapshot = list(connected_clients)
    if not clients_snapshot:
        return
    payload = orjson.dumps(job).decode()
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
    """FIX: Atomic enqueue under a single lock so no jobs are lost.

    Dedupe is now *non-blocking*: even if a job_id was seen before, the job
    is still enqueued and delivered. The recent ID set is kept only for
    diagnostics/metrics and does not filter jobs.
    """
    raw_job_id = job_obj.get("job_id", "")
    job_id = normalize_job_id(raw_job_id)
    if not job_id:
        logger.warning("received job without job_id; dropping: %r", job_obj)
        return False

    # Ensure the canonical job_id is stored in the payload.
    job_obj = dict(job_obj)
    job_obj["job_id"] = job_id

    async with recent_ids_lock:
        is_duplicate = job_id in recent_job_ids
        try:
            await job_store.enqueue(job_obj)
        except Exception as exc:  # pragma: no cover
            logger.error("failed to enqueue job %s: %s", job_id, exc)
            return False
        recent_job_ids.append(job_id)

    if is_duplicate:
        logger.info("duplicate job %s enqueued again", job_id)
    else:
        logger.info("job enqueued: %s", job_id)
    asyncio.create_task(broadcast_job(job_obj))
    return True


async def dequeue_job() -> Optional[Dict[str, Any]]:
    return await job_store.dequeue()


async def authenticate_request(request: Request) -> None:
    """Authenticate external clients using API key + HMAC.

    FIX: restore original authentication hook so all endpoints keep the
    expected security guarantees.
    """
    if not API_KEY:
        return
    provided = request.headers.get(API_KEY_HEADER)
    if provided != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    timestamp = request.headers.get(TIMESTAMP_HEADER)
    signature = request.headers.get(SIGNATURE_HEADER)
    if not timestamp or not signature:
        raise HTTPException(status_code=400, detail="Missing signature headers")
    verify_signature(timestamp, signature)


@app.get("/health", dependencies=[Depends(authenticate_request)])
async def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/job", dependencies=[Depends(authenticate_request)])
async def add_job(payload: JobPayload) -> Dict[str, Any]:
    job_data = payload.dict()
    enqueued = await enqueue_job(job_data)
    return {"ok": True, "enqueued": enqueued}


@app.get("/job/next", dependencies=[Depends(authenticate_request)])
async def get_next_job() -> Response:
    try:
        job = await dequeue_job()
    except Exception as exc:  # pragma: no cover
        logger.error("failed to dequeue job: %s", exc)
        return Response(status_code=204)
    if job is None:
        return Response(status_code=204)
    logger.info("job dequeued: %s", job.get("job_id", "?"))
    return ORJSONResponse(job)


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
    timestamp = (
        websocket.headers.get(TIMESTAMP_HEADER)
        or websocket.query_params.get("ts")
    )
    signature = (
        websocket.headers.get(SIGNATURE_HEADER)
        or websocket.query_params.get("sig")
    )
    if not timestamp or not signature:
        await websocket.close(code=1008)
        logger.warning("websocket rejected due to missing signature")
        return
    try:
        verify_signature(timestamp, signature)
    except HTTPException:
        await websocket.close(code=1008)
        logger.warning("websocket rejected due to invalid signature")
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
    if not job or internal_session is None:
        return
    base = SERVICE_BASE_URL.rstrip("/")
    url = f"{base}/job"
    headers = {
        API_KEY_HEADER: API_KEY,
        TIMESTAMP_HEADER: str(int(time.time())),
    }
    headers[SIGNATURE_HEADER] = hmac.new(
        HMAC_SECRET_BYTES,
        headers[TIMESTAMP_HEADER].encode(),
        hashlib.sha256,
    ).hexdigest()
    headers[INTERNAL_REQUEST_HEADER] = "1"

    try:
        async with internal_session.post(url, json=job, timeout=REQUEST_TIMEOUT, headers=headers) as response:
            if response.status >= 400:
                text = await response.text()
                logger.debug("internal job post failed: %s %s", response.status, text)
    except Exception as exc:  # pragma: no cover
        logger.debug("internal job post exception: %s", exc)


_money_pattern = re.compile(r"(\d+[\d,]*\.?\d*)")
_players_pattern = re.compile(r"(\d+)\s*/\s*(\d+)")
_jobid_cleanup = re.compile(r"[`\"']")


def extract_jobs_from_message(message: Dict[str, Any]) -> List[Dict[str, Any]]:
    embeds = message.get("embeds") or []
    jobs: List[Dict[str, Any]] = []
    for embed in embeds:
        fields = embed.get("fields") or []
        job_data: Dict[str, Any] = {}
        # Primary parse: standard field-based embeds.
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

        # FIX: some servers put job_id/money in title/description/footer instead
        # of fields; use a combined text view as a robust fallback so the last
        # message format still yields a valid job.
        if not job_data.get("job_id") or not job_data.get("money"):
            text_chunks: List[str] = []
            title = embed.get("title") or ""
            description = embed.get("description") or ""
            footer_text = (embed.get("footer") or {}).get("text") or ""
            if title:
                text_chunks.append(str(title))
            if description:
                text_chunks.append(str(description))
            if footer_text:
                text_chunks.append(str(footer_text))
            for field in fields:
                field_val = field.get("value")
                if field_val:
                    text_chunks.append(str(field_val))
            combined = "\n".join(chunk for chunk in text_chunks if chunk)

            if combined:
                if not job_data.get("job_id"):
                    job_id_match = re.search(r"job\s*id\s*[:#-]?\s*([^\s]+)", combined, re.IGNORECASE)
                    if job_id_match:
                        job_data["job_id"] = _jobid_cleanup.sub("", job_id_match.group(1))

                if not job_data.get("money"):
                    money_match = _money_pattern.search(combined)
                    if money_match:
                        job_data["money"] = money_match.group(1)

                if "players" not in job_data or "players_max" not in job_data:
                    players_match = _players_pattern.search(combined)
                    if players_match:
                        job_data["players"] = players_match.group(1)
                        job_data["players_max"] = players_match.group(2)

                if "name" not in job_data:
                    if title:
                        job_data["name"] = str(title).strip()
                    elif description:
                        job_data["name"] = str(description).strip().splitlines()[0][:128]

        # Normalise job_id before validation to avoid false duplicates due to
        # quoting/spacing differences between embed variants.
        if job_data.get("job_id"):
            job_data["job_id"] = normalize_job_id(job_data["job_id"])

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
    if not CHANNEL_ID or not DISCORD_TOKEN or not discord_session:
        logger.info("discord monitor disabled (missing CHANNEL_ID or DISCORD_TOKEN)")
        return

    url = f"{DISCORD_API_BASE}/channels/{CHANNEL_ID}/messages"
    last_seen: Optional[str] = None
    logger.info("discord monitor started")
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT[1])
    try:
        while True:
            params: Dict[str, Any] = {"limit": 50}
            if last_seen:
                params["after"] = last_seen
            try:
                async with discord_session.get(url, params=params, timeout=timeout) as response:
                    if response.status == 429:
                        retry_after = DISCORD_POLL_SECONDS
                        try:
                            payload = await response.json()
                            retry_after = float(payload.get("retry_after", retry_after))
                        except Exception:  # pragma: no cover
                            pass
                        await asyncio.sleep(max(retry_after, DISCORD_POLL_SECONDS))
                        continue

                    if response.status >= 400:
                        text = await response.text()
                        logger.warning("discord poll error %s: %s", response.status, text)
                        await asyncio.sleep(DISCORD_POLL_SECONDS)
                        continue

                    try:
                        payload = await response.json()
                    except Exception as exc:  # pragma: no cover
                        logger.debug("discord poll json decode error: %s", exc)
                        await asyncio.sleep(DISCORD_POLL_SECONDS)
                        continue
            except Exception as exc:  # pragma: no cover
                logger.debug("discord poll failed: %s", exc)
                await asyncio.sleep(DISCORD_POLL_SECONDS)
                continue

            if not isinstance(payload, list) or not payload:
                await asyncio.sleep(0)
                continue

            pending_jobs: List[Dict[str, Any]] = []
            sorted_messages = sorted(
                payload,
                key=lambda item: int(item.get("id", "0")),
            )

            for message in sorted_messages:
                jobs = extract_jobs_from_message(message)
                if jobs:
                    pending_jobs.extend(jobs)

            last_message = sorted_messages[-1]
            last_message_id = last_message.get("id")
            if last_message_id:
                last_seen = last_message_id

            for job in pending_jobs:
                enqueue_success = await enqueue_job(job)
                if enqueue_success and INTERNAL_REPOST_ENABLED:
                    asyncio.create_task(post_job_non_blocking(job))

            await asyncio.sleep(0)
    except asyncio.CancelledError:  # pragma: no cover
        raise
    finally:
        logger.info("discord monitor stopped")


@app.on_event("startup")
async def on_startup() -> None:
    global discord_task
    logger.info("api server started")
    global redis_client, job_store, discord_session, internal_session
    if REDIS_URL:
        try:
            redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
            job_store = LatestJobStore(redis_client, "roblox_job_queue")
            logger.info("redis queue backend enabled")
        except Exception as exc:  # pragma: no cover
            logger.warning("failed to init redis, falling back to memory: %s", exc)
            redis_client = None
            job_store = LatestJobStore(None, "roblox_job_queue")
    else:
        logger.info("in-memory queue backend enabled")

    discord_session = aiohttp.ClientSession(headers=default_discord_headers)
    internal_session = aiohttp.ClientSession()

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
    if discord_session:
        await discord_session.close()
    if internal_session:
        await internal_session.close()
    if redis_client:
        try:
            await redis_client.close()
        except Exception:  # pragma: no cover
            pass


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
