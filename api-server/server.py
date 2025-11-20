import json
import os
import threading
from typing import Optional

from flask import Flask, jsonify, request

app = Flask(__name__)

JOBS_FILE_PATH = os.environ.get("JOBS_FILE_PATH", "jobs.json")
_jobs_lock = threading.Lock()
_latest_job: Optional[dict] = None


def _load_latest_job_from_disk() -> Optional[dict]:
    """قراءة آخر وظيفة محفوظة من القرص (إن وجدت)."""
    if not os.path.exists(JOBS_FILE_PATH):
        return None
    try:
        with open(JOBS_FILE_PATH, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return None


def _persist_job(job: dict) -> None:
    """حفظ آخر وظيفة على القرص لضمان الاستمرارية."""
    try:
        with open(JOBS_FILE_PATH, "w", encoding="utf-8") as fp:
            json.dump(job, fp, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _validate_job(data: dict) -> dict:
    required = {"job_id", "money", "name", "players", "timestamp"}
    missing = required - data.keys()
    if missing:
        raise ValueError(f"missing fields: {', '.join(sorted(missing))}")

    try:
        data["money"] = float(data["money"])
        data["timestamp"] = int(data["timestamp"])
    except Exception as exc:
        raise ValueError(f"invalid numeric field: {exc}")

    data["job_id"] = str(data["job_id"])
    data["name"] = str(data["name"])
    data["players"] = str(data["players"])
    return data


cached_job = _load_latest_job_from_disk()
if cached_job:
    _latest_job = cached_job


@app.route("/job", methods=["POST"])
def post_job():
    """استقبال البيانات من الجسر وحفظها."""
    if not request.is_json:
        return jsonify({"error": "invalid_json"}), 400

    try:
        payload = _validate_job(dict(request.get_json()))
    except ValueError as exc:
        return jsonify({"error": "bad_request", "detail": str(exc)}), 400

    with _jobs_lock:
        global _latest_job
        _latest_job = payload
        _persist_job(payload)

    return jsonify({"status": "ok"})


@app.route("/job/latest", methods=["GET"])
def get_latest_job():
    """إرجاع آخر وظيفة محفوظة للـ Roblox."""
    with _jobs_lock:
        job = _latest_job.copy() if _latest_job else None
    return jsonify({"job": job})


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(exc: Exception):
    return jsonify({"error": "internal_error", "detail": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
