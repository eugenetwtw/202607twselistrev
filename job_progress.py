"""執行中任務的即時進度／活動日誌（供前端小視窗輪詢）。

設計重點：
- 長 API 等待期間每 ~1.2s 打一筆 tick（證明還活著）
- 每次 log 寫入 data/job_live.json，前端可看到
- 回傳摘要可塞進 logs，使用者看得到 API 吐了什麼
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

TZ_TW = timezone(timedelta(hours=8))
APP_DIR = Path(__file__).resolve().parent
LIVE_PATH = APP_DIR / "data" / "job_live.json"

_lock = threading.Lock()
_job: dict[str, Any] | None = None
_seq = 0


def _now() -> str:
    return datetime.now(TZ_TW).strftime("%H:%M:%S")


def _now_iso() -> str:
    return datetime.now(TZ_TW).isoformat(timespec="seconds")


def _persist_unlocked() -> None:
    if not _job:
        return
    try:
        LIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
        snap = dict(_job)
        snap["logs"] = list(_job.get("logs") or [])[-120:]
        snap["server_time"] = _now_iso()
        snap["heartbeat_age_sec"] = round(time.time() - float(_job.get("heartbeat") or time.time()), 2)
        LIVE_PATH.write_text(json.dumps(snap, ensure_ascii=False, indent=0), encoding="utf-8")
    except Exception:
        pass


def get_job() -> dict[str, Any] | None:
    with _lock:
        if _job is None:
            # try disk
            if LIVE_PATH.is_file():
                try:
                    return json.loads(LIVE_PATH.read_text(encoding="utf-8"))
                except Exception:
                    return None
            return None
        j = dict(_job)
        logs = list(_job.get("logs") or [])
        j["logs"] = logs
        j["recent"] = logs[-100:]
        j["server_time"] = _now_iso()
        j["heartbeat_age_sec"] = round(
            time.time() - float(_job.get("heartbeat") or time.time()), 2
        )
        j["alive"] = j["status"] == "running" and j["heartbeat_age_sec"] < 15
        return j


def is_busy() -> bool:
    with _lock:
        return bool(_job and _job.get("status") == "running")


def request_cancel() -> bool:
    with _lock:
        if not _job or _job.get("status") != "running":
            return False
        _job["cancel"] = True
        _append_unlocked("warn", "⏹ 使用者要求取消（等目前 API 呼叫結束）…")
        return True


def should_cancel() -> bool:
    with _lock:
        return bool(_job and _job.get("cancel"))


def _append_unlocked(level: str, message: str, **extra: Any) -> None:
    global _seq
    if not _job:
        return
    _seq += 1
    entry = {
        "id": _seq,
        "t": _now(),
        "level": level,  # info | api | ok | warn | error | phase | tick | data
        "msg": str(message)[:2000],
    }
    if extra:
        entry["extra"] = {k: v for k, v in extra.items() if v is not None}
    logs = _job.setdefault("logs", [])
    logs.append(entry)
    if len(logs) > 800:
        del logs[:-600]
    _job["updated_at"] = _now_iso()
    _job["heartbeat"] = time.time()
    _job["tick"] = int(_job.get("tick") or 0) + 1
    # tick lines don't overwrite headline message
    if level != "tick" and message:
        _job["message"] = str(message)[:500]
    _persist_unlocked()


def log(level: str, message: str, **extra: Any) -> None:
    with _lock:
        _append_unlocked(level, message, **extra)


def pulse(label: str = "alive") -> None:
    """Lightweight heartbeat without changing headline (unless idle)."""
    with _lock:
        if not _job or _job.get("status") != "running":
            return
        _job["heartbeat"] = time.time()
        _job["updated_at"] = _now_iso()
        _job["pulse_label"] = label
        _persist_unlocked()


def set_progress(
    *,
    pct: float | None = None,
    phase: str | None = None,
    current: int | None = None,
    total: int | None = None,
    company_id: str | None = None,
    company_name: str | None = None,
    message: str | None = None,
    waiting_for: str | None = None,
) -> None:
    with _lock:
        if not _job:
            return
        if pct is not None:
            _job["pct"] = max(0, min(100, float(pct)))
        if phase is not None:
            _job["phase"] = phase
        if current is not None:
            _job["current"] = current
        if total is not None:
            _job["total"] = total
        if company_id is not None:
            _job["company_id"] = company_id
        if company_name is not None:
            _job["company_name"] = company_name
        if waiting_for is not None:
            _job["waiting_for"] = waiting_for
        if message is not None:
            _job["message"] = message
            _append_unlocked("info", message)
        else:
            _job["updated_at"] = _now_iso()
            _job["heartbeat"] = time.time()
            _persist_unlocked()


def start_job(kind: str, meta: dict[str, Any] | None = None) -> str:
    """Create a new running job. Raises if busy."""
    global _job, _seq
    with _lock:
        if _job and _job.get("status") == "running":
            # if stuck cancelled > 2 min, force clear
            age = time.time() - float(_job.get("heartbeat") or 0)
            if _job.get("cancel") and age > 90:
                _append_unlocked("warn", "強制結束卡住的舊任務")
                _job["status"] = "cancelled"
            else:
                raise RuntimeError("已有任務進行中")
        _seq = 0
        job_id = uuid.uuid4().hex[:10]
        _job = {
            "id": job_id,
            "kind": kind,
            "status": "running",
            "cancel": False,
            "pct": 0.0,
            "phase": "start",
            "current": 0,
            "total": 0,
            "company_id": "",
            "company_name": "",
            "message": "啟動中…",
            "waiting_for": "",
            "ok": 0,
            "fail": 0,
            "tick": 0,
            "started_at": _now_iso(),
            "updated_at": _now_iso(),
            "heartbeat": time.time(),
            "meta": meta or {},
            "result": None,
            "logs": [],
        }
        _append_unlocked("phase", f"▶ 任務開始（{kind}） id={job_id}")
        return job_id


def finish_job(
    status: str = "done",
    *,
    message: str | None = None,
    result: Any = None,
) -> None:
    with _lock:
        if not _job:
            return
        _job["status"] = status  # done | error | cancelled
        _job["waiting_for"] = ""
        _job["pct"] = 100.0 if status == "done" else _job.get("pct") or 0
        if message:
            _job["message"] = message
            level = "ok" if status == "done" else ("warn" if status == "cancelled" else "error")
            _append_unlocked(level, message)
        if result is not None:
            # strip huge raw fields for status payload
            _job["result"] = result
        _job["finished_at"] = _now_iso()
        _job["updated_at"] = _now_iso()
        _job["heartbeat"] = time.time()
        _persist_unlocked()


def bump_ok(n: int = 1) -> None:
    with _lock:
        if _job:
            _job["ok"] = int(_job.get("ok") or 0) + n
            _persist_unlocked()


def bump_fail(n: int = 1) -> None:
    with _lock:
        if _job:
            _job["fail"] = int(_job.get("fail") or 0) + n
            _persist_unlocked()


def make_step_callback() -> Callable[..., None]:
    """Callback for analysis_service: on_step(level, msg, **extra)."""

    def on_step(level: str, message: str, **extra: Any) -> None:
        log(level or "info", message, **extra)
        if extra.get("pct_hint") is not None:
            set_progress(pct=float(extra["pct_hint"]), message=None)
        if extra.get("waiting_for") is not None:
            set_progress(waiting_for=str(extra["waiting_for"]), message=None)

    return on_step


class WaitTicker:
    """Context manager: while waiting on a slow API, keep printing ticks."""

    def __init__(self, label: str, *, interval: float = 1.2):
        self.label = label
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0 = 0.0

    def __enter__(self) -> "WaitTicker":
        self._t0 = time.time()
        set_progress(waiting_for=self.label, message=None)
        log("api", f"⏳ 開始等待 · {self.label}")
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"tick-{self.label[:20]}")
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=0.5)
        elapsed = int(time.time() - self._t0)
        set_progress(waiting_for="", message=None)
        if elapsed >= 1:
            log("tick", f"✓ 等待結束 · {self.label} · 共 {elapsed}s")

    def _run(self) -> None:
        n = 0
        while not self._stop.wait(self.interval):
            n += 1
            elapsed = int(time.time() - self._t0)
            # rotating dots so UI clearly scrolls
            dots = "." * (1 + (n % 3))
            log(
                "tick",
                f"💓 還活著 · 等待 {self.label}{dots} · 已 {elapsed}s · tick#{n}",
            )
            pulse(self.label)
