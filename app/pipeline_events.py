"""Thread-safe lifecycle events and compact per-run aggregate metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from queue import Empty, Queue
import time
from typing import Any


class PipelineEventBus:
    """Workers publish data only; Streamlit consumes on its main thread."""

    def __init__(self) -> None:
        self._events: Queue[dict[str, Any]] = Queue()

    def emit(self, kind: str, *, run_id: str = "", filename: str = "", count: int = 1,
             message: str = "", **fields: Any) -> None:
        self._events.put({
            "kind": kind, "run_id": run_id, "filename": filename, "count": max(0, int(count)),
            "message": message, "timestamp": time.time(), **fields,
        })

    def drain(self, run_id: str) -> list[dict[str, Any]]:
        """Return this run's events while retaining asynchronous work from others."""
        accepted: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        while True:
            try:
                event = self._events.get_nowait()
            except Empty:
                break
            if event.get("run_id") in {"", run_id}:
                accepted.append(event)
            else:
                deferred.append(event)
        for event in deferred:
            self._events.put(event)
        return accepted


@dataclass
class RunMetrics:
    """Aggregate counters derived only from lifecycle events, never UI calls."""

    existing_downloads: int = 0
    discovered: int = 0
    stage1_approved: int = 0
    stage1_rejected: int = 0
    download_queued: int = 0
    downloading: int = 0
    downloaded_now: int = 0
    recovery_queued: int = 0
    failed_for_round: int = 0
    permanently_failed: int = 0
    stage2_queued: int = 0
    stage2_running: int = 0
    stage2_approved: int = 0
    stage2_rejected: int = 0
    stage2_pending: int = 0
    duplicates: int = 0
    event_count: int = 0
    recent_events: list[str] = field(default_factory=list)

    def consume(self, event: dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        count = max(0, int(event.get("count", 1) or 0))
        self.event_count += 1
        message = str(event.get("message") or "").strip()
        if message:
            self.recent_events.append(message)
            del self.recent_events[:-12]
        if kind == "DOCUMENT_DISCOVERED":
            self.discovered += count
        elif kind == "STAGE1_APPROVED":
            self.stage1_approved += count
        elif kind == "STAGE1_REJECTED":
            self.stage1_rejected += count
        elif kind == "DOWNLOAD_QUEUED":
            self.download_queued += count
        elif kind == "DOWNLOAD_STARTED":
            self.downloading += count
        elif kind == "DOWNLOAD_COMPLETED":
            self.downloading = max(0, self.downloading - count)
            self.downloaded_now += count
        elif kind == "DOWNLOAD_FAILED_FOR_ROUND":
            self.downloading = max(0, self.downloading - count)
            self.failed_for_round += count
        elif kind == "DOWNLOAD_PERMANENTLY_FAILED":
            self.downloading = max(0, self.downloading - count)
            self.permanently_failed += count
        elif kind == "RECOVERY_QUEUED":
            self.recovery_queued += count
        elif kind == "STAGE2_QUEUED":
            self.stage2_queued += count
        elif kind == "STAGE2_STARTED":
            self.stage2_queued = max(0, self.stage2_queued - count)
            self.stage2_running += count
        elif kind == "STAGE2_APPROVED":
            self.stage2_running = max(0, self.stage2_running - count)
            self.stage2_approved += count
        elif kind == "STAGE2_REJECTED":
            self.stage2_running = max(0, self.stage2_running - count)
            self.stage2_rejected += count
        elif kind in {"STAGE2_PENDING", "STAGE2_DUPLICATE_PENDING"}:
            self.stage2_running = max(0, self.stage2_running - count)
            self.stage2_pending += count
        elif kind == "DUPLICATE_DETECTED":
            self.duplicates += count

    @property
    def total_downloaded(self) -> int:
        return self.existing_downloads + self.downloaded_now

    def summary(self, target: int) -> str:
        remaining = max(0, target - self.total_downloaded)
        return (
            f"Live pipeline · discovered {self.discovered} · Stage 1 ✓ {self.stage1_approved} / ✕ {self.stage1_rejected} · "
            f"downloads {self.total_downloaded}/{target} ({remaining} remaining; {self.downloading} active) · "
            f"recovery {self.recovery_queued} · Stage 2 ✓ {self.stage2_approved} / ✕ {self.stage2_rejected} / "
            f"pending {self.stage2_pending + self.stage2_queued + self.stage2_running} · duplicates {self.duplicates}"
        )
