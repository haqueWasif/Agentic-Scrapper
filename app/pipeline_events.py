"""Thread-safe lifecycle events and compact per-run aggregate metrics."""

from __future__ import annotations

from collections import deque
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
class DownloadPresentation:
    """Bounded, main-thread view state for network worker telemetry.

    This deliberately stores presentation state only.  Durable download state
    remains ``downloads.json`` and worker threads only publish dictionaries.
    """

    max_workers: int
    activity_limit: int = 80
    debug_limit: int = 500
    workers: dict[int, dict[str, Any]] = field(default_factory=dict)
    activity: deque[str] = field(init=False)
    debug: deque[str] = field(init=False)

    def __post_init__(self) -> None:
        self.activity = deque(maxlen=max(1, self.activity_limit))
        self.debug = deque(maxlen=max(1, self.debug_limit))

    @staticmethod
    def _short(event: dict[str, Any]) -> str:
        message = str(event.get("short_message") or event.get("message") or "").strip()
        message = message.replace("\n", " · ")
        return message[:220] or str(event.get("kind") or "status")

    def consume(self, event: dict[str, Any]) -> None:
        """Update one worker row without ever rendering or retaining URL noise."""
        level = str(event.get("level") or "STATUS").upper()
        kind = str(event.get("kind") or "status").lower()
        worker_id = int(event.get("worker") or event.get("worker_id") or 0)
        short = self._short(event)
        detail = str(event.get("debug_message") or event.get("message") or "").strip()
        if detail and level in {"DEBUG", "TRACE"}:
            self.debug.append(detail[:2000])
        if level in {"STATUS", "LIFECYCLE"} and short:
            self.activity.append(short)
        if worker_id < 1:
            return
        row = self.workers.setdefault(worker_id, {"worker": worker_id, "state": "IDLE"})
        for key in (
            "filename", "document_id", "source_page", "source_item", "work_kind", "document_attempt",
            "max_attempts", "gateway_host", "bytes_downloaded", "total_bytes", "state", "short_message",
        ):
            if key in event and event[key] is not None:
                row[key] = event[key]
        row["last_event"] = short
        if kind == "progress":
            row["bytes_downloaded"] = int(event.get("downloaded", event.get("bytes_downloaded", 0)) or 0)
            row["total_bytes"] = int(event.get("total", event.get("total_bytes", 0)) or 0)
            row["state"] = event.get("state") or "MAKING_PROGRESS"
        elif kind == "success":
            row["state"] = "COMPLETED"
        elif kind == "failure":
            row["state"] = event.get("state") or "FAILED_FOR_ROUND"

    def worker_rows(self) -> list[dict[str, Any]]:
        return [self.workers.get(index, {"worker": index, "state": "IDLE"}) for index in range(1, self.max_workers + 1)]


@dataclass
class RunMetrics:
    """Aggregate counters derived only from lifecycle events, never UI calls."""

    existing_downloads: int = 0
    discovered: int = 0
    stage1_pending: int = 0
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
    stage2_invalid: int = 0
    duplicates: int = 0
    event_count: int = 0
    recent_events: list[str] = field(default_factory=list)
    _stage2_inflight: dict[str, str] = field(default_factory=dict, repr=False)

    def _set_stage2_state(self, filename: str, state: str) -> None:
        """Transition one file between queued, running, and pending exactly once."""
        previous = self._stage2_inflight.get(filename)
        if previous == state:
            return
        if previous == "queued":
            self.stage2_queued = max(0, self.stage2_queued - 1)
        elif previous == "running":
            self.stage2_running = max(0, self.stage2_running - 1)
        elif previous == "pending":
            self.stage2_pending = max(0, self.stage2_pending - 1)
        self._stage2_inflight[filename] = state
        if state == "queued":
            self.stage2_queued += 1
        elif state == "running":
            self.stage2_running += 1
        elif state == "pending":
            self.stage2_pending += 1

    def _complete_stage2(self, filename: str) -> None:
        previous = self._stage2_inflight.pop(filename, None)
        if previous == "queued":
            self.stage2_queued = max(0, self.stage2_queued - 1)
        elif previous == "running":
            self.stage2_running = max(0, self.stage2_running - 1)
        elif previous == "pending":
            self.stage2_pending = max(0, self.stage2_pending - 1)

    def consume(self, event: dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        filename = str(event.get("filename") or "")
        count = max(0, int(event.get("count", 1) or 0))
        self.event_count += 1
        message = str(event.get("message") or "").strip()
        if message:
            self.recent_events.append(message)
            del self.recent_events[:-12]
        if kind == "DOCUMENT_DISCOVERED":
            self.discovered += count
        elif kind == "STAGE1_STARTED":
            self.stage1_pending += count
        elif kind == "STAGE1_APPROVED":
            self.stage1_pending = max(0, self.stage1_pending - count)
            self.stage1_approved += count
        elif kind == "STAGE1_REJECTED":
            self.stage1_pending = max(0, self.stage1_pending - count)
            self.stage1_rejected += count
        elif kind == "STAGE1_FAILED":
            self.stage1_pending = max(0, self.stage1_pending - count)
        elif kind == "DOWNLOAD_QUEUED":
            self.download_queued += count
        elif kind == "DOWNLOAD_STARTED":
            self.download_queued = max(0, self.download_queued - count)
            if bool(event.get("recovery")):
                self.recovery_queued = max(0, self.recovery_queued - count)
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
        elif kind in {"RECOVERY_QUEUED", "DOWNLOAD_RECOVERY_QUEUED"}:
            self.recovery_queued += count
        elif kind == "STAGE2_QUEUED":
            if filename:
                self._set_stage2_state(filename, "queued")
            else:
                self.stage2_queued += count
        elif kind == "STAGE2_STARTED":
            if filename:
                self._set_stage2_state(filename, "running")
            else:
                self.stage2_queued = max(0, self.stage2_queued - count)
                self.stage2_running += count
        elif kind == "STAGE2_APPROVED":
            if filename:
                self._complete_stage2(filename)
            else:
                self.stage2_running = max(0, self.stage2_running - count)
            self.stage2_approved += count
        elif kind == "STAGE2_REJECTED":
            if filename:
                self._complete_stage2(filename)
            else:
                self.stage2_running = max(0, self.stage2_running - count)
            self.stage2_rejected += count
        elif kind in {"STAGE2_PENDING", "STAGE2_DUPLICATE_PENDING"}:
            if filename:
                self._set_stage2_state(filename, "pending")
            else:
                self.stage2_running = max(0, self.stage2_running - count)
                self.stage2_pending += count
        elif kind == "STAGE2_INVALID":
            if filename:
                self._complete_stage2(filename)
            self.stage2_invalid += count
        elif kind == "DUPLICATE_DETECTED":
            self.duplicates += count

    @property
    def total_downloaded(self) -> int:
        return self.existing_downloads + self.downloaded_now

    def summary(self, target: int, *, download_workers: int = 5, validation_workers: int = 2) -> str:
        remaining = max(0, target - self.total_downloaded)
        return (
            f"Live pipeline · discovered {self.discovered} · Stage 1 pending {self.stage1_pending}, "
            f"✓ {self.stage1_approved} / ✕ {self.stage1_rejected} · download queued {self.download_queued}, "
            f"active {self.downloading}/{download_workers}, complete {self.total_downloaded}/{target} ({remaining} remaining) · "
            f"recovery {self.recovery_queued}, permanent failures {self.permanently_failed} · "
            f"Stage 2 queued {self.stage2_queued}, active {self.stage2_running}/{validation_workers}, ✓ {self.stage2_approved} / "
            f"✕ {self.stage2_rejected}, pending {self.stage2_pending}, invalid {self.stage2_invalid} · final accepted {self.stage2_approved} · "
            f"duplicates {self.duplicates}"
        )
