"""Bounded, deterministic producer/consumer scheduling for download workers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Empty, Full, Queue
import threading
from typing import Any, Callable


@dataclass(frozen=True)
class DownloadOutcome:
    candidate: dict[str, Any]
    result: dict[str, Any]


class GlobalDownloadQueue:
    """One bounded queue consumed by one fixed ThreadPoolExecutor.

    The caller owns UI updates and recovery policy.  Workers only execute the
    supplied deterministic download function and emit completed outcomes.
    """

    _STOP = object()

    def __init__(self, worker: Callable[[dict[str, Any], int], dict[str, Any]], *, max_workers: int,
                 maxsize: int = 50) -> None:
        self._worker = worker
        self.max_workers = max(1, max_workers)
        self._jobs: Queue[dict[str, Any] | object] = Queue(maxsize=max(1, maxsize))
        self.outcomes: Queue[DownloadOutcome] = Queue()
        self._active = 0
        self._active_lock = threading.Lock()
        self._closed = False
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="pdf-download")
        self._futures = [self._executor.submit(self._consume, worker_id) for worker_id in range(1, self.max_workers + 1)]

    def try_enqueue(self, candidate: dict[str, Any]) -> bool:
        if self._closed:
            raise RuntimeError("download queue is closed")
        try:
            self._jobs.put_nowait(dict(candidate))
            return True
        except Full:
            return False

    @property
    def pending(self) -> int:
        return self._jobs.qsize()

    @property
    def active(self) -> int:
        with self._active_lock:
            return self._active

    @property
    def idle(self) -> bool:
        return self._jobs.unfinished_tasks == 0 and self.active == 0

    def drain_outcomes(self) -> list[DownloadOutcome]:
        drained: list[DownloadOutcome] = []
        while True:
            try:
                drained.append(self.outcomes.get_nowait())
            except Empty:
                return drained

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _ in self._futures:
            self._jobs.put(self._STOP)
        for future in self._futures:
            future.result()
        self._executor.shutdown(wait=True)

    def _consume(self, worker_id: int) -> None:
        while True:
            job = self._jobs.get()
            try:
                if job is self._STOP:
                    return
                with self._active_lock:
                    self._active += 1
                try:
                    result = self._worker(dict(job), worker_id)
                except Exception as exc:  # Preserve a failed job for the main scheduler.
                    result = {
                        "success": False, "candidate": dict(job),
                        "attempt": int(job.get("_document_attempt", 1) or 1),
                        "permanent": False, "exception": exc,
                    }
                finally:
                    with self._active_lock:
                        self._active -= 1
                self.outcomes.put(DownloadOutcome(candidate=dict(job), result=result))
            finally:
                self._jobs.task_done()


class BoundedWorkQueue:
    """Reusable bounded work queue with one fixed worker pool.

    Unlike an executor's internal queue, admission is visible to the caller so
    completed downloads can remain durably ``PENDING`` when downstream Stage 2
    capacity is saturated rather than allocating unlimited in-memory work.
    """

    _STOP = object()

    def __init__(self, worker: Callable[[dict[str, Any], int], None], *, max_workers: int,
                 maxsize: int, thread_name_prefix: str) -> None:
        self._worker = worker
        self.max_workers = max(1, max_workers)
        self._jobs: Queue[dict[str, Any] | object] = Queue(maxsize=max(1, maxsize))
        self._known: set[str] = set()
        self._known_lock = threading.Lock()
        self._closed = False
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix=thread_name_prefix)
        self._futures = [self._executor.submit(self._consume, worker_id) for worker_id in range(1, self.max_workers + 1)]

    def try_enqueue(self, job: dict[str, Any], *, key: str) -> bool:
        if self._closed:
            raise RuntimeError("work queue is closed")
        with self._known_lock:
            if key in self._known:
                return True
            try:
                self._jobs.put_nowait(dict(job))
            except Full:
                return False
            self._known.add(key)
            return True

    def contains(self, key: str) -> bool:
        with self._known_lock:
            return key in self._known

    @property
    def pending(self) -> int:
        return self._jobs.qsize()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _ in self._futures:
            self._jobs.put(self._STOP)
        for future in self._futures:
            future.result()
        self._executor.shutdown(wait=True)

    def _consume(self, worker_id: int) -> None:
        while True:
            job = self._jobs.get()
            key = str(job.get("_queue_key", "")) if isinstance(job, dict) else ""
            try:
                if job is self._STOP:
                    return
                try:
                    self._worker(dict(job), worker_id)
                except Exception:
                    # The worker is responsible for durable failure state.  One
                    # malformed validation job must not retire a pool worker.
                    pass
            finally:
                if key:
                    with self._known_lock:
                        self._known.discard(key)
                self._jobs.task_done()


class GlobalRecoveryBacklog:
    """Deduplicate deferred download failures and enforce exact attempt arithmetic."""

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self.not_before = 0.0

    @staticmethod
    def _key(candidate: dict[str, Any]) -> str:
        return str(candidate.get("document_id") or candidate.get("filename") or "")

    def add_restored(self, candidate: dict[str, Any]) -> bool:
        """Add an already-recoverable job without consuming an attempt."""
        attempt = int(candidate.get("_document_attempt", 1) or 1)
        key = self._key(candidate)
        if not key or attempt < 1:
            return False
        existing = self._jobs.get(key)
        if existing is None or attempt < int(existing.get("_document_attempt", attempt)):
            self._jobs[key] = dict(candidate)
        return True

    def add_failed_attempt(self, result: dict[str, Any], *, max_attempts: int) -> tuple[dict[str, Any] | None, bool]:
        """Return the one valid next recovery job or flag terminal exhaustion.

        A completed failed attempt ``n`` schedules exactly ``n + 1``.  Attempt
        ``max_attempts`` is terminal.  Out-of-range inputs never produce a new
        job, preventing an accidental ``6/5`` submission.
        """
        candidate = dict(result.get("candidate") or {})
        attempt = int(result.get("attempt", candidate.get("_document_attempt", 0)) or 0)
        if not candidate or not 1 <= attempt <= max_attempts:
            return None, True
        if attempt >= max_attempts or result.get("permanent"):
            return None, True
        candidate["_document_attempt"] = attempt + 1
        key = self._key(candidate)
        if not key:
            return None, True
        existing = self._jobs.get(key)
        if existing is None or int(existing.get("_document_attempt", 0) or 0) < attempt + 1:
            self._jobs[key] = candidate
        self.not_before = max(self.not_before, float(result.get("retry_after_until", 0) or 0))
        return candidate, False

    def take_round(self, *, max_attempts: int) -> list[dict[str, Any]]:
        """Drain one recovery round; no job can be present twice in that round."""
        jobs = []
        for key, candidate in list(self._jobs.items()):
            attempt = int(candidate.get("_document_attempt", 0) or 0)
            if 1 <= attempt <= max_attempts:
                jobs.append(dict(candidate))
            del self._jobs[key]
        return jobs

    def snapshot(self) -> list[dict[str, Any]]:
        """Return a display-only copy; callers cannot mutate scheduler jobs."""
        return [dict(candidate) for candidate in self._jobs.values()]

    def __bool__(self) -> bool:
        return bool(self._jobs)

    def __len__(self) -> int:
        return len(self._jobs)
