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
