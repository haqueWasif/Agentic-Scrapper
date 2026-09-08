"""Cheap transfer telemetry; durable lifecycle transitions remain caller-owned."""
from dataclasses import dataclass
import threading
import time

PROGRESS_STATE_INTERVAL_SECONDS = 2.0
PROGRESS_STATE_BYTES = 8 * 1024 * 1024
PROGRESS_EVENT_INTERVAL_SECONDS = 0.5
DOWNLOAD_CHUNK_SIZE = 256 * 1024


@dataclass(frozen=True)
class ProgressPolicy:
    state_interval: float = PROGRESS_STATE_INTERVAL_SECONDS
    state_bytes: int = PROGRESS_STATE_BYTES
    event_interval: float = PROGRESS_EVENT_INTERVAL_SECONDS
    chunk_size: int = DOWNLOAD_CHUNK_SIZE

    def __post_init__(self):
        if min(self.state_interval, self.state_bytes, self.event_interval, self.chunk_size) <= 0:
            raise ValueError("Progress intervals, byte threshold and chunk size must be positive")


class TransferMetrics:
    """Process totals; snapshots can be differenced around an isolated benchmark."""
    def __init__(self):
        self._lock = threading.Lock()
        self._values = dict(bytes_downloaded=0, transfer_seconds=0.0,
                            ledger_writes=0, ledger_write_seconds=0.0,
                            progress_events=0, retry_wait_seconds=0.0,
                            pacing_wait_seconds=0.0, mirror_resolution_seconds=0.0)

    def add(self, **values):
        with self._lock:
            for name, value in values.items():
                self._values[name] += value

    def snapshot(self):
        with self._lock:
            return dict(self._values)


METRICS = TransferMetrics()


class DownloadProgress:
    """One owner per transfer attempt. No disk, locks or Drive on skipped ticks.

    The caller persists DOWNLOADING start before constructing this object and
    always persists terminal transitions with the actual file size afterward.
    A failed durable write does not advance the successful-write watermark.
    """
    def __init__(self, persist, emit, *, initial_bytes=0, policy=None, clock=time.monotonic):
        self.persist, self.emit = persist, emit
        self.policy, self.clock = policy or ProgressPolicy(), clock
        self.downloaded = self.last_bytes = initial_bytes
        self.total = 0
        self.last_state = clock()
        self.last_event = float('-inf')

    def __call__(self, downloaded, total):
        now = self.clock()
        self.downloaded, self.total = downloaded, total or 0
        # A server ignoring Range may restart from zero. Rebase the byte gate
        # while leaving the time gate active; lifecycle writes are never gated.
        if downloaded < self.last_bytes:
            self.last_bytes = downloaded
        if (now - self.last_state >= self.policy.state_interval or
                downloaded - self.last_bytes >= self.policy.state_bytes):
            self.checkpoint()
        if now - self.last_event >= self.policy.event_interval:
            self.emit(downloaded, self.total)
            METRICS.add(progress_events=1)
            self.last_event = now

    def checkpoint(self):
        """Explicit safe checkpoint, also usable by cancellation handlers."""
        if self.persist(self.downloaded) is not False:
            self.last_bytes, self.last_state = self.downloaded, self.clock()

    def mark_durable(self, downloaded):
        """Rebase after a caller's successful DOWNLOADING-start lifecycle write."""
        self.downloaded = self.last_bytes = downloaded
        self.last_state = self.clock()
