"""In-memory goodput windows and worker occupancy; no per-chunk persistence."""
from collections import defaultdict
from contextlib import contextmanager
import threading
import time

STATES = ('RECEIVING_BYTES', 'RESOLVING', 'CONNECTING', 'STALL', 'RETRY_WAIT',
          'ADMISSION_WAIT', 'LEDGER_IO', 'DRIVE_SYNC', 'IDLE')


class Throughput:
    def __init__(self, workers, *, clock=time.monotonic):
        self.clock = clock
        self.started = clock()
        self.lock = threading.RLock()
        self.workers = {i: ['IDLE', self.started, self.started] for i in range(1, workers + 1)}
        self.totals = {i: defaultdict(float) for i in self.workers}
        self.buckets = defaultdict(lambda: defaultdict(float))
        self.history = defaultdict(lambda: defaultdict(float))
        self.highwater = {}
        self.uncertain_documents = set()
        self.received_bytes = self.goodput_bytes = self.completions = 0
        self.connection_seconds = self.connections = self.document_seconds = 0
        self.background_drive_seconds = 0.0
        self.queue_history = {}

    def _add(self, key, value, now):
        second = int(now - self.started)
        self.buckets[second][key] += value
        self.history[second // 300][key] += value

    def _account(self, worker, now):
        state, since, last_byte = self.workers[worker]
        self.totals[worker][state] += now - since
        cursor = since
        while cursor < now:
            end = min(now, self.started + int(cursor - self.started) + 1)
            self._add(state, end - cursor, cursor)
            cursor = end
        self.workers[worker][1] = now

    def state(self, worker, state):
        if worker not in self.workers:
            return
        with self.lock:
            now = self.clock()
            self._account(worker, now)
            self.workers[worker][0] = state

    def baseline(self, document, size):
        with self.lock:
            self.highwater[document] = max(size, self.highwater.get(document, 0))

    def exclude_uncertain_replay(self, document):
        # A dead runtime can have received bytes beyond its last coarse DB
        # checkpoint. Count this restart as wire traffic, not guessed goodput.
        with self.lock:
            self.uncertain_documents.add(document)

    def received(self, worker, document, offset, count, *, useful=True):
        with self.lock:
            self.state(worker, 'RECEIVING_BYTES')
            now = self.clock()
            self.workers[worker][2] = now
            fresh = max(0, offset - self.highwater.get(document, 0)) if useful and document not in self.uncertain_documents else 0
            if useful:
                self.highwater[document] = max(offset, self.highwater.get(document, 0))
            self.received_bytes += count
            self.goodput_bytes += fresh
            self._add('received_bytes', count, now)
            self._add('goodput_bytes', fresh, now)

    def completed(self, duration):
        with self.lock:
            self.completions += 1
            self.document_seconds += duration
            self._add('completions', 1, self.clock())

    @contextmanager
    def timed(self, worker, state):
        previous = self.workers.get(worker, ['IDLE'])[0]
        self.state(worker, state)
        start = self.clock()
        try:
            yield
        finally:
            self.state(worker, previous)
            if state == 'DRIVE_SYNC':
                with self.lock:
                    self.background_drive_seconds += self.clock() - start

    def connection(self, seconds):
        with self.lock:
            self.connection_seconds += seconds
            self.connections += 1

    def snapshot(self, queues=None, stall_seconds=60):
        with self.lock:
            now = self.clock()
            for worker, row in self.workers.items():
                if row[0] == 'RECEIVING_BYTES' and now - row[2] >= stall_seconds:
                    self.state(worker, 'STALL')
                self._account(worker, now)
            elapsed = max(now - self.started, .001)
            windows = {}
            for seconds in (30, 60, 300):
                values = defaultdict(float)
                for bucket, data in self.buckets.items():
                    if bucket >= int(elapsed) - seconds + 1:
                        for key, value in data.items():
                            values[key] += value
                duration = min(seconds, elapsed)
                windows[str(seconds)] = dict(
                    goodput_mib_s=values['goodput_bytes'] / 1024**2 / duration,
                    received_mib_s=values['received_bytes'] / 1024**2 / duration,
                    completions=int(values['completions']),
                    average_workers={s: values[s] / duration for s in STATES})
            for bucket in list(self.buckets):
                if bucket < elapsed - 301:
                    del self.buckets[bucket]
            counts = {s: sum(row[0] == s for row in self.workers.values()) for s in STATES}
            self.queue_history[int(elapsed) // 300] = dict(queues or {})
            trend = []
            for period, data in sorted(self.history.items()):
                duration = max(.001, min(300, elapsed-period*300))
                waiting = self.queue_history.get(period, {})
                trend.append(dict(minute_start=period*5, minute_end=min(elapsed/60,(period+1)*5),
                    goodput_mib_s=data.get('goodput_bytes',0)/1024**2/duration,
                    receiving_workers_average=data.get('RECEIVING_BYTES',0)/duration,
                    completions=int(data.get('completions',0)), waiting=waiting))
            return dict(elapsed_seconds=elapsed, windows=windows, worker_counts=counts,
                        goodput_bytes=self.goodput_bytes, received_bytes=self.received_bytes,
                        completions=self.completions, run_goodput_mib_s=self.goodput_bytes / 1024**2 / elapsed,
                        average_connection_seconds=self.connection_seconds / max(1, self.connections),
                        average_document_seconds=self.document_seconds / max(1, self.completions),
                        worker_seconds={w: {s: totals[s] for s in STATES} for w, totals in self.totals.items()},
                        background_drive_seconds=self.background_drive_seconds, queues=queues or {},
                        uncertain_replay_documents=len(self.uncertain_documents),
                        five_minute_history={str(k): dict(v) for k, v in self.history.items()},
                        five_minute_report=trend)
