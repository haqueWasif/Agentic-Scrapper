"""Admission and cooldowns live on one dispatcher, never on network workers."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import math
import random
import threading
import time
from urllib.parse import urlparse

from app.pipeline_scheduler import DownloadOutcome
from app.pipeline_state import stable_document_id


class FairDownloadQueue:
    cycle = ('primary', 'primary', 'handoff', 'primary', 'recovery')

    def __init__(self, worker, *, network, max_workers, maxsize=50, on_defer=None):
        self.worker, self.network = worker, network
        self.max_workers, self.maxsize = max_workers, maxsize
        self.on_defer = on_defer
        self.outcomes = Queue()
        self.queues = {kind: deque() for kind in ('primary', 'handoff', 'recovery')}
        self.known, self.completed_ids = set(), set()
        self.running = {}
        self.condition = threading.Condition(threading.RLock())
        self.closed = False
        self.cursor = 0
        self.next_admission = 0.0
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='pdf-download')
        self.dispatcher = threading.Thread(target=self._dispatch, name='download-admission', daemon=True)
        self.dispatcher.start()

    def try_enqueue(self, candidate):
        key = candidate.get('document_id') or stable_document_id(candidate)
        with self.condition:
            if self.closed:
                raise RuntimeError('Download queue is closed')
            if key in self.known or key in self.completed_ids:
                return True
            kind = 'recovery' if candidate.get('_recovery_round') else 'primary'
            if len(self.queues[kind]) >= (self.maxsize if kind == 'primary' else self.maxsize * 20):
                return False
            job = dict(candidate, document_id=key, _queue_kind=kind)
            job.setdefault('_first_admitted_monotonic', time.monotonic())
            self.queues[kind].append(job)
            self.known.add(key)
            self.condition.notify_all()
            return True

    @property
    def active(self):
        with self.condition:
            return len(self.running)

    @property
    def pending(self):
        with self.condition:
            return sum(map(len, self.queues.values()))

    @property
    def idle(self):
        with self.condition:
            return not self.running and not any(self.queues.values())

    def queue_stats(self):
        with self.condition:
            now = time.time()
            return {**{kind: len(q) for kind, q in self.queues.items()},
                    'not_before_waiting': sum(job.get('_not_before', 0) > now for q in self.queues.values() for job in q),
                    'admission_waiting': sum(len(q) for q in self.queues.values()) if time.monotonic() < self.next_admission else 0}

    def drain_outcomes(self):
        result = []
        while True:
            try:
                result.append(self.outcomes.get_nowait())
            except Empty:
                return result

    def _ready(self, kind):
        now = time.time()
        return any(self._eligible_at(job) <= now for job in self.queues[kind])

    def _eligible_at(self, job):
        until = job.get('_not_before', 0)
        routes = [job.get('last_failed_route', ''), *(job.get('mirrors') or [job.get('source_url','')])]
        with self.network._lock:
            until = max([until, *(self.network.route_waits.get(urlparse(route).netloc.lower(), 0) for route in routes)])
        return until

    def _select(self):
        recovery_active = sum(job['_queue_kind'] != 'primary' for job in self.running.values())
        limit = max(1, math.ceil(self.max_workers * .4))
        for step in range(len(self.cycle)):
            index = (self.cursor + step) % len(self.cycle)
            kind = self.cycle[index]
            if kind != 'primary' and self._ready('primary') and recovery_active >= limit:
                continue
            queue = self.queues[kind]
            for job in list(queue):
                if self._eligible_at(job) <= time.time():
                    queue.remove(job)
                    self.cursor = (index + 1) % len(self.cycle)
                    return job
        return None

    def _dispatch(self):
        with self.condition:
            while not self.closed:
                if len(self.running) < self.max_workers and time.monotonic() >= self.next_admission:
                    job = self._select()
                    if job is not None:
                        worker = next(i for i in range(1, self.max_workers + 1) if i not in self.running)
                        self.running[worker] = job
                        self.next_admission = time.monotonic() + random.uniform(
                            self.network.settings.new_download_min_seconds, self.network.settings.new_download_max_seconds)
                        self.executor.submit(self._execute, job, worker)
                        continue
                # Wake at the actual admission deadline. A fixed 50 ms poll
                # needlessly left eligible jobs idle under short pacing.
                remaining = self.next_admission - time.monotonic()
                self.condition.wait(min(.05, remaining) if remaining > 0 else .05)

    def _execute(self, job, worker):
        started = time.monotonic()
        net = self.network
        net.context.not_before, net.context.rate_limited = 0, False
        net.context.deferred = False
        try:
            result = self.worker(dict(job), worker)
        except Exception as exc:
            result = dict(success=False, candidate=job, attempt=job.get('_document_attempt', 1),
                          permanent=False, reason=type(exc).__name__)
        result.setdefault('candidate', job)
        result.setdefault('attempt', job.get('_document_attempt', 1))
        if getattr(net.context,'deferred',False):
            result['permanent']=False
        with self.condition:
            del self.running[worker]
            if net.telemetry:
                net.telemetry.state(worker, 'IDLE')
                if result.get('success') and not result.get('local_reused'):
                    net.telemetry.completed(time.monotonic() - job.get('_first_admitted_monotonic', started))
            if result.get('success'):
                self.completed_ids.add(job['document_id'])
                self.known.discard(job['document_id'])
                self.outcomes.put(DownloadOutcome(job, result))
            elif not self.closed and not result.get('permanent'):
                attempt = result['attempt'] + (0 if getattr(net.context, 'rate_limited', False) or getattr(net.context,'deferred',False) or result.get('unclaimed') else 1)
                if attempt <= net.settings.max_document_attempts:
                    failures = int(job.get('consecutive_failures', 0)) + 1
                    partial = int(result.get('downloaded_bytes', 0)) > 0
                    handoffs = int(job.get('fast_handoff_count', 0)) + int(partial)
                    kind = 'handoff' if partial and handoffs <= 2 else 'recovery'
                    delay = 1 if kind == 'handoff' else min(300, 5 * 2 ** min(failures - 1, 6))
                    deferred = dict(job, _document_attempt=attempt, _queue_kind=kind, _recovery_round=True,
                                    consecutive_failures=failures, fast_handoff_count=handoffs,
                                    _not_before=max(time.time() + delay, getattr(net.context, 'not_before', 0),
                                                    result.get('retry_after_until', 0)),
                                    last_failure_reason=result.get('reason', 'transfer did not complete'))
                    deferred['last_failed_route']=getattr(net.context,'last_route','')
                    if self.on_defer:
                        try:
                            self.on_defer(deferred)
                        except Exception:
                            deferred['_not_before']=max(deferred['_not_before'],time.time()+10)
                    self.queues[kind].append(deferred)
                else:
                    result['permanent'] = True
                    self.known.discard(job['document_id'])
                    self.outcomes.put(DownloadOutcome(job, result))
            else:
                self.known.discard(job['document_id'])
                self.outcomes.put(DownloadOutcome(job, result))
            self.condition.notify_all()

    def close(self, *, cancel_pending=False, wait=True):
        with self.condition:
            self.closed = True
            for queue in self.queues.values():
                queue.clear()
            self.condition.notify_all()
        if wait:
            self.dispatcher.join()
        self.executor.shutdown(wait=wait, cancel_futures=cancel_pending)
