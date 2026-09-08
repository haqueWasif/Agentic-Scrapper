"""Admission and cooldowns live on one dispatcher, never on network workers."""
from collections import deque
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import math
import random
import threading
import time

from app.pipeline_scheduler import DownloadOutcome
from app.pipeline_state import stable_document_id
from app.network_manager import RequestDeferred


class FairDownloadQueue:
    cycle = ('primary', 'primary', 'handoff', 'primary', 'recovery')

    def __init__(self, worker, *, network, max_workers, maxsize=50, on_defer=None):
        self.worker, self.network = worker, network
        self.max_workers, self.maxsize = max_workers, maxsize
        self.on_defer = on_defer
        self.outcomes = Queue()
        self.queues = {kind: deque() for kind in ('primary', 'handoff', 'recovery')}
        self.known, self.completed_ids = set(), set()
        self.terminal_ids = set()
        self.running = {}
        self.condition = threading.Condition(threading.RLock())
        self.closed = False
        self.cursor = 0
        self.next_admission = 0.0
        self.counters = Counter()
        self._starved_since = None
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='pdf-download')
        self.dispatcher = threading.Thread(target=self._dispatch, name='download-admission', daemon=True)
        self.dispatcher.start()

    def try_enqueue(self, candidate):
        key = candidate.get('document_id') or stable_document_id(candidate)
        with self.condition:
            if self.closed:
                raise RuntimeError('Download queue is closed')
            if key in self.known or key in self.completed_ids or key in self.terminal_ids:
                return True
            kind = candidate.get('_queue_kind') or ('recovery' if candidate.get('_recovery_round') else 'primary')
            if kind not in self.queues:
                kind = 'recovery'
            if len(self.queues[kind]) >= (self.maxsize if kind == 'primary' else self.maxsize * 20):
                return False
            job = dict(candidate, document_id=key, _queue_kind=kind)
            job['_first_admitted_monotonic'] = time.monotonic()
            self.queues[kind].append(job)
            self.known.add(key)
            self.counters['enqueued'] += 1
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
            counts = Counter({kind: len(q) for kind, q in self.queues.items()})
            for kind, queue in self.queues.items():
                for job in queue:
                    deadline, state = self._deadline(job)
                    if deadline > now:
                        counts['not_before_waiting'] += 1
                        counts[state] += 1
                    else:
                        counts[kind + '_ready'] += 1
            if time.monotonic() < self.next_admission:
                counts['deferred_pacing'] = sum(counts[kind+'_ready'] for kind in self.queues)
            counts['admission_waiting'] = counts['deferred_pacing']
            return {**{key: counts[key] for key in ('primary','handoff','recovery','primary_ready',
                    'handoff_ready','recovery_ready','deferred_route','deferred_pacing','rate_limit_waiting',
                    'retry_waiting','not_before_waiting','admission_waiting')}, 'totals': dict(self.counters)}

    def drain_outcomes(self):
        result = []
        while True:
            try:
                result.append(self.outcomes.get_nowait())
            except Empty:
                return result

    def recovery_snapshot(self):
        with self.condition:
            return [dict(job) for kind, queue in self.queues.items() for job in queue
                    if kind != 'primary' or job.get('_queue_state','').startswith('DEFERRED')]

    def _ready(self, kind):
        now = time.time()
        return any(self._eligible_at(job) <= now for job in self.queues[kind])

    def _eligible_at(self, job):
        return self._deadline(job)[0]

    def _deadline(self, job):
        until = job.get('_not_before', 0)
        state = {'DEFERRED_ROUTE':'deferred_route','DEFERRED_RATE_LIMIT':'rate_limit_waiting'}.get(job.get('_queue_state'), 'retry_waiting')
        mirrors = job.get('mirrors') or [job.get('source_url','')]
        # Check the route this attempt will use, not every alternative mirror.
        index = (int(job.get('_document_attempt',1))-1) % len(mirrors)
        routes = [mirrors[index]]
        if job.get('_queue_state') == 'DEFERRED_ROUTE' or job.get('_queue_kind') == 'handoff':
            routes.append(job.get('last_failed_route',''))
        for route in routes:
            deadline, kind = self.network.request_deadline(route)
            if deadline > until:
                until = deadline
                state = 'rate_limit_waiting' if kind == 'DEFERRED_RATE_LIMIT' else 'deferred_route'
        return until, state

    def _select(self):
        active_ids = {job['document_id'] for job in self.running.values()}
        recovery_active = sum(job['_queue_kind'] != 'primary' for job in self.running.values())
        limit = max(1, math.ceil(self.max_workers * .4))
        for step in range(len(self.cycle)):
            index = (self.cursor + step) % len(self.cycle)
            kind = self.cycle[index]
            if kind != 'primary' and self._ready('primary') and recovery_active >= limit:
                continue
            queue = self.queues[kind]
            for job in list(queue):
                if job['document_id'] not in active_ids and self._eligible_at(job) <= time.time():
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
                        self.counters['admitted'] += 1
                        self._starved_since = None
                        self.next_admission = time.monotonic() + random.uniform(
                            self.network.settings.new_download_min_seconds, self.network.settings.new_download_max_seconds)
                        self.executor.submit(self._execute, job, worker)
                        continue
                self._check_starvation()
                # Wake at the actual admission deadline. A fixed 50 ms poll
                # needlessly left eligible jobs idle under short pacing.
                remaining = self.next_admission - time.monotonic()
                self.condition.wait(min(.05, remaining) if remaining > 0 else .05)

    def _check_starvation(self):
        eligible = (len(self.running) < self.max_workers and time.monotonic() >= self.next_admission
                    and any(self._ready(kind) for kind in self.queues))
        if not eligible:
            self._starved_since = None
        elif self._starved_since is None:
            self._starved_since = time.monotonic()
        elif time.monotonic() - self._starved_since >= .25:
            self.counters['scheduler_starvation'] += 1
            self.network.emit('scheduler_starvation', 0, '', 'SCHEDULER_STARVATION',
                              queues=self.queue_stats(), free_workers=self.max_workers-len(self.running))
            self._starved_since = time.monotonic()

    def _next_state(self, job, result):
        """Exactly one next state; admission deferral consumes no failure budget."""
        net = self.network
        if result.get('success') or result.get('cancelled'):
            return None
        attempt = int(result.get('attempt', job.get('_document_attempt',1)))
        now = time.time()
        until = max(result.get('not_before',0), result.get('retry_after_until',0), getattr(net.context,'not_before',0))
        deferred = result.get('deferred') or result.get('unclaimed') or getattr(net.context,'deferred',False) or getattr(net.context,'rate_limited',False)
        if deferred:
            state = result.get('retry_class') or ('DEFERRED_RATE_LIMIT' if getattr(net.context,'rate_limited',False) else 'DEFERRED_ROUTE')
            self.counters['route_defers' if state == 'DEFERRED_ROUTE' else 'rate_limit_defers'] += 1
            return dict(job, _queue_state=state, _not_before=max(until,now+.05),
                        last_failed_route=result.get('route') or getattr(net.context,'last_route',''),
                        last_failure_reason=result.get('reason','Admission deferred'),_document_attempt=attempt)
        failures = int(job.get('consecutive_failures',0)) + 1
        handoffs = int(job.get('fast_handoff_count',0))
        partial = int(result.get('part_size',result.get('downloaded_bytes',0))) > 0
        common = dict(job, consecutive_failures=failures,
                      last_failure_reason=result.get('reason','Transfer did not complete'),
                      last_failed_route=result.get('route') or getattr(net.context,'last_route',''))
        if not result.get('permanent') and partial and handoffs < net.settings.max_fast_handoffs_per_attempt:
            self.counters['handoffs'] += 1
            return dict(common,_queue_kind='handoff',_queue_state='HANDOFF_READY',_recovery_round=True,
                        _document_attempt=attempt,fast_handoff_count=handoffs+1,
                        _not_before=max(until,now+net.settings.fast_handoff_delay_seconds))
        if result.get('permanent') or attempt >= net.settings.max_document_attempts:
            result['permanent'] = True
            return dict(common,_queue_state='PERMANENTLY_FAILED',_document_attempt=attempt,_not_before=0)
        self.counters['recoveries'] += 1
        delay = min(300, net.settings.recovery_round_delay_seconds * 2 ** min(failures-1,6))
        return dict(common,_queue_kind='recovery',_queue_state='RECOVERY_READY',_recovery_round=True,
                    _document_attempt=attempt+1,fast_handoff_count=0,_not_before=max(until,now+delay))

    def _execute(self, job, worker):
        started = time.monotonic()
        net = self.network
        net.context.not_before, net.context.rate_limited = 0, False
        net.context.deferred = False
        net.context.last_route = ''
        try:
            try:
                result = self.worker(dict(job), worker)
            except RequestDeferred as exc:
                result = exc.outcome(job)
            except BaseException as exc:
                result = dict(success=False,candidate=job,attempt=job.get('_document_attempt',1),
                              cancelled=not isinstance(exc,Exception) or net.stop_event.is_set(),reason=type(exc).__name__)
            result.setdefault('candidate',job)
            result.setdefault('attempt',job.get('_document_attempt',1))
            with self.condition:
                next_job = self._next_state(job,result)
            # Persistence must not hold the dispatcher lock. The document stays
            # owned until its next state is durably recorded and enqueued once.
            if next_job is not None and self.on_defer:
                try:
                    from contextlib import nullcontext
                    with net.telemetry.timed(worker,'LEDGER_IO') if net.telemetry else nullcontext():
                        self.on_defer(next_job)
                except Exception:
                    self.counters['persistence_errors'] += 1
                    net.emit('status',worker,job.get('filename',''),
                             'Scheduler state persistence failed; in-memory ownership retained',level='WARNING')
            with self.condition:
                if result.get('success'):
                    self.completed_ids.add(job['document_id'])
                    if net.telemetry and not result.get('local_reused'):
                        net.telemetry.completed(time.monotonic()-job.get('_first_admitted_monotonic',started))
                if result.get('permanent'):
                    self.terminal_ids.add(job['document_id'])
                if next_job is not None and not result.get('permanent') and not self.closed:
                    self.queues[next_job['_queue_kind']].append(next_job)
                else:
                    self.known.discard(job['document_id'])
                    self.outcomes.put(DownloadOutcome(job,result))
        finally:
            if net.telemetry:
                net.telemetry.state(worker,'IDLE')
            net.emit('worker_state',worker,job.get('filename',''),'Worker released',state='IDLE',
                     bytes_downloaded=0,total_bytes=0)
            with self.condition:
                self.running.pop(worker,None)
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
