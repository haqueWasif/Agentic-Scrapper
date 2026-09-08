"""Notebook adapter for the single, import-safe pipeline implemented in app.py.

The app package and app.py share a name, so load the entry module by its explicit
path once. This executes normal Python imports, never copies/extracts functions,
and does not execute the guarded Streamlit main().
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace, asdict
from types import SimpleNamespace
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time

from app.download_progress import ProgressPolicy, METRICS

_RUN_LOCK = threading.Lock()


class _SecretFilter(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        for name in ('OPENROUTER_API_KEY', 'ZENROWS_API_KEY', 'LANGSMITH_API_KEY', 'DATABASE_URL'):
            value = os.environ.get(name, '')
            if value:
                message = message.replace(value, '[REDACTED]')
        record.msg, record.args = message, ()
        return True


def shared_pipeline():
    name = 'app._shared_pipeline'
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / 'app.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(name, None)
            raise
    return sys.modules[name]


@dataclass(frozen=True)
class ColabConfig:
    local_work_dir: str = '/content/ashrae_work'
    use_google_drive: bool = False
    drive_output_dir: str = '/content/drive/MyDrive/ASHRAE_Scraper'
    existing_library_dirs: tuple[str, ...] = ()
    download_workers: int = 5
    stage2_workers: int = 2
    new_download_min_seconds: float = 2.0
    new_download_max_seconds: float = 6.0
    progress_policy: ProgressPolicy = ProgressPolicy()
    checkpoint_partials_to_drive: bool = False
    partial_checkpoint_seconds: float = 600.0
    debug_verbose: bool = False
    distributed_mode: bool = False
    distributed_run_id: str = 'ashrae-hvac-001'
    shard_count: int = 3
    shard_id: int = 0
    lease_seconds: int = 120
    heartbeat_seconds: float = 25
    db_progress_interval_seconds: float = 10
    database_pool_size: int = 4
    shared_storage_dir: str = ''
    shared_storage_id: str = 'ashrae-shared-pdfs-v1'
    takeover_abandoned_pages: bool = False

    def __post_init__(self):
        for name in ('download_workers', 'stage2_workers'):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f'{name} must be a positive integer')
        if self.shard_count < 1 or not 0 <= self.shard_id < self.shard_count:
            raise ValueError('SHARD_ID must be between 0 and SHARD_COUNT - 1')
        if not 0 < self.db_progress_interval_seconds <= self.heartbeat_seconds < self.lease_seconds / 2:
            raise ValueError('Progress interval <= heartbeat interval < half the lease duration is required')
        if not 1 <= self.database_pool_size <= 8:
            raise ValueError('Use a small database pool (1 to 8 connections)')
        if self.distributed_mode and (not self.shared_storage_dir or not self.distributed_run_id):
            raise ValueError('Distributed mode needs RUN_ID and SHARED_STORAGE_DIR')
        if not 0 <= self.new_download_min_seconds <= self.new_download_max_seconds:
            raise ValueError('Invalid start pacing interval')
        if self.partial_checkpoint_seconds < 300:
            raise ValueError('Partial checkpoints must be at least five minutes apart')
        local, drive = Path(self.local_work_dir).resolve(), Path(self.drive_output_dir).resolve()
        if self.use_google_drive and (local == drive or drive in local.parents or local in drive.parents):
            raise ValueError('Local active storage and persistent storage must be separate directories')
        if '/content/drive' in local.as_posix():
            raise ValueError('Active transfers must use local disk, outside the mounted Drive')


class NotebookProgress:
    """One updating output area, capped at one refresh per second."""
    def __init__(self):
        self.handle = None
        self.last = float('-inf')

    def __call__(self, snapshot):
        if time.monotonic() - self.last < 1:
            return
        self.last = time.monotonic()
        lines = [f'Search: Page {snapshot.discovery_page} / {snapshot.max_pages}',
                 f'Stage 1: {snapshot.stage1_approved} approved | {snapshot.stage1_rejected} rejected',
                 f'Transfers available (integrity separate): {snapshot.downloaded} | recovery waiting: {snapshot.recovery_backlog}']
        distributed = getattr(snapshot, 'distributed', {})
        if distributed:
            lines[:0] = [f"Run: {distributed['run_id']} | shard {distributed['shard_id'] + 1}/{distributed['shard_count']} | {distributed['instance_id']}"]
            global_stats = distributed.get('global', {})
            lines.append(f"Global: {global_stats.get('pages_completed',0)} pages | {global_stats.get('completed',0)} PDFs | {global_stats.get('accepted',0)}/{distributed['target']} accepted | {global_stats.get('active_claims',0)} active claims")
            lines.append(f"Global goodput: {global_stats.get('global_goodput_mib_s',0):.2f} MiB/s | discovered {global_stats.get('discovered',0)} | handoff {global_stats.get('fast_handoff_ready',0)} | recovery {global_stats.get('recovery_ready',0)}")
        throughput = getattr(snapshot, 'throughput', {})
        if throughput:
            windows=throughput.get('windows',{})
            lines.append('Goodput: ' + ' | '.join(f"{seconds}s: {windows.get(str(seconds),{}).get('goodput_mib_s',0):.2f} MiB/s" for seconds in (30,60,300)))
            lines.append(f"New transfers this runtime: {throughput.get('completions',0)} | run average {throughput.get('run_goodput_mib_s',0):.2f} MiB/s")
            lines.append(f"Useful worker ratio: {throughput.get('useful_worker_ratio',0):.1%} | Drive sync active: {throughput.get('drive_sync_active',0)}")
            lines.append(f"Worker states: {throughput.get('worker_counts',{})} | Waiting: {throughput.get('queues',{})}")
            health = throughput.get('route_health',{})
            lines.append(f"Routes deferred: {health.get('routes_deferred',0)} | host rate-limit waits: {len(health.get('host_rate_limit_waits',{}))} | Errors: {throughput.get('error_counts',{})}")
        visible_rows = [row for row in snapshot.worker_rows if row.get('state') not in ('IDLE', 'COMPLETED')]
        for row in visible_rows[:10]:
            state = row.get('state', 'IDLE')
            size = (row.get('bytes_downloaded', 0) or 0) / 1024**2
            total = (row.get('total_bytes', 0) or 0) / 1024**2
            if state == 'IDLE':
                progress = ''
            elif total > 0 and total >= size:
                progress = f'  {size:.1f} / {total:.1f} MiB'
            else:
                progress = f'  {size:.1f} MiB / unknown'
            lines.append(f"  Worker {row['worker']}: {state}{progress}")
        if len(visible_rows) > 10:
            lines.append(f'  {len(visible_rows) - 10} more active workers; totals shown above')
        lines += [f'Stage 2: {snapshot.stage2_active} active | {snapshot.stage2_queued} queued | {snapshot.stage2_pending} pending',
                  f'Library this run: {snapshot.stage2_approved} accepted | {snapshot.stage2_rejected} rejected']
        from IPython.display import display
        output = '\n'.join(lines)
        if self.handle is None:
            self.handle = display({'text/plain': output}, raw=True, display_id=True)
        else:
            self.handle.update({'text/plain': output}, raw=True)


async def run_colab_pipeline(query, max_pages=50, target_documents=500, *, config=None, on_snapshot=None):
    """Await directly in Colab; no asyncio.run(), Streamlit context or server."""
    if not _RUN_LOCK.acquire(blocking=False):
        raise RuntimeError('A pipeline is already running; interrupt and let its checkpoint finish first')
    core = None
    sync = None
    old = {}
    handler = None
    old_handlers = None
    distributed = None
    try:
        config = config or ColabConfig()
        core = shared_pipeline()
        core._THROUGHPUT_SNAPSHOT = {}
        local = Path(config.local_work_dir).resolve()
        local.mkdir(parents=True, exist_ok=True)
        (local / 'logs').mkdir(exist_ok=True)
        handler = logging.FileHandler(local / 'logs' / 'scraper.log', encoding='utf-8')
        handler.addFilter(_SecretFilter())
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
        old_level = logging.getLogger().level
        old_handlers = list(logging.getLogger().handlers)
        logging.getLogger().handlers = [handler]
        if config.debug_verbose:
            console = logging.StreamHandler()
            console.addFilter(_SecretFilter())
            logging.getLogger().addHandler(console)
        logging.getLogger().setLevel(logging.DEBUG)
        mapping = dict(DATA_DIRECTORY=local, DOWNLOAD_DIRECTORY=local / 'ASHRAE_Files',
                       LOW_RELEVANCE_DIRECTORY=local / 'Low_Relevance_Files',
                       SEARCH_CACHE_DIRECTORY=local / 'search_cache', DOWNLOAD_STATE_FILE=local / 'downloads.json')
        for name, value in mapping.items():
            old[name] = getattr(core, name)
            setattr(core, name, value)
        if config.distributed_mode:
            if not os.environ.get('DATABASE_URL'):
                raise ValueError('Set DATABASE_URL using a Colab secret or environment variable')
            from colab.distributed_coordinator import DistributedCoordinator
            distributed = await asyncio.to_thread(DistributedCoordinator, config, core, query, max_pages, target_documents)
            for name, callback in (('invoke_scraper_graph',distributed.stage1),('invoke_pdf_validation_graph',distributed.stage2)):
                old[name]=getattr(core,name)
                setattr(core,name,callback)
            core._DISTRIBUTED=distributed
        from colab.storage import DriveSnapshots
        snapshot_config = replace(config, drive_output_dir=str(Path(config.drive_output_dir)/'runtimes'/distributed.instance)) if distributed else config
        sync = DriveSnapshots(snapshot_config, core)
        await asyncio.to_thread(sync.restore)
        from app.local_library import LocalLibrary
        core._LOCAL_LIBRARY = LocalLibrary(local, config.existing_library_dirs)
        await asyncio.to_thread(core._LOCAL_LIBRARY.refresh)
        settings = replace(core.NetworkManager._read_settings(core.PROJECT_ROOT / 'config' / 'settings.yaml'),
                           max_workers=config.download_workers, validation_workers=config.stage2_workers,
                           defer_download_retries=True,
                           scheduler_admission=True,
                           new_download_min_seconds=config.new_download_min_seconds,
                           new_download_max_seconds=config.new_download_max_seconds)
        core._NETWORK_SETTINGS = settings
        core._PROGRESS_POLICY = config.progress_policy
        sync.start()
        metrics_before = METRICS.snapshot()
        run_started = time.monotonic()
        sink=on_snapshot or NotebookProgress()
        def show(snapshot):
            extra = dict(throughput=getattr(core,'_THROUGHPUT_SNAPSHOT',{}))
            if distributed:
                extra['distributed']=dict(run_id=distributed.run_id,instance_id=distributed.instance,
                    shard_id=config.shard_id,shard_count=config.shard_count,target=target_documents,global_stats=distributed.global_stats)
                extra['distributed']['global']=distributed.global_stats
            sink(SimpleNamespace(**asdict(snapshot),**extra))
        result = await core.run_scraping_pipeline(query, max_pages, target_documents=target_documents,
                                                  headless=True, on_snapshot=show,
                                                  network_settings=settings)
        reports = []
        for report_path in (local / 'validation_reports').glob('*.json'):
            try:
                report = json.loads(report_path.read_text(encoding='utf-8'))
                if report.get('query') == query.strip():
                    reports.append(report)
            except (OSError, ValueError):
                continue
        result.update(
            ashrae_approved=sum(report.get('ashrae_identity') is True for report in reports),
            query_relevant=sum(report.get('query_relevant') is True for report in reports),
            stage2_approved=sum(report.get('status') == 'APPROVED' for report in reports),
            rejected=sum(report.get('status') == 'REJECTED' for report in reports),
        )
        result['benchmark_totals'] = {key: value - metrics_before[key] for key, value in METRICS.snapshot().items()}
        totals = result['benchmark_totals']
        totals['wall_seconds'] = time.monotonic() - run_started
        totals['binary_mib_per_second'] = (totals['bytes_downloaded'] / 1024**2 / totals['transfer_seconds']
                                          if totals['transfer_seconds'] else 0.0)
        result['output_directory'] = str(local)
        result['throughput']=getattr(core,'_THROUGHPUT_SNAPSHOT',{})
        if distributed:
            result['global']=await asyncio.to_thread(distributed.db.counters,distributed.run_id,distributed.qh)
            result['instance_id']=distributed.instance
        result['persistent_directory'] = config.drive_output_dir if config.use_google_drive else None
        from app.local_library import atomic_json
        atomic_json(local / 'run_summary.json', result)
        return result
    finally:
        # The shared core waits for its download pool in its own finally block.
        # Wait for Stage 2 before copying files or restoring process globals.
        try:
            if core is not None:
                await asyncio.to_thread(core._stop_validation_queue, cancel_pending=True)
            if sync is not None:
                await asyncio.to_thread(sync.stop_and_sync)
        finally:
            if core is not None:
                for name, value in old.items():
                    setattr(core, name, value)
                core._LOCAL_LIBRARY = None
                core._NETWORK_SETTINGS = None
                core._PROGRESS_POLICY = None
                core._DISTRIBUTED = None
                core._LIVE_NETWORK = None
            try:
                if distributed is not None:
                    await asyncio.to_thread(distributed.close)
            finally:
                if handler is not None:
                    logging.getLogger().handlers = old_handlers or []
                    handler.close()
                    logging.getLogger().setLevel(old_level)
                _RUN_LOCK.release()
