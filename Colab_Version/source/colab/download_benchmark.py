"""Optional local-overhead benchmark using the real downloader and ledger."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time

from app.download_progress import DownloadProgress, METRICS, ProgressPolicy


def benchmark_local_io(*, size_mb=100, chunk_sizes=(256 * 1024, 512 * 1024, 1024 * 1024), directory=None):
    """Synthetic binary stream: measures local overhead, never network bandwidth.

    Uses the production transfer loop, atomic ledger and progress policy. Results
    include actual bytes written and deliberately label remote timing as zero.
    The temporary work directory and its synthetic files are removed afterward.
    """
    from colab.pipeline_runner import shared_pipeline, _RUN_LOCK
    if not _RUN_LOCK.acquire(blocking=False):
        raise RuntimeError('Run benchmarks only while the pipeline is idle')
    core = shared_pipeline()
    names = ('DOWNLOAD_DIRECTORY', 'DOWNLOAD_STATE_FILE')
    old = {name: getattr(core, name) for name in names}
    try:
        rows = []
        with tempfile.TemporaryDirectory(dir=directory) as temporary:
            for chunk_size in chunk_sizes:
                root = Path(temporary) / str(chunk_size)
                core.DOWNLOAD_DIRECTORY = root
                core.DOWNLOAD_STATE_FILE = root / 'downloads.json'
                size = int(size_mb * 1024**2)
                class Response:
                    status_code = 200
                    headers = {'Content-Type': 'application/pdf', 'content-length': str(size)}

                    def iter_content(self, chunk_size):
                        chunk = b'x' * chunk_size
                        for offset in range(0, size, chunk_size):
                            yield chunk[:min(chunk_size, size - offset)]

                network = SimpleNamespace(
                    settings=SimpleNamespace(max_request_retries_per_attempt=1, max_stall_seconds=60),
                    session=lambda: SimpleNamespace(get=lambda *a, **kw: Response()),
                    request_options=lambda proxy: {}, result=lambda *a: None,
                    gateway_result=lambda *a, **kw: None,
                    progress_policy=replace(ProgressPolicy(), chunk_size=chunk_size),
                )
                progress = DownloadProgress(
                    lambda downloaded: core._update_download_state('test.pdf', '', 'DOWNLOADING', size=downloaded),
                    lambda *_: None, policy=network.progress_policy,
                )
                before = METRICS.snapshot()
                started = time.perf_counter()
                success = core.download_file('https://synthetic.invalid/test.pdf', 'test.pdf', '',
                                             network=network, on_progress=progress, on_status=lambda *_: None)
                elapsed = time.perf_counter() - started
                values = {key: value - before[key] for key, value in METRICS.snapshot().items()}
                rows.append(dict(source='synthetic local IO (no network)', chunk_kib=chunk_size // 1024,
                                 success=success is True, duration_seconds=elapsed,
                                 binary_mib_per_second=values['bytes_downloaded'] / 1024**2 / max(values['transfer_seconds'], 1e-9), **values))
        return rows
    finally:
        for name, value in old.items():
            setattr(core, name, value)
        _RUN_LOCK.release()
