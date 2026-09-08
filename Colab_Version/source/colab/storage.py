"""Coarse Drive persistence, entirely outside network chunk callbacks."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
from contextlib import nullcontext

from app.local_library import atomic_json


def rebase(value, old_root, new_root):
    if isinstance(value, dict):
        return {key: rebase(item, old_root, new_root) for key, item in value.items()}
    if isinstance(value, list):
        return [rebase(item, old_root, new_root) for item in value]
    if isinstance(value, str):
        # Saved paths may originate on Windows; normalize separators explicitly.
        normalized, prefix = value.replace('\\', '/'), str(old_root).replace('\\', '/').rstrip('/')
        if normalized.startswith(prefix + '/'):
            return str(Path(new_root) / normalized[len(prefix) + 1:])
    return value


def copy_prefix(source, destination):
    """Copy one bounded filesystem snapshot, then atomically publish it."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with source.open('rb') as reader:
            stat = os.fstat(reader.fileno())
            remaining = stat.st_size
            with tempfile.NamedTemporaryFile(dir=destination.parent, suffix='.tmp', delete=False) as writer:
                temporary = Path(writer.name)
                while remaining:
                    chunk = reader.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise OSError('Source changed while checkpointing')
                    writer.write(chunk)
                    remaining -= len(chunk)
        os.utime(temporary, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        os.replace(temporary, destination)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


class DriveSnapshots:
    def __init__(self, config, core):
        self.config, self.core = config, core
        self.local = Path(config.local_work_dir).resolve()
        self.drive = Path(config.drive_output_dir).resolve()
        self.enabled = config.use_google_drive
        self._stop = threading.Event()
        self._thread = None
        self._last_partials = time.monotonic()
        self._lock = threading.Lock()
        self._signatures = {}

    def restore(self):
        if not self.enabled or not self.drive.exists():
            return
        manifest = self.drive / 'snapshot_manifest.json'
        paths = (json.loads(manifest.read_text(encoding='utf-8'))['files'] if manifest.exists()
                 else [str(path.relative_to(self.drive)) for path in self.drive.rglob('*') if path.is_file()])
        for relative in paths:
            if Path(relative).is_absolute() or '..' in Path(relative).parts:
                raise ValueError('Snapshot path escapes its storage directory')
            source = self.drive / relative
            destination = self.local / relative
            if self.drive not in source.resolve().parents or self.local not in destination.absolute().parents:
                raise ValueError('Snapshot path escapes its storage directory')
            if not source.is_file() or destination.exists():
                continue  # Preserve this runtime's newer local progress and changes.
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.suffix == '.part':
                if self.config.checkpoint_partials_to_drive:
                    copy_prefix(source, destination)
            elif source.suffix == '.pdf':
                # Completed immutable data may be read from Drive on demand;
                # no multi-GB copy or page parsing at every notebook startup.
                try:
                    destination.symlink_to(source)
                except OSError:  # Filesystems without symlink support (Windows).
                    copy_prefix(source, destination)
            elif source.suffix == '.json':
                value = json.loads(source.read_text(encoding='utf-8'))
                atomic_json(destination, rebase(value, self.drive, self.local))

    def start(self):
        if not self.enabled:
            return
        def persist_loop():
            while not self._stop.wait(60):
                try:
                    self.sync()
                except Exception:
                    logging.getLogger(__name__).exception('Drive checkpoint failed; local data remains available')
        self._thread = threading.Thread(target=persist_loop, name='drive-checkpoint', daemon=True)
        self._thread.start()

    def sync(self, *, final=False):
        if not self.enabled:
            return
        telemetry=getattr(getattr(self.core,'_LIVE_NETWORK',None),'telemetry',None)
        with self._lock, (telemetry.timed(0,'DRIVE_SYNC') if telemetry else nullcontext()):
            self.drive.mkdir(parents=True, exist_ok=True)
            now = time.monotonic()
            partials = self.config.checkpoint_partials_to_drive and (
                final or now - self._last_partials >= self.config.partial_checkpoint_seconds)
            files = []
            # Completed PDFs first; metadata is committed afterward. Never hold
            # the ledger lock while copying large binary files.
            for source in self.local.rglob('*'):
                if not source.is_file() or source.suffix not in {'.pdf', '.part'}:
                    continue
                relative = source.relative_to(self.local)
                destination = self.drive / relative
                if source.suffix == '.part' and not partials:
                    if destination.exists():
                        files.append(relative.as_posix())
                    continue
                try:
                    if source.resolve() != destination.resolve():
                        stat = source.stat()
                        signature = (stat.st_size, stat.st_mtime_ns)
                        unchanged = destination.exists() and destination.stat().st_size == stat.st_size and (
                            self._signatures.get(relative.as_posix()) == signature or destination.stat().st_mtime_ns == stat.st_mtime_ns)
                        if not unchanged:
                            copy_prefix(source, destination)
                        self._signatures[relative.as_posix()] = signature
                    files.append(relative.as_posix())
                except FileNotFoundError:
                    continue  # Stage 2 moved this completed PDF; next pass catches it.
            if partials:
                self._last_partials = now
            for source in self.local.rglob('*.json'):
                if 'logs' in source.relative_to(self.local).parts:
                    continue
                relative = source.relative_to(self.local)
                if source == self.core.DOWNLOAD_STATE_FILE:
                    with self.core._DOWNLOAD_STATE_LOCK:
                        value = self.core._load_download_state()
                else:
                    try:
                        value = json.loads(source.read_text(encoding='utf-8'))
                    except (OSError, ValueError):
                        continue
                atomic_json(self.drive / relative, rebase(value, self.local, self.drive))
                files.append(relative.as_posix())
            atomic_json(self.drive / 'snapshot_manifest.json', {'version': 1, 'files': files})

    def stop_and_sync(self):
        self._stop.set()
        if self._thread:
            self._thread.join()
        self.sync(final=True)
