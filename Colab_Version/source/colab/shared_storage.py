"""Immutable PDFs on a shared mounted directory; PostgreSQL owns coordination."""
import hashlib
import os
from pathlib import Path
import shutil
import tempfile


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


class SharedFiles:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key):
        target = (self.root / key).resolve()
        if self.root not in target.parents:
            raise ValueError('Persistent object key escapes shared storage')
        return target

    def persistent_file_exists(self, key, *, size=None, sha=None):
        if not key:
            return False
        path = self.path(key)
        return path.is_file() and (size is None or path.stat().st_size == size) and (sha is None or digest(path) == sha)

    def upload_completed_pdf(self, local_path, key):
        path = self.path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix='.upload-', dir=path.parent)
        try:
            with os.fdopen(fd, 'wb') as target, Path(local_path).open('rb') as source:
                shutil.copyfileobj(source, target, 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def download_existing_pdf(self, key, local_path):
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix='.restore-', dir=target.parent)
        os.close(fd)
        try:
            shutil.copyfile(self.path(key), temporary)
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
