"""Cheap lifecycle predicates; PDF parsing belongs to the validation pool."""
from pathlib import Path


def file_signature(path):
    try:
        stat = Path(path).stat()
        return [stat.st_size, stat.st_mtime_ns]
    except (OSError, TypeError):
        return None


def verified_download(entry, path=None):
    entry = entry or {}
    return (str(entry.get('status','')).upper() == 'COMPLETED'
            and entry.get('integrity_status') == 'VALID'
            and (path is None or Path(path).absolute() == Path(entry.get('integrity_path','')).absolute())
            and entry.get('integrity_signature') is not None
            and entry['integrity_signature'] == file_signature(path or entry.get('integrity_path')))
