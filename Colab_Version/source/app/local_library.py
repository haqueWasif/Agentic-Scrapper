"""Cached legacy-name lookup and bounded evidence retrieval over whole PDFs."""
from __future__ import annotations

import heapq
import json
import os
from pathlib import Path
import re
import tempfile

from pypdf import PdfReader


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         suffix='.tmp', delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def normalized_title(value):
    value = re.sub(r'^page_\d+_\d+_', '', str(value), flags=re.I)
    value = re.sub(r'\.pdf$', '', value, flags=re.I)
    return ' '.join(re.findall(r'[a-z0-9]+', value.lower()))


class LocalLibrary:
    """Stat-based cache. Startup never extracts page text or performs RAG.

    Only metadata of new/changed PDFs is read. Exact normalized titles preserve
    edition/year distinctions; ambiguous titles deliberately do not auto-match.
    """
    def __init__(self, data_dir, extra_roots=()):
        self.data_dir = Path(data_dir)
        self.roots = [self.data_dir, *(Path(root) for root in extra_roots)]
        self.index_path = self.data_dir / 'local_library_index.json'
        try:
            self.entries = json.loads(self.index_path.read_text(encoding='utf-8')).get('files', {})
        except (OSError, ValueError):
            self.entries = {}
        self.by_title = {}

    def refresh(self):
        current = {}
        for root_number, root in enumerate(self.roots):
            for path in root.rglob('*'):
                if path.suffix.lower() != '.pdf' or not path.is_file():
                    continue
                key = f'{root_number}/{path.relative_to(root).as_posix()}'
                stat = path.stat()
                signature = [stat.st_size, stat.st_mtime_ns]
                cached = self.entries.get(key, {})
                if cached.get('signature') != signature:
                    title = ''
                    try:
                        with path.open('rb') as source:
                            title = str((PdfReader(source).metadata or {}).get('/Title') or '')
                    except Exception:
                        pass
                    cached = {'signature': signature, 'title': title}
                current[key] = {**cached, 'path': str(path)}
        self.entries = current
        self.by_title = {}
        for entry in current.values():
            for title in {normalized_title(entry['title']), normalized_title(Path(entry['path']).name)} - {''}:
                self.by_title.setdefault(title, set()).add(entry['path'])
        atomic_json(self.index_path, {'version': 1, 'files': current})

    def match(self, title):
        matches = self.by_title.get(normalized_title(title), set())
        existing = list({Path(path).resolve(): Path(path) for path in matches if Path(path).is_file()}.values())
        if len(existing) == 1:
            # The legacy ledger is keyed by basename; avoid silently associating
            # two distinct PDFs with the same filename across library roots.
            namesakes = {Path(entry['path']).resolve() for entry in self.entries.values()
                         if Path(entry['path']).name == existing[0].name}
            if len(namesakes) > 1:
                return None
        return existing[0] if len(existing) == 1 else None


def retrieve_pdf_evidence(path, query, *, chunks_per_topic=4, chunk_characters=1400):
    """Parse every extractable page, retaining only bounded page-aware evidence.

    Ranking is local token overlap. Two bounded heaps independently retrieve
    publisher identity and the user's query; no model call occurs here.
    """
    identity_terms = set('ashrae american society heating refrigerating air conditioning engineers copyright published publisher standard handbook'.split())
    query_terms = set(re.findall(r'[a-z0-9]+', query.lower()))
    heaps = [[], []]
    text_pages = total_characters = sequence = 0
    errors = []
    try:
        with Path(path).open('rb') as source:
            reader = PdfReader(source)
            page_count = len(reader.pages)
            metadata = {str(k).lstrip('/'): str(v) for k, v in (reader.metadata or {}).items()}
            for page_number, page in enumerate(reader.pages, 1):
                try:
                    text = (page.extract_text() or '').strip()
                except Exception:
                    errors.append(page_number)
                    continue
                total_characters += len(text)
                text_pages += int(bool(text))
                for offset in range(0, len(text), chunk_characters):
                    chunk = text[offset:offset + chunk_characters]
                    tokens = set(re.findall(r'[a-z0-9]+', chunk.lower()))
                    sequence += 1
                    for heap, terms in zip(heaps, (identity_terms, query_terms)):
                        score = len(tokens & terms) / max(1, len(terms))
                        item = (score, -sequence, page_number, chunk)
                        if len(heap) < chunks_per_topic:
                            heapq.heappush(heap, item)
                        elif item > heap[0]:
                            heapq.heapreplace(heap, item)
        selected_pages = set()
        evidence = []
        for label, heap in zip(('ASHRAE publisher identity evidence', 'Query relevance evidence'), heaps):
            evidence.append(label)
            for _, _, page_number, chunk in sorted(heap, reverse=True):
                evidence.append(f'[Page {page_number}] {chunk}')
                selected_pages.add(page_number)
        bounded = '\n\n'.join(evidence)[:12000]
        return dict(text=bounded, metadata=metadata, page_count=page_count,
                    pages_sampled=sorted(selected_pages), pages_with_text=text_pages,
                    characters=len(bounded), toc_pages=[], pages_scanned=page_count,
                    extraction_quality='good' if total_characters >= 2500 else 'sparse',
                    error=None, extraction_failed_pages=errors)
    except Exception as exc:
        return dict(text='', metadata={}, page_count=0, pages_sampled=[], pages_with_text=0,
                    characters=0, toc_pages=[], pages_scanned=0, extraction_quality='empty',
                    error=f'{type(exc).__name__}: {exc}', extraction_failed_pages=[])
