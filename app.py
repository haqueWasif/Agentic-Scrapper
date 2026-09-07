"""Streamlit entry point for the complete ASHRAE scraping pipeline."""

import asyncio
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager, nullcontext
import hashlib
import json
import logging
import os
import random
import re
from queue import Empty
import shutil
import tempfile
import threading
import time
import traceback
import urllib.parse
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import httpx
import streamlit as st
from bs4 import BeautifulSoup
from curl_cffi import requests as c_requests
from dotenv import load_dotenv
from pypdf import PdfReader

from app.agents.orchestrator import invoke_pdf_validation_graph, invoke_scraper_graph
from app.network_manager import NetworkManager
from app.observability import langsmith_status
from app.pipeline_events import DownloadPresentation, PipelineDashboardSnapshot, PipelineEventBus, RunMetrics
from app.pipeline_scheduler import BoundedWorkQueue, DownloadOutcome, GlobalDownloadQueue, GlobalRecoveryBacklog
from app.pipeline_state import lifecycle_view, stable_document_id
from app.pdf_validation import (
    check_pdf_integrity,
    completed_validation_report,
    extract_pdf_preview,
    scan_existing_pdfs,
    store_validated_pdf,
    validation_summary,
    write_validation_report,
)
from app.semantic_extractor import extract_markdown


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOAD_DIRECTORY = PROJECT_ROOT / "data" / "ASHRAE_Files"
LOW_RELEVANCE_DIRECTORY = PROJECT_ROOT / "data" / "Low_Relevance_Files"
SEARCH_CACHE_DIRECTORY = PROJECT_ROOT / "data" / "search_cache"
DOWNLOAD_STATE_FILE = PROJECT_ROOT / "data" / "downloads.json"
DATA_DIRECTORY = PROJECT_ROOT / "data"
SEARCH_TIMEOUT_SECONDS = 45
SEARCH_RETRIES = 2
DEFAULT_MODE = "research"
VALIDATION_TIMEOUT_SECONDS = 3
_DOWNLOAD_STATE_LOCK = threading.Lock()
_VALIDATION_QUEUE: BoundedWorkQueue | None = None
_VALIDATION_QUEUE_LOCK = threading.Lock()
_PIPELINE_EVENTS = PipelineEventBus()
BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}
QUERY_TERMS = [
    "ashrae", "hvac", "chiller", "chilled water",
    "air-conditioning", "air conditioning", "refrigeration", "refrigerant",
    "cooling tower", "heating", "ventilating", "ventilation",
    "psychrometric", "compressor", "condenser", "evaporator", "ductwork",
    "handbook", "standard", "guideline", "ansi/ashrae",
]
CORE_ASHRAE_STANDARDS = [
    "15", "23", "30", "34", "55", "62.1", "62.2",
    "90.1", "90.2", "135", "154", "170", "180", "181", "189.1",
]


def _emit_pipeline_event(kind: str, *, candidate: dict[str, Any] | None = None, filename: str = "",
                         count: int = 1, message: str = "", **fields: Any) -> None:
    """Publish lifecycle telemetry without allowing worker threads to call Streamlit."""
    source = candidate or {}
    _PIPELINE_EVENTS.emit(
        kind,
        run_id=str(source.get("run_id") or fields.pop("run_id", "")),
        filename=filename or str(source.get("filename") or ""),
        count=count,
        message=message,
        **fields,
    )


def verify_ashrae_relevance(file_path, filename, query_terms, core_standards):
    """Port the baseline first-page rules, including pass-on-unreadable behavior."""
    try:
        with open(file_path, "rb") as pdf_file:
            reader = PdfReader(pdf_file)
            if len(reader.pages) > 0:
                first_page_text = reader.pages[0].extract_text()
                if not first_page_text or len(first_page_text.strip()) < 50:
                    return True
                text_lower = first_page_text.lower()
                title_lower = filename.lower()
                if "ashrae" not in title_lower and "ashrae" not in text_lower:
                    return False
                for std_num in core_standards:
                    pattern = rf"\b(standard|st|std)[\s/_-]*{re.escape(std_num)}\b"
                    if re.search(pattern, title_lower) or re.search(pattern, text_lower):
                        return True
                if "handbook" in title_lower and any(
                    word in title_lower
                    for word in ("fundamentals", "refrigeration", "applications", "systems")
                ):
                    return True
                return sum(1 for term in query_terms if term in text_lower) >= 2
    except Exception:
        logging.getLogger(__name__).warning(
            "First-page verification unavailable for %s; retaining per baseline policy", filename,
        )
    return True


def _safe_pdf_filename(title: str, page_number: int, item_number: int) -> str:
    """Build a filesystem-safe, collision-resistant PDF filename."""
    clean_title = re.sub(r"[^A-Za-z0-9._ -]+", "_", title).strip(" ._")
    clean_title = re.sub(r"\s+", "_", clean_title)[:100] or "document"
    return f"page_{page_number:03d}_{item_number:03d}_{clean_title}.pdf"


def _candidate_provenance(candidate: dict[str, Any]) -> tuple[int | None, int | None]:
    """Return source page/item metadata, including a safe legacy filename fallback."""
    try:
        page = int(candidate.get("source_page"))
        item = int(candidate.get("source_item"))
        if page > 0 and item > 0:
            return page, item
    except (TypeError, ValueError):
        pass
    match = re.match(r"page_(\d{3})_(\d{3})_", str(candidate.get("filename") or ""), re.IGNORECASE)
    return (int(match.group(1)), int(match.group(2))) if match else (None, None)


def _candidate_provenance_label(candidate: dict[str, Any]) -> str:
    page, item = _candidate_provenance(candidate)
    return f"source page {page} · item {item}" if page is not None and item is not None else "source page unavailable"


def _extract_search_mirror_rows(html: str, search_url: str) -> list[dict[str, list[str]]]:
    """Keep mirror candidates grouped by their original book row, never by title."""
    rows = []
    for row in BeautifulSoup(html, "html.parser").find_all("tr"):
        if not row.find_all("td", recursive=False) or row.find("tr") is not None:
            continue
        row_links, library_mirrors, libgen_mirrors, other_mirrors = [], [], [], []
        for anchor in row.find_all("a", href=True):
            if anchor.find_parent("tr") is not row:
                continue  # Do not merge nested table rows into a parent row.
            href = str(anchor.get("href", "")).strip()
            if not href or href.startswith("#"):
                continue
            link = urllib.parse.urldefrag(urljoin(search_url, href))[0]
            parsed = urlparse(link)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                continue
            if link not in row_links:
                row_links.append(link)
            label = anchor.get_text(" ", strip=True).strip("[]() ")
            path = parsed.path.lower()
            # Search/author navigation is not an alternative download mirror.
            if path in ("", "/") or "req" in urllib.parse.parse_qs(parsed.query):
                continue
            host = parsed.hostname.lower()
            # Libgen result rows use two complementary routes for the same MD5:
            # library.lol's book page and a libgen ads/get page.  Preserve both,
            # with library.lol first, so a dead mirror can hand straight to its peer.
            is_library = host == "library.lol" or host.endswith(".library.lol")
            is_libgen = host.startswith("libgen.") or ".libgen." in host
            is_mirror_route = any(token in path for token in ("ads.php", "get.php", "book/", "main/"))
            if is_library and (is_mirror_route or label.isdigit()):
                if link not in library_mirrors:
                    library_mirrors.append(link)
            elif is_libgen:
                if link not in libgen_mirrors:
                    libgen_mirrors.append(link)
            elif label.isdigit() or is_mirror_route:
                # Keep existing row-scoped third-party mirrors as a last resort.
                if link not in other_mirrors:
                    other_mirrors.append(link)
        mirror_links = library_mirrors + libgen_mirrors + other_mirrors
        if mirror_links:
            rows.append({"links": row_links, "mirror_links": mirror_links})
    return rows


def _document_mirror_links(document: dict, rows: list[dict], search_url: str) -> list[str]:
    """Associate an evaluated document with exactly one source row."""
    href = str(document.get("link") or "").strip()
    if not href:
        return []
    primary = urllib.parse.urldefrag(urljoin(search_url, href))[0]
    if urlparse(primary).scheme not in ("http", "https"):
        return []
    matches = [row for row in rows if primary in row["links"]]
    if not matches:
        def md5(link):
            parsed = urlparse(link)
            query = urllib.parse.parse_qs(parsed.query)
            values = [value for key, items in query.items() if key.lower() == "md5" for value in items]
            values += parsed.path.split("/")
            return next((value.lower() for value in values if re.fullmatch(r"[a-fA-F0-9]{32}", value)), None)

        document_md5 = md5(primary)
        if document_md5:
            matches = [row for row in rows if any(md5(link) == document_md5 for link in row["links"])]
    if len(matches) == 1:
        mirrors = matches[0]["mirror_links"]
        # Keep the evaluator's selection first only if it is an actual row mirror.
        return ([primary] if primary in mirrors else []) + [link for link in mirrors if link != primary]
    # An ambiguous/missing association must never borrow another book's mirrors.
    return [primary]


def _extract_search_documents(html: str, search_url: str) -> list[dict[str, str]]:
    """Extract one evaluator-safe title/link record per visible search-result row."""
    documents: list[dict[str, str]] = []
    seen_links: set[str] = set()
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if not cells or row.find("tr") is not None:
            continue
        row_text = row.get_text(" ", strip=True)
        for anchor in row.find_all("a", href=True):
            if anchor.find_parent("tr") is not row:
                continue
            href = str(anchor.get("href", "")).strip()
            link = urllib.parse.urldefrag(urljoin(search_url, href))[0]
            parsed = urlparse(link)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                continue
            query = urllib.parse.parse_qs(parsed.query)
            # Exclude result-page navigation and retain only document/mirror candidates.
            if parsed.path in ("", "/", "/index.php") and "req" in query:
                continue
            if link in seen_links:
                continue
            label = anchor.get_text(" ", strip=True)
            if not label and not any(token in link.lower() for token in ("ads.php", "book/", "main/", "md5=")):
                continue
            seen_links.add(link)
            documents.append({
                "title": row_text[:240] if not label or label.isdigit() else label,
                "link": link,
                "text": row_text,
            })
            break
    return documents


def _evaluator_document_context(documents: list[dict[str, str]]) -> str:
    """Pass bounded, link-preserving result records to the existing semantic agent."""
    records = []
    for document in documents:
        records.append(
            f"Title: {document['title']}\n"
            f"Source URL: {document['link']}\n"
            f"Search-result text: {document['text'][:1200]}"
        )
    return "\n\n--- DOCUMENT RECORD ---\n".join(records)


def _download_paths(filename: str) -> tuple[str, Path, Path]:
    """Constrain output to the project's download directory."""
    safe_filename = Path(filename).name
    if not safe_filename or safe_filename in (".", ".."):
        raise ValueError("A valid filename is required")
    if not safe_filename.lower().endswith(".pdf"):
        safe_filename += ".pdf"
    DOWNLOAD_DIRECTORY.mkdir(parents=True, exist_ok=True)
    destination = DOWNLOAD_DIRECTORY / safe_filename
    return safe_filename, destination, destination.with_name(destination.name + ".part")


def _load_download_state() -> dict[str, Any]:
    """Read the durable transfer ledger; malformed state never blocks a run."""
    try:
        if DOWNLOAD_STATE_FILE.exists():
            state = json.loads(DOWNLOAD_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(state, dict) and isinstance(state.get("downloads"), dict):
                return state
    except (OSError, json.JSONDecodeError) as exc:
        logging.getLogger(__name__).warning("Ignoring unreadable download state: %s", type(exc).__name__)
    return {"downloads": {}}


@contextmanager
def _download_state_process_lock():
    """Cooperatively serialize the ledger transaction across app processes."""
    lock_path = DOWNLOAD_STATE_FILE.with_name(DOWNLOAD_STATE_FILE.name + ".lock")
    handle = None
    acquired = False
    try:
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = lock_path.open("a+b")
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
        except OSError as exc:
            logging.getLogger(__name__).warning(
                "Download state persistence temporarily failed: ledger lock unavailable: %s", exc,
            )
            yield False
            return
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError:
                time.sleep(0.05)
        if not acquired:
            logging.getLogger(__name__).warning(
                "Download state persistence temporarily failed: another process holds the ledger lock."
            )
        yield acquired
    finally:
        if acquired:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        if handle:
            handle.close()


def _save_download_state(state: dict[str, Any]) -> bool:
    """Atomically persist the ledger; callers already hold ``_DOWNLOAD_STATE_LOCK``.

    A unique same-directory temporary file avoids collisions between workers or
    overlapping Streamlit runs.  The previous valid ledger remains untouched if
    Windows briefly locks the destination during ``os.replace``.
    """
    temporary_path: Path | None = None
    try:
        DOWNLOAD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=DOWNLOAD_STATE_FILE.parent,
            prefix="downloads_", suffix=".tmp", delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            json.dump(state, temporary_file, indent=2, sort_keys=True)
            temporary_file.flush()
            try:
                os.fsync(temporary_file.fileno())
            except OSError:
                # fsync is best-effort on filesystems that do not support it;
                # the closed-file + atomic-replace sequence is still preserved.
                pass

        for replace_attempt in range(5):
            try:
                os.replace(temporary_path, DOWNLOAD_STATE_FILE)
                return True
            except PermissionError as exc:
                if replace_attempt == 4:
                    logging.getLogger(__name__).warning(
                        "Download state persistence temporarily failed after 5 replace attempts: %s", exc,
                    )
                    return False
                logging.getLogger(__name__).debug(
                    "Download state persistence temporarily locked; retry %s/5", replace_attempt + 2,
                )
                time.sleep(0.05 * (2 ** replace_attempt))
            except FileNotFoundError as exc:
                # Never recreate or replace a missing temp file: another actor
                # may only affect its own unique path, so preserve the old ledger.
                logging.getLogger(__name__).warning(
                    "Download state persistence temporarily failed; temp file unavailable: %s", exc,
                )
                return False
    except OSError as exc:
        logging.getLogger(__name__).warning(
            "Download state persistence temporarily failed before atomic replacement: %s", exc,
        )
        return False
    finally:
        if temporary_path and temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                # Only our uniquely-created path is considered for cleanup.
                pass
    return False


def _update_download_state(
    filename: str,
    url: str,
    status: str,
    *,
    size: int | None = None,
    reason: str | None = None,
    candidate: dict[str, Any] | None = None,
    document_attempt: int | None = None,
    request_attempt: int | None = None,
    completed: bool | None = None,
    retry_after_seconds: float | None = None,
    validation_status: str | None = None,
    technical_error_type: str | None = None,
    technical_error_message: str | None = None,
) -> bool:
    """Atomically record transfer and round-recovery state for one document."""
    with _DOWNLOAD_STATE_LOCK:
        with _download_state_process_lock() as acquired:
            if not acquired:
                return False
            state = _load_download_state()
            entry = state["downloads"].get(filename, {})
            _, destination, part_path = _download_paths(filename)
            current_size = int(size if size is not None else _download_size(filename))
            previous_size = int(entry.get("bytes_downloaded", entry.get("size", 0)) or 0)
            entry.update({
                "filename": filename,
                "url": url or entry.get("url", ""),
                "status": status,
                "size": current_size,
                "bytes_downloaded": current_size,
                "part_path": str(part_path),
                "last_source": url or entry.get("last_source", entry.get("url", "")),
                "last_progress_time": int(time.time()) if current_size > previous_size else entry.get("last_progress_time"),
                "last_attempt_time": int(time.time()),
                "updated_at": int(time.time()),
            })
            if candidate:
                # Metadata is limited to the already-approved row information needed
                # to refresh a mirror in a later recovery round or after an app restart.
                entry["candidate"] = candidate
                entry["document_id"] = str(candidate.get("document_id") or filename)
                entry["title"] = str(candidate.get("title") or entry.get("title") or filename)
                entry["query"] = str(candidate.get("query") or entry.get("query") or "")
                entry["run_id"] = str(candidate.get("run_id") or entry.get("run_id") or "")
            if document_attempt is not None:
                entry["document_attempt"] = int(document_attempt)
            if request_attempt is not None:
                entry["request_attempt"] = int(request_attempt)
            if completed is not None:
                entry["completed"] = bool(completed)
            elif status.upper() == "COMPLETED":
                entry["completed"] = True
            elif status.upper() in {"QUEUED", "DOWNLOADING", "FAILED_FOR_ROUND", "RECOVERING", "PERMANENTLY_FAILED", "STAGE1_REJECTED"}:
                entry["completed"] = False
            if retry_after_seconds is not None:
                entry["retry_after_until"] = time.time() + max(0.0, float(retry_after_seconds))
            elif status.upper() == "COMPLETED":
                entry.pop("retry_after_until", None)
            if validation_status is not None:
                entry["validation_status"] = validation_status
            if technical_error_type:
                entry["technical_error"] = {
                    "error_type": str(technical_error_type)[:80],
                    "error_message": str(technical_error_message or "")[:240],
                    "timestamp": int(time.time()),
                }
                entry["error_type"] = entry["technical_error"]["error_type"]
                entry["error_message"] = entry["technical_error"]["error_message"]
            if reason:
                entry["reason"] = reason
                entry["last_error"] = reason
            else:
                entry.pop("reason", None)
                entry.pop("last_error", None)
            source_candidate = candidate or entry.get("candidate") or {}
            entry["source_metadata"] = {
                **(entry.get("source_metadata") if isinstance(entry.get("source_metadata"), dict) else {}),
                "source_url": entry.get("url", ""),
                "mirrors": list(source_candidate.get("mirrors", [])),
                "title": entry.get("title", filename),
            }
            lifecycle_builder = globals().get("lifecycle_view")
            if callable(lifecycle_builder):
                entry["lifecycle"] = lifecycle_builder(
                    entry, status=status, candidate=source_candidate,
                    validation_status=validation_status,
                )
            state["downloads"][filename] = entry
            return _save_download_state(state)


def _download_state_entry(filename: str) -> dict[str, Any] | None:
    with _DOWNLOAD_STATE_LOCK:
        entry = _load_download_state()["downloads"].get(filename)
    return entry if isinstance(entry, dict) else None


def _completed_download_count() -> int:
    """Count durable finished PDFs once, including files created before the ledger existed."""
    if not DOWNLOAD_DIRECTORY.exists():
        return 0
    paths = list(DOWNLOAD_DIRECTORY.glob("*.pdf"))
    # Read and (when necessary) persist the ledger once.  Calling
    # ``_download_state_entry`` for every file repeatedly parsed the multi-MB
    # downloads.json file and dominated Streamlit's initial render time.
    with _DOWNLOAD_STATE_LOCK:
        with _download_state_process_lock() as acquired:
            if not acquired:
                return len(paths)
            state = _load_download_state()
            downloads = state["downloads"]
            changed = False
            now = int(time.time())
            for path in paths:
                entry = downloads.get(path.name)
                if isinstance(entry, dict) and str(entry.get("status", "")).upper() == "COMPLETED":
                    continue
                size = path.stat().st_size
                entry = dict(entry) if isinstance(entry, dict) else {}
                entry.update({
                    "filename": path.name,
                    "url": str(entry.get("url") or ""),
                    "status": "COMPLETED",
                    "size": size,
                    "bytes_downloaded": size,
                    "completed": True,
                    "updated_at": now,
                })
                candidate = entry.get("candidate") if isinstance(entry.get("candidate"), dict) else {}
                entry["lifecycle"] = lifecycle_view(
                    entry, status="COMPLETED", candidate=candidate,
                    validation_status=entry.get("validation_status"),
                )
                downloads[path.name] = entry
                changed = True
            if changed:
                _save_download_state(state)
    return len(paths)


def _stage1_completed_document_ids() -> set[str]:
    """Return durable identities already through Stage 1 for idempotent reruns."""
    with _DOWNLOAD_STATE_LOCK:
        records = _load_download_state().get("downloads", {}).values()
    completed: set[str] = set()
    for entry in records:
        if not isinstance(entry, dict):
            continue
        lifecycle = entry.get("lifecycle") if isinstance(entry.get("lifecycle"), dict) else {}
        stage1 = lifecycle.get("stage1") if isinstance(lifecycle.get("stage1"), dict) else {}
        if stage1.get("completed") and entry.get("document_id"):
            completed.add(str(entry["document_id"]))
    return completed


def _pdf_sha256(file_path: Path) -> str:
    """Hash completed content incrementally without loading large PDFs into memory."""
    digest = hashlib.sha256()
    with file_path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_completed_content_hash(filename: str) -> dict[str, Any] | None:
    """Persist a PDF hash and return its canonical-content relationship.

    The PDF is never removed.  A later Stage 2 worker can use this compact
    result to avoid a second extraction/LLM evaluation for byte-identical data.
    """
    _, destination, _ = _download_paths(filename)
    if not destination.exists():
        return None
    try:
        digest = _pdf_sha256(destination)
    except OSError as exc:
        logging.getLogger(__name__).warning("Could not hash completed PDF %s: %s", filename, type(exc).__name__)
        return None
    with _DOWNLOAD_STATE_LOCK:
        with _download_state_process_lock() as acquired:
            if not acquired:
                return {"sha256": digest, "duplicate_of": None, "canonical_filename": None, "canonical_validation_status": None}
            state = _load_download_state()
            entry = state["downloads"].get(filename)
            if not isinstance(entry, dict):
                return {"sha256": digest, "duplicate_of": None, "canonical_filename": None, "canonical_validation_status": None}
            duplicate_of = None
            canonical_filename = None
            canonical_validation_status = None
            for other_filename, other in state["downloads"].items():
                if other_filename == filename or not isinstance(other, dict):
                    continue
                if other.get("content_sha256") == digest and str(other.get("status", "")).upper() == "COMPLETED":
                    duplicate_of = str(other.get("document_id") or other_filename)
                    canonical_filename = other_filename
                    canonical_validation_status = str(other.get("validation_status") or "PENDING").upper()
                    break
            entry["content_sha256"] = digest
            if duplicate_of:
                entry["duplicate_of"] = duplicate_of
            else:
                entry.pop("duplicate_of", None)
            lifecycle = entry.get("lifecycle") if isinstance(entry.get("lifecycle"), dict) else {}
            download_lifecycle = lifecycle.get("download") if isinstance(lifecycle.get("download"), dict) else {}
            entry["lifecycle"] = {
                **lifecycle,
                "download": {**download_lifecycle, "content_sha256": digest, "duplicate_of": duplicate_of},
            }
            _save_download_state(state)
    return {
        "sha256": digest,
        "duplicate_of": duplicate_of,
        "canonical_filename": canonical_filename,
        "canonical_validation_status": canonical_validation_status,
    }


def _propagate_duplicate_validation(canonical_filename: str, validation_status: str) -> int:
    """Copy a terminal canonical Stage 2 result to matching duplicate records."""
    status = str(validation_status).upper()
    if status not in {"APPROVED", "REJECTED"}:
        return 0
    canonical_entry = _download_state_entry(canonical_filename) or {}
    canonical_id = str(canonical_entry.get("document_id") or canonical_filename)
    with _DOWNLOAD_STATE_LOCK:
        duplicates = [
            {"filename": str(entry.get("filename")), "candidate": dict(entry.get("candidate") or {})}
            for entry in _load_download_state().get("downloads", {}).values()
            if isinstance(entry, dict)
            and str(entry.get("duplicate_of") or "") == canonical_id
            and str(entry.get("validation_status") or "").upper() == "DUPLICATE_PENDING"
        ]
    event_emitter = globals().get("_emit_pipeline_event")
    for duplicate in duplicates:
        filename = duplicate["filename"]
        _update_download_state(
            filename, "", "COMPLETED", size=_download_size(filename), completed=True,
            validation_status=status,
            reason=f"Byte-identical to {canonical_filename}; inherited canonical Stage 2 {status.lower()} result",
        )
        if callable(event_emitter):
            event_emitter(f"STAGE2_{status}", candidate=duplicate["candidate"], filename=filename)
    return len(duplicates)


def _download_size(filename: str) -> int:
    _, destination, part_path = _download_paths(filename)
    if destination.exists():
        return destination.stat().st_size
    return part_path.stat().st_size if part_path.exists() else 0


def log_status(message: str, on_status=None) -> None:
    """Send technical transfer events to the file card and debug history."""
    logging.getLogger(__name__).info("download: %s", message)
    if threading.current_thread() is threading.main_thread():
        try:
            history = st.session_state.setdefault("_download_debug", [])
            history.append(message)
            del history[:-500]
        except Exception:
            pass
    if on_status:
        on_status(message)


def download_file(direct_link, filename, referer_url, *, on_status=None, on_progress=None, network=None, worker_id=0, proxy=None):
    """Port of the proven baseline transfer loop; returns True or ``retry_next``."""
    (on_status or logging.getLogger(__name__).info)(f"TRACE download_file ENTER: URL={direct_link}; filename={filename}")
    status_logger = globals().get("log_status")
    report = (
        (lambda message: status_logger(message, on_status))
        if status_logger else (on_status or logging.getLogger(__name__).info)
    )
    _, file_path, part_file_path = _download_paths(filename)
    state_updater = globals().get("_update_download_state")

    def record_state(status: str, reason: str | None = None) -> None:
        if state_updater:
            state_updater(
                filename, direct_link, status, size=_download_size(filename), reason=reason,
            )

    record_state("partial" if part_file_path.exists() else "downloading")
    report(f"Filename: {filename}\nGateway attempted: {direct_link}")
    download_headers = {
        "Referer": referer_url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    current_gateway = direct_link
    # REQUEST-LEVEL RETRY: this is intentionally bounded.  A worker returns
    # control to the document scheduler after this small budget, where the
    # document can be retried in a later recovery round without starving peers.
    request_budget = (
        network.settings.max_request_retries_per_attempt
        if network else max(1, int(os.getenv("MAX_REQUEST_RETRIES_PER_ATTEMPT", "2")))
    )
    temporary_attempt = 0
    request_attempt = 0
    transfer_started_at = time.monotonic()
    retry_origin_size = None
    last_progress_time = transfer_started_at

    def retry_same_gateway(session, reason: str, current_size: int, *, refresh_cookies: bool = False) -> bool:
        """Retry a short transient transfer only within this document attempt."""
        nonlocal temporary_attempt, retry_origin_size
        if temporary_attempt + 1 >= request_budget:
            stall = retry_origin_size == current_size
            report(
                f"Gateway unhealthy:\nURL: {current_gateway}\nExisting bytes: {current_size}\n"
                f"Reason: {'download stalled after retry; no new bytes received' if stall else reason}\n"
                "Action: request retry budget exhausted; preserve .part and schedule the document for the next recovery round."
            )
            record_state("partial" if part_file_path.exists() else "failed", f"gateway unhealthy: {reason}")
            return False

        if refresh_cookies:
            cookies = getattr(session, "cookies", None)
            clear_cookies = getattr(cookies, "clear", None)
            if callable(clear_cookies):
                clear_cookies()
                cookie_action = "refresh session cookies + retry same gateway"
            else:
                cookie_action = "retry same gateway (session exposes no cookie jar)"
        else:
            cookie_action = "retry same gateway"

        temporary_attempt += 1
        retry_origin_size = current_size
        # Keep the proven short cooldown before one same-source Range retry.
        # Longer document-level cooling happens between recovery rounds.
        retry_delay = random.randint(8, 15)
        record_state("partial" if part_file_path.exists() else "failed", reason)
        report(
            f"Download recovery:\nURL: {current_gateway}\nExisting bytes: {current_size}\n"
            f"Reason: {reason}\nAction: {cookie_action}\n"
            + (f"Range: bytes={current_size}-" if current_size else "Range: (none; no partial file yet)")
            + f"\nNext retry: {retry_delay} seconds (request retry {temporary_attempt + 1}/{request_budget}; HTTP/1.1 streaming enabled)."
        )
        time.sleep(retry_delay)
        return True

    while request_attempt < request_budget:
        existing_size = part_file_path.stat().st_size if part_file_path.exists() else 0
        report(f"TRACE .part detected={part_file_path.exists()}; path={part_file_path}; existing_size={existing_size}")
        bytes_downloaded = existing_size
        transfer_started = existing_size > 0
        if existing_size > 0:
            download_headers["Range"] = f"bytes={existing_size}-"
            if request_attempt > 0:
                report(
                    f"Filename: {filename}\nGateway attempted: {direct_link}\n"
                    f"♻️ Found .part file after connection loss. Resuming via HTTP Range headers from byte {existing_size} "
                    f"(request attempt {request_attempt + 1})."
                )
            else:
                report(f"♻️ Found .part file. Resuming download via HTTP Range headers from byte {existing_size}...")
        else:
            if "Range" in download_headers:
                del download_headers["Range"]
        try:
            report(f"Transfer state: current_gateway={current_gateway}; bytes_downloaded={bytes_downloaded}; request_attempt={request_attempt + 1}")
            if state_updater:
                state_updater(
                    filename, direct_link, "DOWNLOADING", size=existing_size,
                    request_attempt=request_attempt + 1, completed=False,
                )
            with (nullcontext(network.session()) if network else c_requests.Session()) as s:
                report(f"TRACE curl_cffi request: URL={direct_link}; Range={download_headers.get('Range', '(none)')}")
                report(
                    f"🛡️ curl_cffi (chrome120): Initiating TLS connection to {direct_link} "
                    f"(request attempt {request_attempt + 1}, connect timeout=15s, read timeout=60s, Referer={referer_url}, HTTP/1.1)..."
                )
                r = s.get(
                    direct_link, headers=download_headers, stream=True,
                    timeout=(15, 60), impersonate="chrome120", http_version="v1",
                    **(network.request_options(proxy) if network else {}),
                )
                report(
                    f"📡 Gateway response: HTTP {r.status_code}; "
                    f"Content-Type={r.headers.get('Content-Type', 'not supplied')}; "
                    f"Content-Length={r.headers.get('content-length', 'unknown')} bytes."
                )
                if r.status_code >= 500:
                    if network:
                        network.result(proxy, False)
                        network.gateway_result(direct_link, r.status_code)
                    reason = f"HTTP {r.status_code} after Range request" if existing_size else f"HTTP {r.status_code} before transfer"
                    if retry_same_gateway(s, reason, existing_size, refresh_cookies=r.status_code == 500):
                        request_attempt += 1
                        continue
                    return "retry_next"
                if r.status_code == 416:
                    report(f"Filename: {filename}\nGateway attempted: {direct_link}\nFailure reason: range not satisfied; restarting from scratch.")
                    if part_file_path.exists():
                        part_file_path.unlink()
                    request_attempt += 1
                    continue
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After", "")
                    try:
                        retry_after_seconds = max(0, int(float(retry_after)))
                    except (TypeError, ValueError):
                        retry_after_seconds = 0
                    report(
                        f"Gateway deferred:\nURL: {direct_link}\nReason: HTTP 429\n"
                        f"Action: respecting access policy; document will wait for the next recovery round"
                        + (f" (Retry-After: {retry_after}s)." if retry_after else ".")
                    )
                    if state_updater:
                        state_updater(
                            filename, direct_link, "partial" if part_file_path.exists() else "failed",
                            size=_download_size(filename), reason=f"HTTP 429 Retry-After={retry_after}".strip(),
                            retry_after_seconds=retry_after_seconds,
                        )
                    if network:
                        network.result(proxy, False)
                        network.gateway_result(direct_link, r.status_code)
                    return "retry_next"
                if r.status_code not in [200, 206]:
                    report(f"Gateway permanently failed:\nURL: {direct_link}\nReason: HTTP {r.status_code}\nAction: switch gateway without retrying this response.")
                    record_state("partial" if part_file_path.exists() else "failed", f"HTTP {r.status_code}")
                    if network:
                        network.result(proxy, False)
                        network.gateway_result(direct_link, r.status_code)
                    return "retry_next"
                if "text/html" in r.headers.get("Content-Type", "").lower():
                    if transfer_started or existing_size > 0:
                        if network:
                            network.result(proxy, False)
                            network.gateway_result(direct_link)
                        reason = f"HTML response (HTTP {r.status_code}) during PDF transfer"
                        if retry_same_gateway(s, reason, existing_size, refresh_cookies=r.status_code == 500):
                            request_attempt += 1
                            continue
                        return "retry_next"
                    report(f"Gateway permanently failed:\nURL: {direct_link}\nReason: HTML challenge page (HTTP {r.status_code})\nAction: switch gateway.")
                    record_state("partial" if part_file_path.exists() else "failed", "HTML challenge page")
                    if network:
                        network.result(proxy, False)
                        network.gateway_result(direct_link, r.status_code)
                    return "retry_next"

                content_type = r.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                if content_type and content_type not in {
                    "application/pdf", "application/x-pdf", "application/octet-stream",
                    "binary/octet-stream", "application/download", "application/force-download",
                }:
                    report(f"Gateway permanently failed:\nURL: {direct_link}\nReason: invalid content type {content_type}\nAction: switch gateway.")
                    record_state("partial" if part_file_path.exists() else "failed", f"invalid content type: {content_type}")
                    if network:
                        network.result(proxy, False)
                        network.gateway_result(direct_link)
                    return "retry_next"

                transfer_started = True
                server_total_size = int(r.headers.get("content-length", 0))
                if r.status_code == 200:
                    existing_size = 0
                    mode = "wb"
                    total_size = server_total_size
                else:
                    total_size = server_total_size + existing_size
                    mode = "ab"

                initial_progress = existing_size if r.status_code == 206 else 0
                if on_progress:
                    on_progress(initial_progress, total_size)
                with part_file_path.open(mode) as pdf_file:
                    report(f"TRACE streaming ENTER: mode={mode}; expected_total={total_size}; offset={pdf_file.tell()}")
                    for chunk in r.iter_content(chunk_size=256 * 1024):
                        if chunk:
                            if pdf_file.tell() == initial_progress:
                                report(f"TRACE streaming first chunk: {len(chunk)} bytes")
                            pdf_file.write(chunk)
                            bytes_downloaded = pdf_file.tell()
                            last_progress_time = time.monotonic()
                            if on_progress:
                                on_progress(pdf_file.tell(), total_size)

                # Some interrupted streams end without raising a transport error.
                # Keep the partial rather than promoting a truncated response.
                if server_total_size and part_file_path.stat().st_size < total_size:
                    raise ConnectionError(f"Incomplete TCP transfer: {part_file_path.stat().st_size}/{total_size} bytes")

            if part_file_path.exists() and part_file_path.stat().st_size < 2048:
                report(f"Filename: {filename}\nGateway attempted: {direct_link}\nFailure reason: file is smaller than 2048 bytes.")
                part_file_path.unlink()
                record_state("failed", "file is smaller than 2048 bytes")
                return "retry_next"
            os.replace(part_file_path, file_path)
            record_state("completed")
            if network:
                network.result(proxy, True)
                network.gateway_result(direct_link, r.status_code, completed=True)
            elapsed_seconds = max(time.monotonic() - transfer_started_at, 0.001)
            completed_bytes = file_path.stat().st_size
            average_speed = completed_bytes / elapsed_seconds / (1024 * 1024)
            report(
                f"PDF: {filename}\nTime: {elapsed_seconds:.1f} seconds\n"
                f"Average speed: {average_speed:.1f} MB/s\nRetries: {temporary_attempt}\n"
                f"Gateway: {urlparse(current_gateway).hostname or current_gateway}"
            )
            return True
        except Exception as exc:
            new_size = part_file_path.stat().st_size if part_file_path.exists() else 0
            record_state("partial" if new_size else "failed", f"{type(exc).__name__}: {exc}")
            if network:
                network.result(proxy, False)
                network.gateway_result(direct_link)
            temporary_failure = (
                transfer_started or new_size > 0
                or isinstance(exc, (ConnectionError, TimeoutError))
                or type(exc).__name__ in {"DNSError", "IncompleteRead", "SocketError", "ReadTimeout"}
                or "http/2 stream" in str(exc).lower()
                or "connection reset" in str(exc).lower()
            )
            if not temporary_failure:
                report(f"Filename: {filename}\nGateway attempted: {direct_link}\nFailure reason: {type(exc).__name__}: {exc}; no partial transfer to resume, trying next gateway.")
                return "retry_next"
            bytes_downloaded = new_size
            stalled = new_size == existing_size and time.monotonic() - last_progress_time >= (
                network.settings.max_stall_seconds if network else 60
            )
            reason = "connection stalled without byte progress" if stalled else f"{type(exc).__name__}: {exc}"
            if retry_same_gateway(None, reason, bytes_downloaded):
                request_attempt += 1
                continue
            return "retry_next"
    record_state("partial" if part_file_path.exists() else "failed", "request retry budget exhausted")
    report(f"Filename: {filename}\nGateway attempted: {direct_link}\nRequest retry budget exhausted; preserving .part and returning control to the recovery scheduler.")
    return "retry_next"


def _check_download_relevance(filename, on_status=None) -> bool:
    """Retain baseline relevance rules and move rejected files without overwriting."""
    report = on_status or logging.getLogger(__name__).info
    safe_filename, destination, _ = _download_paths(filename)
    os.makedirs(LOW_RELEVANCE_DIRECTORY, exist_ok=True)
    report("📄 PyPDF: Extracting first-page text for strict ASHRAE/HVAC keyword and core-standard verification...")
    if verify_ashrae_relevance(destination, safe_filename, QUERY_TERMS, CORE_ASHRAE_STANDARDS):
        return True
    rejected_path = LOW_RELEVANCE_DIRECTORY / safe_filename
    suffix = 1
    while rejected_path.exists():
        rejected_path = LOW_RELEVANCE_DIRECTORY / f"{destination.stem}_{suffix}{destination.suffix}"
        suffix += 1
    shutil.move(str(destination), str(rejected_path))
    report(f"Moved to Low_Relevance_Files: {rejected_path.name}")
    return False


def parse_mirror_and_download(mirror_link, filename, needs_page_check=True, *, on_status=None, on_progress=None, _visited=None, _attempt_context=None, network=None, worker_id=0, proxy=None):
    """Port of the baseline mirror parser and alternative-gateway loop."""
    status_logger = globals().get("log_status")
    report = (
        (lambda message: status_logger(message, on_status))
        if status_logger else (on_status or logging.getLogger(__name__).info)
    )
    try:
        # This context is deliberately created by one document worker and
        # shared across all of that worker's top-level mirrors. It is never
        # persisted, so a later recovery round receives a fresh opportunity.
        attempt_context = _attempt_context if isinstance(_attempt_context, dict) else {}
        visited = attempt_context.setdefault("visited_pages", _visited if _visited is not None else set())
        attempted_routes = attempt_context.setdefault("attempted_download_routes", set())
        failed_routes = attempt_context.setdefault("failed_routes", set())

        def route_identity(link):
            """Conservative attempt-local URL identity: no fragment, normalized host/query order."""
            parsed = urlparse(urllib.parse.urldefrag(link)[0])
            host = (parsed.hostname or "").lower().rstrip(".")
            if not host:
                return urllib.parse.urldefrag(link)[0]
            try:
                port = f":{parsed.port}" if parsed.port else ""
            except ValueError:
                # A malformed port is not a route worth normalising further;
                # preserve it as-is so it cannot collapse a valid route.
                return urllib.parse.urldefrag(link)[0]
            query = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)), doseq=True)
            return urllib.parse.urlunparse((parsed.scheme.lower(), host + port, parsed.path, "", query, ""))

        mirror_link = urllib.parse.urldefrag(mirror_link)[0]
        mirror_identity = route_identity(mirror_link)
        if mirror_identity in visited or len(visited) >= 8:
            return "retry_next"
        visited.add(mirror_identity)
        mirror_parts = urlparse(mirror_link)
        mirror_host = (mirror_parts.hostname or "").lower()

        def is_ipfs_gateway(link):
            parts = urlparse(link)
            host = (parts.hostname or "").lower().rstrip(".")
            return any(token in host for token in (
                "ipfs.io", "cloudflare-ipfs", "gateway.ipfs", "pinata.cloud", "dweb.link",
            )) or "/ipfs/" in parts.path.lower()

        def candidate_priority(link):
            parts = urlparse(link)
            path = parts.path.lower()
            host = (parts.hostname or "").lower()
            if path.endswith(".pdf"):
                return 0
            if host == "library.lol" and path.startswith("/main/"):
                return 1
            if "get.php" in path:
                return 2
            if is_ipfs_gateway(link):
                return 3
            return 4

        def order_candidates(links):
            return sorted(
                links,
                key=lambda link: (
                    candidate_priority(link),
                    -(network.gateway_score(link) if network else 0),
                    link,
                ),
            )

        def blocked(link):
            parts = urlparse(link)
            host = (parts.hostname or "").lower().rstrip(".")
            return (
                parts.scheme not in ("http", "https") or not host
                or host in ("localhost", "::1", "0.0.0.0")
                or host.startswith("127.") or host.endswith(".localhost")
                or host == "onion" or host.endswith(".onion")
            )

        if blocked(mirror_link):
            return "retry_next"

        def attempt_download_route(direct_link, referer):
            identity = route_identity(direct_link)
            if identity in attempted_routes or identity in failed_routes:
                report(f"TRACE route skipped: already attempted during this document attempt: {direct_link}")
                return "retry_next"
            attempted_routes.add(identity)
            result = download_file(
                direct_link, filename, referer,
                on_status=on_status, on_progress=on_progress,
                network=network, worker_id=worker_id, proxy=proxy,
            )
            if result is not True:
                failed_routes.add(identity)
            return result

        # A result row can already contain a direct Libgen GET/CDN route.  Do
        # not fetch it as HTML first: send it to the proven resumable transfer.
        if (
            "get.php" in mirror_parts.path.lower()
            or mirror_parts.path.lower().endswith(".pdf")
            or is_ipfs_gateway(mirror_link)
        ):
            report(f"Final download candidates:\n1. {mirror_link}")
            return attempt_download_route(mirror_link, mirror_link)
        with (nullcontext(network.session()) if network else c_requests.Session()) as s:
            report(f"🔍 curl_cffi (chrome120): Fetching mirror page HTML from {mirror_link} (timeout=15s)...")
            res = s.get(
                mirror_link,
                timeout=15,
                impersonate="chrome120",
                **(network.request_options(proxy) if network else {}),
            )
            report(f"📡 Mirror response: HTTP {res.status_code} from {mirror_link}.")
            if res.status_code != 200:
                report(f"Filename: {filename}\nGateway attempted: {mirror_link}\nFailure reason: mirror page HTTP {res.status_code}.")
                return "retry_next"
            report("🕷️ BeautifulSoup: Parsing mirror HTML for GET, DOWNLOAD, IPFS and get.php links...")
            soup = BeautifulSoup(res.text, "html.parser")
        download_links = []
        discovery_links = []
        extracted_candidates = []

        def add_candidate(link):
            if link not in extracted_candidates:
                extracted_candidates.append(link)
            parsed = urlparse(link)
            host = (parsed.hostname or "").lower().rstrip(".")
            if parsed.scheme not in ("http", "https") or not host:
                return
            if blocked(link):
                return
            path = parsed.path.lower()
            identity = route_identity(link)
            if any(route in path for route in ("edition.php", "file.php", "ads.php")):
                if (host.startswith("libgen.") or ".libgen." in host) and identity not in visited and link not in discovery_links:
                    discovery_links.append(link)
                return
            if not ("get.php" in path or (host == "library.lol" and path.startswith("/main/")) or path.endswith(".pdf") or is_ipfs_gateway(link)):
                return
            if link not in download_links and identity not in attempted_routes and identity not in failed_routes:
                download_links.append(link)

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            text = anchor.get_text(strip=True).upper()
            if ("GET" in text or "DOWNLOAD" in text or "IPFS" in text
                    or any(token in href.lower() for token in ("get.php", "library.lol/main/", ".pdf"))):
                full_link = urljoin(mirror_link, href)
                add_candidate(full_link)
            elif any(route in href.lower() for route in ("edition.php", "file.php", "ads.php")):
                add_candidate(urljoin(mirror_link, href))
        if not download_links:
            for anchor in soup.find_all("a", href=True):
                if "md5=" in anchor["href"]:
                    full_link = urljoin(mirror_link, anchor["href"])
                    add_candidate(full_link)
        if not download_links:
            # Non-Libgen mirrors frequently hide a file URL behind a generic anchor,
            # button, onclick handler, or inline script. Keep the proven Libgen routes
            # above intact and inspect these broader page-level sources only as a fallback.
            fallback_values = []
            for anchor in soup.find_all("a", href=True):
                fallback_values.append(anchor["href"])
            for button in soup.find_all("button"):
                for attribute in ("href", "data-href", "data-url", "data-download", "onclick"):
                    value = button.get(attribute)
                    if value:
                        if attribute == "onclick":
                            fallback_values.extend(
                                re.findall(r"https?://[^\s\"'<>]+|/[^\s\"'<>]+", str(value))
                            )
                        else:
                            fallback_values.append(value)
            for element in soup.find_all(onclick=True):
                onclick = element.get("onclick", "")
                fallback_values.extend(
                    re.findall(r"https?://[^\s\"'<>]+|/[^\s\"'<>]+", str(onclick))
                )
            for script in soup.find_all("script"):
                fallback_values.extend(re.findall(r"https?://[^\s\"'<>]+|/[^\s\"'<>]+", script.get_text(" ")))

            candidate_tokens = (".pdf", "download", "file", "book", "get", "storage", "cloud", "/d/", "/download/")
            for value in fallback_values:
                value = str(value).strip().strip("\"'")
                if not value or value.lower().startswith(("javascript:", "#")):
                    continue
                if not any(token in value.lower() for token in candidate_tokens):
                    continue
                full_link = urljoin(mirror_link, value)
                parsed = urlparse(full_link)
                if (
                    parsed.scheme in ("http", "https")
                    and parsed.hostname
                    and full_link != mirror_link
                    and full_link not in download_links
                ):
                    add_candidate(full_link)
        if not download_links and not discovery_links:
            report(
                f"Filename: {filename}\nGateway attempted: {mirror_link}\n"
                "Failure reason: no valid download links found on mirror page."
            )
            return "retry_next"

        # Navigation pages can expose a preferred direct route. Resolve them before
        # falling through to an IPFS-only candidate set.
        if discovery_links and not any(candidate_priority(link) < 3 for link in download_links):
            for discovery_link in discovery_links:
                if parse_mirror_and_download(discovery_link, filename, False, on_status=on_status, on_progress=on_progress, _visited=visited, _attempt_context=attempt_context, network=network, worker_id=worker_id, proxy=proxy) is True:
                    return True

        download_links = order_candidates(download_links)
        report("Final download candidates:\n" + ("\n".join(
            f"{index}. {link}" for index, link in enumerate(download_links, start=1)
        ) or "None; checking internal discovery pages."))
        success = False
        pending_candidates = list(download_links)
        while pending_candidates:
            direct_link = pending_candidates.pop(0)
            if blocked(direct_link):
                continue
            if urlparse(direct_link).hostname == "library.lol" and urlparse(direct_link).path.startswith("/main/"):
                result = parse_mirror_and_download(direct_link, filename, False, on_status=on_status, on_progress=on_progress, _visited=visited, _attempt_context=attempt_context, network=network, worker_id=worker_id, proxy=proxy)
            else:
                result = attempt_download_route(direct_link, mirror_link)
            if result is True:
                success = True
                break
            if result == "retry_next":
                report(f"Filename: {filename}\nGateway attempted: {direct_link}\nFailure reason: switching to alternative gateway.")
                pending_candidates = order_candidates(pending_candidates)
                continue
            break
        if not success:
            for discovery_link in discovery_links:
                report(f"🔍 Internal discovery page (not a download): {discovery_link}")
                if parse_mirror_and_download(discovery_link, filename, False, on_status=on_status, on_progress=on_progress, _visited=visited, _attempt_context=attempt_context, network=network, worker_id=worker_id, proxy=proxy) is True:
                    success = True
                    break
        if success and filename.lower().endswith(".pdf") and needs_page_check:
            report(f"Filename: {filename}\nValidating PDF relevance")
            if not _check_download_relevance(filename, on_status):
                return False
        return success
    except Exception as exc:
        report(f"Filename: {filename}\nGateway attempted: {mirror_link}\nFailure reason: {type(exc).__name__}: {exc}")
        return False


def download_pdf(url: str | list[str], filename: str, *, defer_validation: bool = False) -> bool:
    """Synchronous Streamlit bridge for all mirrors belonging to one book."""
    with st.status(f"⬇️ Processing: {filename[:40]}...", expanded=False) as dl_status:
        dl_status.write("🛡️ curl_cffi (chrome120): Resolving this document's mirror and gateway routes...")
        progress_placeholder = st.empty()

        def stream_log(message):
            # Preserve every network event inside this card; byte updates still
            # reuse the progress bar so streaming does not flood the log.
            dl_status.write(message)

        status_logger = globals().get("log_status")
        report = (
            (lambda message: status_logger(message, stream_log))
            if status_logger else stream_log
        )

        def progress(downloaded, total):
            fraction = min(downloaded / total, 1.0) if total and total > 0 else 0.0
            size_text = f"{downloaded / (1024 * 1024):.2f} MB"
            if total:
                size_text += f" / {total / (1024 * 1024):.2f} MB"
            progress_placeholder.progress(fraction, text=f"Downloading: {filename[:30]}... ({size_text})")

        try:
            candidates = [url] if isinstance(url, str) else list(url)
            mirrors = []
            for candidate in candidates:
                if not isinstance(candidate, str):
                    continue
                candidate = urllib.parse.urldefrag(candidate.strip())[0]
                parsed = urlparse(candidate)
                if parsed.scheme in ("http", "https") and parsed.hostname and candidate not in mirrors:
                    mirrors.append(candidate)
            for mirror in mirrors:
                mirror_result = parse_mirror_and_download(
                    mirror, filename, needs_page_check=False, on_status=stream_log, on_progress=progress,
                )
                if mirror_result is True:
                    # Validation is deliberately queued in bulk collection so PDF
                    # extraction never prevents the next document from starting.
                    if defer_validation:
                        scheduled = _schedule_validation(filename)
                        validation_status = str((_download_state_entry(filename) or {}).get("validation_status") or "").upper()
                        report(
                            f"⚠️ PDF integrity gate marked {filename} technically invalid; Stage 2 was not queued."
                            if validation_status == "PDF_INVALID" else
                            f"📄 PyPDF: {'Queued' if scheduled else 'Deferred'} first-page ASHRAE/HVAC keyword verification in the background; "
                            f"validation taking over {VALIDATION_TIMEOUT_SECONDS}s is marked pending while downloads continue."
                        )
                        relevant = True
                    else:
                        relevant = _check_download_relevance(filename, report)
                    progress_placeholder.empty()
                    dl_status.update(
                        label=f"✅ Secured: {filename[:40]}" if relevant else f"⚠️ Low relevance: {filename[:40]}",
                        state="complete", expanded=False,
                    )
                    return relevant
                if mirror_result == "retry_next":
                    report(f"Filename: {filename}\nGateway attempted: {mirror}\nFailure reason: trying the next mirror.")
        except Exception as exc:
            report(f"Filename: {filename}\nFailure reason: {type(exc).__name__}: {exc}")
        progress_placeholder.empty()
        dl_status.update(label=f"❌ Failed: {filename[:40]}", state="error", expanded=False)
        return False


def _bulk_keyword_match(document: dict[str, str], query: str) -> bool:
    """Use lightweight title/row matching for collection mode; no agent call required."""
    text = f"{document.get('title', '')} {document.get('text', '')}".lower()
    query_terms = [term.lower() for term in re.findall(r"[a-zA-Z0-9.]+", query) if len(term) > 2]
    return any(term in text for term in query_terms) or any(term in text for term in QUERY_TERMS)


def _bulk_candidates_from_page(
    documents: list[dict[str, str]], rows: list[dict[str, list[str]]], search_url: str,
    query: str, target: int, page_number: int,
) -> tuple[list[dict[str, Any]], int]:
    """Build resumable download jobs from already-fetched result rows."""
    candidates: list[dict[str, Any]] = []
    skipped_completed = 0
    seen_filenames: set[str] = set()
    for item_number, document in enumerate(documents, start=1):
        if len(candidates) >= target or not _bulk_keyword_match(document, query):
            continue
        filename = _safe_pdf_filename(str(document.get("title") or "Untitled document"), page_number, item_number)
        if filename in seen_filenames:
            continue
        seen_filenames.add(filename)
        _, destination, _ = _download_paths(filename)
        entry = _download_state_entry(filename)
        if destination.exists():
            if not entry or str(entry.get("status", "")).upper() != "COMPLETED":
                _update_download_state(filename, str(document.get("link") or ""), "completed", size=destination.stat().st_size)
            skipped_completed += 1
            continue
        mirrors = _document_mirror_links(document, rows, search_url)
        if mirrors:
            candidates.append({
                "filename": filename, "mirrors": mirrors, "source_url": str(document.get("link") or ""),
                "source_page": page_number, "source_item": item_number,
            })
    return candidates, skipped_completed


def _validate_pdf_stage2(
    file_path: Path, source: str, *, stage1_score: int | None = None, on_status=None, force: bool = False,
) -> dict[str, Any]:
    """Run conservative Stage 2 validation and preserve inconclusive files as pending."""
    report = on_status or logging.getLogger(__name__).info
    existing_report = completed_validation_report(file_path, DATA_DIRECTORY)
    if existing_report and not force:
        return existing_report

    integrity = check_pdf_integrity(file_path)
    if not integrity["valid"]:
        report(
            f"⚠️ PDF integrity gate blocked Stage 2 for {file_path.name}: "
            f"{integrity['error_type']}"
        )
        preview = {
            "text": "", "metadata": {}, "page_count": int(integrity.get("page_count") or 0),
            "pages_sampled": [], "pages_with_text": 0, "characters": 0,
            "toc_pages": [], "extraction_quality": "empty", "error": integrity["error_message"],
        }
        validation = {
            "status": "PDF_INVALID", "approved": False, "score": 0, "confidence": "technical",
            "needs_more_text": False, "categories": [],
            "reason": f"{integrity['error_type']}: {integrity['error_message']}",
            "technical_error_type": integrity["error_type"], "technical_error_message": integrity["error_message"],
        }
    else:
        report(f"📄 PyPDF: Sampling representative pages and metadata for Stage 2 validation: {file_path.name}")
        preview = extract_pdf_preview(file_path)
        if preview["error"]:
            validation = {
                "status": "PENDING", "approved": False, "score": 0, "confidence": "low",
                "needs_more_text": False, "categories": [], "reason": preview["error"],
            }
        else:
            report(
                f"📄 Preview quality: {preview['extraction_quality']} · pages {preview['pages_sampled']} · "
                f"{preview['characters']} characters · {preview['pages_with_text']} page(s) with usable text."
            )

            def evaluate(current_preview: dict[str, Any]) -> dict[str, Any]:
                report(f"🧠 Stage 2: Sending PDF evidence for {file_path.name} to CrewAI/OpenRouter...")
                return asyncio.run(invoke_pdf_validation_graph({
                    "filename": file_path.name,
                    "pdf_text": current_preview["text"],
                    "pdf_metadata": current_preview["metadata"],
                    "extraction_error": current_preview["error"],
                    "extraction_quality": current_preview["extraction_quality"],
                    "pages_sampled": current_preview["pages_sampled"],
                    "page_count": current_preview["page_count"],
                    "source_metadata": {"source": source, "stage1_score": stage1_score},
                    "validation": {},
                }, report))["validation"]

            validation = evaluate(preview)
            if validation.get("needs_more_text") and len(preview["pages_sampled"]) < min(preview["page_count"], 10):
                report("📄 Stage 2 requested more evidence; sampling the next representative PDF pages once.")
                extra = extract_pdf_preview(
                    file_path,
                    start_page=len(preview["pages_sampled"]),
                    max_pages=10 - len(preview["pages_sampled"]),
                    max_characters=max(0, 12000 - preview["characters"]),
                )
                if not extra["error"]:
                    preview = {
                        **preview,
                        "text": (preview["text"] + "\n\n" + extra["text"]).strip()[:12000],
                        "pages_sampled": preview["pages_sampled"] + extra["pages_sampled"],
                        "pages_with_text": preview["pages_with_text"] + extra["pages_with_text"],
                        "characters": min(12000, preview["characters"] + extra["characters"]),
                        "toc_pages": preview["toc_pages"] + extra["toc_pages"],
                        "extraction_quality": "good" if preview["characters"] + extra["characters"] >= 2500 and preview["pages_with_text"] + extra["pages_with_text"] >= 2 else preview["extraction_quality"],
                    }
                    validation = evaluate(preview)

    original_size = file_path.stat().st_size if file_path.exists() else 0
    status = str(validation.get("status") or "PENDING").upper()
    if status not in {"APPROVED", "REJECTED", "PENDING", "PDF_INVALID"}:
        status = "PENDING"
    if status == "APPROVED" and (not validation.get("approved") or int(validation.get("score", 0)) < 70):
        status = "PENDING"
    if status == "REJECTED" and (validation.get("approved") or preview.get("extraction_quality") != "good"):
        status = "PENDING"
    stored_path = (
        store_validated_pdf(file_path, DATA_DIRECTORY, status == "APPROVED")
        if status in {"APPROVED", "REJECTED"} else file_path
    )
    validation_report = {
        "filename": file_path.name,
        "filepath": str(file_path),
        "stored_path": str(stored_path),
        "source": source,
        "stage1_score": stage1_score,
        "status": status,
        "stage2_score": validation.get("score", 0),
        "approved": status == "APPROVED",
        "confidence": validation.get("confidence", "unknown"),
        "needs_more_text": bool(validation.get("needs_more_text")),
        "categories": validation.get("categories", []),
        "reason": validation.get("reason", "Stage 2 returned no reason"),
        "technical_error_type": validation.get("technical_error_type"),
        "technical_error_message": validation.get("technical_error_message"),
        "stage2_raw_response": str(validation.get("raw_response") or "")[:4000],
        "stage2_normalized_result": {
            key: validation.get(key) for key in (
                "status", "approved", "score", "confidence", "needs_more_text", "categories", "reason",
            )
        },
        "pages_sampled": preview["pages_sampled"],
        "pages_with_text": preview["pages_with_text"],
        "preview_character_count": preview["characters"],
        "extraction_quality": preview["extraction_quality"],
        "page_count": preview["page_count"],
        "toc_pages": preview["toc_pages"],
        "pdf_metadata": preview["metadata"],
        "stage2_completed": status in {"APPROVED", "REJECTED", "PDF_INVALID"},
    }
    write_validation_report(file_path, DATA_DIRECTORY, validation_report)
    report(
        f"Stage 2 {status.lower()}: {file_path.name} (score {validation_report['stage2_score']}) → {stored_path.name}"
    )
    if source == "new_download":
        _update_download_state(
            file_path.name, "", "COMPLETED", size=original_size,
            reason=validation_report["reason"], completed=True, validation_status=status,
            technical_error_type=validation.get("technical_error_type"),
            technical_error_message=validation.get("technical_error_message"),
        )
    return validation_report


def _run_validation_job(job: dict[str, Any], worker_id: int) -> None:
    """Execute an existing Stage 2 validation after bounded queue admission."""
    filename = str(job["filename"])
    stage1_score = job.get("stage1_score", 100)
    event_candidate = {"run_id": str(job.get("run_id") or ""), "filename": filename}
    event_emitter = globals().get("_emit_pipeline_event")
    _, destination, _ = _download_paths(filename)
    if not destination.exists():
        return
    _update_download_state(
        filename, "", "COMPLETED", size=destination.stat().st_size,
        completed=True, validation_status="RUNNING",
    )
    if callable(event_emitter):
        event_emitter("STAGE2_STARTED", candidate=event_candidate, filename=filename,
                       message=f"Stage 2 worker {worker_id} started {filename}")
    completed = threading.Event()

    def mark_pending_if_slow() -> None:
        if not completed.is_set():
            _update_download_state(
                filename, "", "COMPLETED", size=_download_size(filename), completed=True,
                validation_status="PENDING", reason="validation exceeded timeout",
            )
            logging.getLogger(__name__).info("Validation timed out for %s; left pending", filename)

    timer = threading.Timer(VALIDATION_TIMEOUT_SECONDS, mark_pending_if_slow)
    timer.daemon = True
    timer.start()
    try:
        content_hasher = globals().get("_record_completed_content_hash")
        content_record = None
        if callable(content_hasher):
            content_record = content_hasher(filename)
        if isinstance(content_record, dict) and content_record.get("duplicate_of"):
            canonical_filename = str(content_record.get("canonical_filename") or content_record["duplicate_of"])
            canonical_status = str(content_record.get("canonical_validation_status") or "PENDING").upper()
            inherited_status = canonical_status if canonical_status in {"APPROVED", "REJECTED"} else "DUPLICATE_PENDING"
            _update_download_state(
                filename, "", "COMPLETED", size=_download_size(filename), completed=True,
                validation_status=inherited_status,
                reason=(
                    f"Byte-identical to {canonical_filename}; inherited canonical Stage 2 {canonical_status.lower()} result"
                    if inherited_status != "DUPLICATE_PENDING" else
                    f"Byte-identical to {canonical_filename}; waiting for its Stage 2 result"
                ),
            )
            logging.getLogger(__name__).info(
                "Skipped duplicate Stage 2 validation for %s; canonical content is %s", filename, canonical_filename,
            )
            if callable(event_emitter):
                event_emitter("DUPLICATE_DETECTED", candidate=event_candidate, filename=filename)
                event_emitter(
                    "STAGE2_DUPLICATE_PENDING" if inherited_status == "DUPLICATE_PENDING" else f"STAGE2_{inherited_status}",
                    candidate=event_candidate, filename=filename,
                    message=f"Duplicate content linked to {canonical_filename}",
                )
            return
        validation = _validate_pdf_stage2(destination, "new_download", stage1_score=stage1_score)
        propagator = globals().get("_propagate_duplicate_validation")
        if callable(propagator):
            propagator(filename, str(validation.get("status") or "PENDING"))
        if callable(event_emitter):
            validation_status = str(validation.get("status") or "PENDING").upper()
            event_kind = (
                "STAGE2_INVALID" if validation_status == "PDF_INVALID" else
                f"STAGE2_{validation_status}" if validation_status in {"APPROVED", "REJECTED"} else
                "STAGE2_PENDING"
            )
            event_emitter(
                event_kind,
                candidate=event_candidate, filename=filename,
            )
    except Exception as exc:
        _update_download_state(
            filename, "", "COMPLETED", size=_download_size(filename), completed=True,
            validation_status="PENDING", reason=f"validation error: {type(exc).__name__}",
        )
        logging.getLogger(__name__).warning("Validation pending for %s: %s", filename, type(exc).__name__)
    finally:
        completed.set()
        timer.cancel()


def _validation_queue() -> BoundedWorkQueue:
    """Create exactly one bounded Stage 2 pool per Python process."""
    global _VALIDATION_QUEUE
    with _VALIDATION_QUEUE_LOCK:
        if _VALIDATION_QUEUE is None:
            settings = NetworkManager._read_settings(PROJECT_ROOT / "config" / "settings.yaml")
            _VALIDATION_QUEUE = BoundedWorkQueue(
                _run_validation_job,
                max_workers=settings.validation_workers,
                maxsize=settings.stage2_queue_maxsize,
                thread_name_prefix="pdf-validation",
            )
        return _VALIDATION_QUEUE


def _stop_validation_queue(*, cancel_pending: bool = False, wait: bool = True) -> None:
    """Release Stage 2 workers when a Streamlit run/server is cancelled."""
    global _VALIDATION_QUEUE
    with _VALIDATION_QUEUE_LOCK:
        queue, _VALIDATION_QUEUE = _VALIDATION_QUEUE, None
    if queue is not None:
        queue.close(cancel_pending=cancel_pending, wait=wait)


def _schedule_validation(filename: str, *, stage1_score: int | None = 100) -> bool:
    """Release a completed PDF into the bounded Stage 2 queue, never blocking download workers."""
    _, destination, _ = _download_paths(filename)
    if not destination.exists():
        return False
    existing = _download_state_entry(filename) or {}
    validation_status = str(existing.get("validation_status") or "").upper()
    if validation_status in {"APPROVED", "REJECTED", "PDF_INVALID"}:
        return True
    integrity = check_pdf_integrity(destination)
    if not integrity["valid"]:
        reason = f"{integrity['error_type']}: {integrity['error_message']}"
        _update_download_state(
            filename, "", "COMPLETED", size=int(integrity.get("size") or 0), completed=True,
            validation_status="PDF_INVALID", reason=reason,
            technical_error_type=integrity["error_type"], technical_error_message=integrity["error_message"],
        )
        event_emitter = globals().get("_emit_pipeline_event")
        if callable(event_emitter):
            event_emitter(
                "STAGE2_INVALID", candidate={"run_id": str((existing.get("candidate") or {}).get("run_id") or "")},
                filename=filename, message=reason,
            )
        logging.getLogger(__name__).warning("PDF integrity gate rejected %s: %s", filename, integrity["error_type"])
        return False
    queue = _validation_queue()
    if queue.contains(filename):
        return True
    run_id = str((existing.get("candidate") or {}).get("run_id") or "")
    job = {"filename": filename, "stage1_score": stage1_score, "run_id": run_id, "_queue_key": filename}
    # Persist admission before handing the job to a fast worker, so RUNNING can
    # never be overwritten by a delayed QUEUED state update.
    _update_download_state(
        filename, "", "COMPLETED", size=destination.stat().st_size,
        completed=True, validation_status="QUEUED",
    )
    if queue.try_enqueue(job, key=filename):
        logging.getLogger(__name__).info("Validation queued for %s", filename)
        event_emitter = globals().get("_emit_pipeline_event")
        if callable(event_emitter):
            event_emitter("STAGE2_QUEUED", candidate=job, filename=filename)
        return True
    _update_download_state(
        filename, "", "COMPLETED", size=destination.stat().st_size,
        completed=True, validation_status="PENDING", reason="Stage 2 queue is at configured capacity",
    )
    logging.getLogger(__name__).info("Validation pending for %s: bounded queue is full", filename)
    event_emitter = globals().get("_emit_pipeline_event")
    if callable(event_emitter):
        event_emitter("STAGE2_PENDING", candidate=job, filename=filename, message="Stage 2 queue capacity reached")
    return False


def _restore_pending_validation_jobs() -> int:
    """Reconstruct only incomplete Stage 2 work after restart; never redownload."""
    scheduled = 0
    with _DOWNLOAD_STATE_LOCK:
        entries = [dict(entry) for entry in _load_download_state().get("downloads", {}).values() if isinstance(entry, dict)]
    for entry in entries:
        if str(entry.get("status", "")).upper() != "COMPLETED":
            continue
        if str(entry.get("validation_status") or "PENDING").upper() in {
            "APPROVED", "REJECTED", "RUNNING", "DUPLICATE_PENDING", "PDF_INVALID",
        }:
            continue
        filename = str(entry.get("filename") or "")
        if filename and _schedule_validation(filename, stage1_score=entry.get("candidate", {}).get("stage1_score", 100)):
            scheduled += 1
    return scheduled


def scan_and_validate_existing_pdfs(on_status, *, revalidate: bool = False) -> dict[str, int]:
    """Process unmanaged PDFs in data/ directly through Stage 2, never Stage 1."""
    candidates = scan_existing_pdfs(DATA_DIRECTORY, revalidate=revalidate)
    on_status(f"📂 Existing PDF Scan: Found {len(candidates)} PDF(s) requiring Stage 2 validation.")
    approved = rejected = pending = 0
    for index, candidate in enumerate(candidates, start=1):
        try:
            validation = _validate_pdf_stage2(
                Path(candidate["filepath"]), "existing_pdf", on_status=on_status, force=revalidate,
            )
            if validation["status"] == "APPROVED":
                approved += 1
            elif validation["status"] == "REJECTED":
                rejected += 1
            else:
                pending += 1
        except Exception as exc:
            pending += 1
            on_status(f"⚠️ Stage 2 could not validate {candidate['filename']}: {type(exc).__name__}")
        on_status(
            f"Stage 2 Validation progress: Processed {index} / {len(candidates)} · "
            f"Approved {approved} · Rejected {rejected} · Pending/Error {pending}"
        )
    return {"found": len(candidates), "processed": approved + rejected + pending, "approved": approved, "rejected": rejected, "pending": pending}


async def _download_bulk_cards(candidates: list[dict[str, Any]], debug) -> int:
    """Download jobs continuously while preserving Streamlit's clean file cards."""
    completed = 0
    for candidate in candidates:
        filename = candidate["filename"]
        try:
            if download_pdf(candidate["mirrors"], filename, defer_validation=True):
                completed += 1
                debug(f"Filename: {filename}\nDownloaded successfully")
            else:
                _update_download_state(
                    filename, candidate.get("source_url", ""),
                    "partial" if _download_size(filename) else "failed",
                    size=_download_size(filename), reason="all mirror candidates failed",
                )
                debug(f"Filename: {filename}\nFailure reason: all mirror candidates failed")
        except Exception:
            _update_download_state(
                filename, candidate.get("source_url", ""), "failed",
                size=_download_size(filename), reason="unexpected download error",
            )
            debug(f"Filename: {filename}\nFailure reason:\n{traceback.format_exc()}")
        # Yield to Streamlit's event loop between files without delaying the queue.
        await asyncio.sleep(0)
    return completed


def _recovery_candidates(
    query: str, *, max_document_attempts: int = 5, limit: int | None = None,
) -> list[dict[str, Any]]:
    """Rebuild interrupted approved jobs without repeating search or Stage 1."""
    normalized_query = query.strip().lower()
    recovered: list[dict[str, Any]] = []
    if limit is not None and limit <= 0:
        return recovered
    with _DOWNLOAD_STATE_LOCK:
        with _download_state_process_lock() as acquired:
            if not acquired:
                return recovered
            state = _load_download_state()
            changed = False
            entries = list(state["downloads"].items())
            # Resume real partial transfers first.  Records with no downloaded
            # bytes remain durable and eligible for a later run, but they must
            # not crowd out useful .part files when this run needs only a small
            # number of additional documents.
            def recovery_priority(item: tuple[str, Any]) -> tuple[int, int, int]:
                filename, entry = item
                if not isinstance(entry, dict):
                    return (2, 0, 0)
                _, _, part_path = _download_paths(filename)
                try:
                    part_size = part_path.stat().st_size
                except OSError:
                    part_size = 0
                return (
                    0 if part_size > 0 else 1,
                    -part_size,
                    -int(entry.get("updated_at", 0) or 0),
                )

            entries.sort(key=recovery_priority)
            for filename, entry in entries:
                if not isinstance(entry, dict):
                    continue
                status = str(entry.get("status", "")).upper()
                candidate = entry.get("candidate")
                if status not in {"QUEUED", "FAILED_FOR_ROUND", "RECOVERING", "PARTIAL", "FAILED"}:
                    if status != "DOWNLOADING":
                        continue
                if not isinstance(candidate, dict) or not candidate.get("mirrors"):
                    continue
                if entry.get("query", "").strip().lower() not in {"", normalized_query}:
                    continue
                _, destination, _ = _download_paths(filename)
                if destination.exists():
                    entry["status"] = "COMPLETED"
                    entry["completed"] = True
                    entry["size"] = destination.stat().st_size
                    changed = True
                    continue
                previous_attempt = max(1, int(entry.get("document_attempt", 0) or 0))
                if previous_attempt > max_document_attempts:
                    entry["status"] = "PERMANENTLY_FAILED"
                    entry["completed"] = False
                    entry["last_error"] = "persisted document attempt exceeds configured maximum"
                    changed = True
                    continue
                if status == "DOWNLOADING":
                    # An interrupted process did not finish this document-level
                    # attempt. Resume its .part with the same attempt number.
                    entry["status"] = "RECOVERING"
                    entry["last_error"] = "stale DOWNLOADING state resumed after restart"
                    next_attempt = previous_attempt
                    changed = True
                elif status in {"FAILED_FOR_ROUND", "PARTIAL", "FAILED"}:
                    if previous_attempt >= max_document_attempts:
                        entry["status"] = "PERMANENTLY_FAILED"
                        entry["completed"] = False
                        entry["last_error"] = "maximum document attempts reached before restart recovery"
                        changed = True
                        continue
                    next_attempt = previous_attempt + 1
                else:  # QUEUED or RECOVERING: no completed attempt to consume.
                    next_attempt = previous_attempt
                if limit is not None and len(recovered) >= limit:
                    continue
                job = dict(candidate)
                job["filename"] = filename
                job["_document_attempt"] = next_attempt
                recovered.append(job)
            if changed:
                _save_download_state(state)
    return recovered


def _download_worker(candidate: dict[str, Any], network: NetworkManager, worker_id: int) -> dict[str, Any]:
    """Perform one bounded document-level attempt; never render Streamlit here."""
    filename = candidate["filename"]
    document_attempt = int(candidate.get("_document_attempt", 1) or 1)
    max_attempts = network.settings.max_document_attempts
    work_kind = str(candidate.get("work_kind") or ("recovery" if candidate.get("_recovery_round") else "primary"))
    state_candidate = {key: value for key, value in candidate.items() if not key.startswith("_")}
    event_emitter = globals().get("_emit_pipeline_event")

    def emit_network(kind: str, message: str = "", *, level: str = "STATUS", state: str | None = None,
                     gateway_host: str | None = None, **extra: Any) -> None:
        network.emit(
            kind, worker_id, filename, message,
            level=level, category="download", run_id=str(candidate.get("run_id") or ""),
            document_id=str(candidate.get("document_id") or filename),
            document_title=str(candidate.get("title") or ""),
            source_page=candidate.get("source_page"), source_item=candidate.get("source_item"),
            work_kind=work_kind, document_attempt=document_attempt, max_attempts=max_attempts,
            state=state, gateway_host=gateway_host, short_message=extra.pop("short_message", message),
            **extra,
        )
    if not 1 <= document_attempt <= max_attempts:
        _update_download_state(
            filename, candidate.get("source_url", ""), "PERMANENTLY_FAILED", size=_download_size(filename),
            candidate=state_candidate,
            document_attempt=min(max(document_attempt, 1), max_attempts), completed=False,
            reason="download worker rejected out-of-range document attempt",
        )
        return {
            "success": False, "candidate": candidate,
            "attempt": min(max(document_attempt, 1), max_attempts), "permanent": True,
        }
    _update_download_state(
        filename, candidate.get("source_url", ""), "DOWNLOADING", size=_download_size(filename),
        candidate=state_candidate, document_attempt=document_attempt, request_attempt=0, completed=False,
    )
    if callable(event_emitter):
        event_emitter(
            "DOWNLOAD_STARTED", candidate=state_candidate, filename=filename,
            message=f"Download worker {worker_id} started {filename}", attempt=document_attempt,
            recovery=bool(candidate.get("_recovery_round")),
        )
    proxy = network.begin_download(worker_id, filename)

    def status(message: str) -> None:
        lowered = message.lower()
        # Keep this local helper dependency-free: scheduler unit tests load the
        # worker in isolation and a status update only needs a compact host.
        gateway_host = None
        for token in str(message).split():
            if token.startswith(("https://", "http://")):
                gateway_host = token.split("/", 3)[2].rstrip(".,;:)")
                break
        state = (
            "RETRYING" if "download recovery" in lowered or "resuming" in lowered else
            "STALLED" if "gateway unhealthy" in lowered or "stalled" in lowered else
            "SWITCHING_ROUTE" if "alternative gateway" in lowered or "switch gateway" in lowered else
            "DOWNLOADING"
        )
        # Raw transfer diagnostics remain available in the bounded debug view;
        # only meaningful state changes become normal activity events.
        emit_network(
            "debug", message, level="TRACE" if message.startswith("TRACE") else "DEBUG",
            state=state, gateway_host=gateway_host,
        )
        if state != "DOWNLOADING":
            emit_network(
                "worker_state", state=state, gateway_host=gateway_host,
                short_message=message.split("\n", 1)[0],
            )

    def progress(downloaded: int, total: int | None) -> None:
        _update_download_state(
            filename, candidate.get("source_url", ""), "DOWNLOADING", size=downloaded,
            candidate=state_candidate, document_attempt=document_attempt,
        )
        emit_network(
            "progress", level="DEBUG", state="MAKING_PROGRESS", downloaded=downloaded, total=total or 0,
            bytes_downloaded=downloaded, total_bytes=total or 0,
            short_message=f"Worker {worker_id} resumed/progressed to {downloaded / (1024 * 1024):.2f} MB",
        )

    emit_network(
        "worker_state", state="DOWNLOADING",
        short_message=(
            f"↓ Worker {worker_id} started {work_kind.title()} attempt {document_attempt}/{max_attempts}"
        ),
        bytes_downloaded=_download_size(filename),
    )
    status(
        f"⬇️ {work_kind.title()} download attempt {document_attempt}/{max_attempts}; "
        f"existing partial: {_download_size(filename) / (1024 * 1024):.2f} MB."
    )
    last_reason = "all valid gateways failed"
    attempt_context = {"visited_pages": set(), "attempted_download_routes": set(), "failed_routes": set()}
    try:
        for mirror_attempt, mirror in enumerate(candidate["mirrors"], start=1):
            mirror_host = str(mirror).split("/", 3)[2] if str(mirror).startswith(("https://", "http://")) else ""
            emit_network(
                "worker_state", state="SWITCHING_ROUTE", gateway_host=mirror_host,
                short_message=f"↻ Worker {worker_id} resolving route {mirror_attempt}/{len(candidate['mirrors'])}",
            )
            result = parse_mirror_and_download(
                mirror, filename, needs_page_check=False, on_status=status,
                on_progress=progress, _attempt_context=attempt_context,
                network=network, worker_id=worker_id, proxy=proxy,
            )
            if result is True:
                _update_download_state(
                    filename, candidate.get("source_url", ""), "COMPLETED", size=_download_size(filename),
                    candidate=state_candidate, document_attempt=document_attempt, completed=True,
                )
                scheduled = _schedule_validation(filename, stage1_score=candidate.get("stage1_score", 100))
                validation_status = str((_download_state_entry(filename) or {}).get("validation_status") or "").upper()
                validation_message = (
                    "Downloaded successfully; PDF integrity gate marked the file technically invalid; Stage 2 was not queued."
                    if validation_status == "PDF_INVALID" else
                    "Downloaded successfully; Stage 2 PDF validation queued."
                    if scheduled else "Downloaded successfully; Stage 2 validation is pending queue capacity."
                )
                emit_network("success", validation_message, level="LIFECYCLE", state="COMPLETED",
                             short_message=f"✓ Worker {worker_id} completed PDF; Stage 2 {'queued' if scheduled else 'pending'}")
                if callable(event_emitter):
                    event_emitter("DOWNLOAD_COMPLETED", candidate=state_candidate, filename=filename)
                return {"success": True, "candidate": candidate, "attempt": document_attempt}
            if result == "retry_next":
                last_reason = f"source {mirror_attempt} did not complete within its request retry budget"
                state_entry = _download_state_entry(filename) or {}
                if str(state_entry.get("last_error", "")).startswith("HTTP 429"):
                    # Do not use another route to evade a source's explicit rate
                    # limit.  The scheduler will wait through Retry-After.
                    last_reason = str(state_entry.get("last_error"))
                    break
                network.cooldown(mirror_attempt)
    except Exception as exc:
        last_reason = f"{type(exc).__name__}: {exc}"

    final_status = "PERMANENTLY_FAILED" if document_attempt >= max_attempts else "FAILED_FOR_ROUND"
    _update_download_state(
        filename, candidate.get("source_url", ""), final_status, size=_download_size(filename),
        reason=last_reason, candidate=state_candidate, document_attempt=document_attempt, completed=False,
    )
    action = "permanent failure limit reached" if final_status == "PERMANENTLY_FAILED" else "scheduled for next recovery round"
    terminal_state = "PERMANENTLY_FAILED" if final_status == "PERMANENTLY_FAILED" else "RECOVERY_WAIT"
    emit_network(
        "failure",
        f"⚠️ Download attempt ended\nDocument: {filename}\nAttempt: {document_attempt}/{max_attempts}\n"
        f"Downloaded: {_download_size(filename) / (1024 * 1024):.2f} MB\nReason: {last_reason}\nAction: {action}",
        level="LIFECYCLE", state=terminal_state,
        short_message=(
            f"✕ Worker {worker_id} final failure · attempt {document_attempt}/{max_attempts} exhausted"
            if final_status == "PERMANENTLY_FAILED" else
            f"⚠ Worker {worker_id} stalled; moved to recovery after attempt {document_attempt}/{max_attempts}"
        ),
        bytes_downloaded=_download_size(filename),
    )
    if callable(event_emitter):
        event_emitter(
            "DOWNLOAD_PERMANENTLY_FAILED" if final_status == "PERMANENTLY_FAILED" else "DOWNLOAD_FAILED_FOR_ROUND",
            candidate=state_candidate, filename=filename, message=last_reason, attempt=document_attempt,
        )
    state_entry = _download_state_entry(filename) or {}
    return {
        "success": False, "candidate": candidate, "attempt": document_attempt,
        "permanent": final_status == "PERMANENTLY_FAILED",
        "retry_after_until": float(state_entry.get("retry_after_until", 0) or 0),
    }


async def _download_round(
    candidates: list[dict[str, Any]], network: NetworkManager, debug, *,
    executor: ThreadPoolExecutor | None = None, round_number: int = 1, recovery: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    """Perform exactly one document-level attempt for every supplied job.

    Request-level retries remain in ``download_file``.  Global orchestration owns
    when failed jobs re-enter a later recovery round, so a page never consumes
    its own recovery cycle before subsequent pages are discovered.
    """
    if not candidates:
        return 0, []
    unique_candidates: list[dict[str, Any]] = []
    seen_filenames: set[str] = set()
    for original in candidates:
        candidate = dict(original)
        filename = candidate["filename"]
        attempt = int(candidate.get("_document_attempt", 1) or 1)
        if filename in seen_filenames:
            continue
        seen_filenames.add(filename)
        if not 1 <= attempt <= network.settings.max_document_attempts:
            _update_download_state(
                filename, candidate.get("source_url", ""), "PERMANENTLY_FAILED",
                size=_download_size(filename), candidate={key: value for key, value in candidate.items() if not key.startswith("_")},
                document_attempt=min(max(attempt, 1), network.settings.max_document_attempts), completed=False,
                reason="scheduler rejected out-of-range document attempt",
            )
            debug(f"Filename: {filename}\nFailure reason: out-of-range document attempt {attempt}/{network.settings.max_document_attempts}")
            continue
        candidate["_document_attempt"] = attempt
        unique_candidates.append(candidate)
    if not unique_candidates:
        return 0, []

    cards = {}
    for candidate in unique_candidates:
        filename = candidate["filename"]
        with st.status(f"⬇️ Processing: {filename[:40]}...", expanded=False) as card:
            card.write("⏳ Queued for a controlled curl_cffi worker...")
            cards[filename] = (card, st.empty())
        _update_download_state(
            filename, candidate.get("source_url", ""), "RECOVERING" if recovery else "QUEUED",
            size=_download_size(filename), candidate={key: value for key, value in candidate.items() if not key.startswith("_")},
            document_attempt=int(candidate["_document_attempt"]), completed=False,
        )

    st.caption(
        f"{'Recovery' if recovery else 'Primary'} Download Round: {round_number} · "
        f"Configured Workers: {network.settings.max_workers} · "
        f"Active Workers: {min(network.settings.max_workers, len(unique_candidates))} · "
        f"Jobs: {len(unique_candidates)}"
    )
    completed, failed_for_round = 0, []
    executor_context = nullcontext(executor) if executor else ThreadPoolExecutor(
        max_workers=network.settings.max_workers, thread_name_prefix="pdf-download",
    )
    with executor_context as active_executor:
        pending = {
            active_executor.submit(_download_worker, candidate, network, (index % network.settings.max_workers) + 1): candidate
            for index, candidate in enumerate(unique_candidates)
        }
        while pending:
            while True:
                try:
                    event = network.events.get_nowait()
                except Empty:
                    break
                if event["filename"] not in cards:
                    continue
                card, progress = cards[event["filename"]]
                if event["kind"] == "progress":
                    total, transferred = event["total"], event["downloaded"]
                    fraction = min(transferred / total, 1.0) if total else 0.0
                    progress.progress(fraction, text=f"Worker {event['worker']}/{network.settings.max_workers}: {transferred / (1024 * 1024):.2f} MB" + (f" / {total / (1024 * 1024):.2f} MB" if total else ""))
                else:
                    card.write(event["message"])
            done, _ = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
            for future in done:
                candidate = pending.pop(future)
                filename = candidate["filename"]
                card, progress = cards[filename]
                progress.empty()
                try:
                    result = future.result()
                except Exception:
                    result = {"success": False, "candidate": candidate, "attempt": int(candidate["_document_attempt"]), "permanent": False}
                    debug(f"Filename: {filename}\nFailure reason:\n{traceback.format_exc()}")
                if result["success"]:
                    completed += 1
                    card.update(label=f"✅ Secured: {filename[:40]}", state="complete", expanded=False)
                    debug(f"Filename: {filename}\nDownloaded successfully")
                elif result.get("permanent"):
                    card.update(label=f"❌ Failed: {filename[:40]}", state="error", expanded=False)
                else:
                    failed_for_round.append(result)
                    card.update(label=f"⚠️ Pending recovery: {filename[:40]}", state="running", expanded=False)
            await asyncio.sleep(0)
    return completed, failed_for_round


class _StreamlitDownloadCoordinator:
    """Main-thread Streamlit adapter around the bounded global download queue.

    Worker threads only call ``_download_worker``. This adapter turns worker
    outcomes into recovery candidates and asks one dashboard renderer to refresh.
    """

    def __init__(self, network: NetworkManager, debug, *, on_drain=None, dashboard_renderer=None) -> None:
        self.network = network
        self.debug = debug
        self.on_drain = on_drain
        self.dashboard_renderer = dashboard_renderer
        self.presentation = DownloadPresentation(max_workers=network.settings.max_workers)
        self.recovery_jobs: dict[str, dict[str, Any]] = {}
        self.completed = 0
        self.queue = GlobalDownloadQueue(
            lambda candidate, worker_id: _download_worker(candidate, network, worker_id),
            max_workers=network.settings.max_workers,
            maxsize=network.settings.download_queue_maxsize,
        )

    def _card(self, candidate: dict[str, Any], *, recovery: bool) -> None:
        provenance = _candidate_provenance_label(candidate)
        if recovery:
            self.recovery_jobs[candidate["filename"]] = dict(candidate)
            self.presentation.add_activity(f"♻ {provenance} queued for recovery")
        else:
            self.presentation.add_activity(f"↓ {provenance} queued for a download worker")
        self._render_panels()

    @staticmethod
    def _worker_row(row: dict[str, Any]) -> str:
        worker = int(row.get("worker", 0))
        state = str(row.get("state") or "IDLE")
        if state == "IDLE":
            return f"W{worker} | Idle"
        source_page, source_item = row.get("source_page"), row.get("source_item")
        source = f"page {source_page} · item {source_item}" if source_page and source_item else "source unavailable"
        downloaded = float(row.get("bytes_downloaded", 0) or 0) / (1024 * 1024)
        total = float(row.get("total_bytes", 0) or 0) / (1024 * 1024)
        progress = f"{downloaded:.2f} MB" + (f" / {total:.2f} MB" if total else "")
        if total:
            percent = max(0, min(100, round(downloaded / total * 100)))
            filled = round(percent / 10)
            progress += f" [{'█' * filled}{'░' * (10 - filled)}] {percent}%"
        attempt = f"{row.get('document_attempt', '?')}/{row.get('max_attempts', '?')}"
        route = str(row.get("gateway_host") or "resolving")
        title = str(row.get("filename") or "").replace("\n", " ")[:72]
        last_event = str(row.get("last_event") or "").replace("\n", " · ")[:120]
        return (
            f"W{worker} | {str(row.get('work_kind', 'primary')).title()} | {state} | {progress} | "
            f"{source} | attempt {attempt} | {route} | {title}"
            + (f"\n   Last event: {last_event}" if last_event else "")
        )

    def _render_panels(self) -> None:
        """Ask the single main-thread dashboard to refresh; never render here."""
        if callable(self.dashboard_renderer):
            self.dashboard_renderer()

    def set_recovery_backlog(self, candidates: list[dict[str, Any]]) -> None:
        self.recovery_jobs = {str(job.get("filename")): dict(job) for job in candidates if job.get("filename")}
        self._render_panels()

    async def enqueue(self, candidate: dict[str, Any], *, recovery: bool = False) -> None:
        """Apply backpressure without blocking the search producer's event loop."""
        candidate["work_kind"] = "recovery" if recovery else str(candidate.get("work_kind") or "primary")
        if recovery:
            candidate["_recovery_round"] = True
        # Persist and render before admission: a fast worker must never have its
        # DOWNLOADING transition overwritten by a late QUEUED transition.
        self._card(candidate, recovery=recovery)
        _update_download_state(
            candidate["filename"], candidate.get("source_url", ""),
            "RECOVERING" if recovery else "QUEUED",
            size=_download_size(candidate["filename"]),
            candidate={key: value for key, value in candidate.items() if not key.startswith("_")},
            document_attempt=int(candidate.get("_document_attempt", 1) or 1), completed=False,
        )
        while not self.queue.try_enqueue(candidate):
            self.drain()
            await asyncio.sleep(0.1)

    def drain(self) -> list[dict[str, Any]]:
        """Render worker events and collect failures for a later recovery round."""
        while True:
            try:
                event = self.network.events.get_nowait()
            except Empty:
                break
            self.presentation.consume(event)

        failed: list[dict[str, Any]] = []
        for outcome in self.queue.drain_outcomes():
            candidate, result = outcome.candidate, outcome.result
            filename = candidate["filename"]
            if result.get("success"):
                self.completed += 1
                self.recovery_jobs.pop(filename, None)
                self.presentation.add_activity(f"✓ Download completed · {_candidate_provenance_label(candidate)} · {filename[:60]}")
                self.debug(f"Filename: {filename}\nDownloaded successfully")
            elif result.get("permanent"):
                self.recovery_jobs.pop(filename, None)
                self.presentation.add_activity(
                    f"✕ Final failure · {_candidate_provenance_label(candidate)} · attempt "
                    f"{result.get('attempt', '?')}/{self.network.settings.max_document_attempts} exhausted"
                )
            else:
                if result.get("exception"):
                    self.debug(f"Filename: {filename}\nFailure reason:\n{result['exception']!r}")
                self.presentation.add_activity(f"⚠ Download stalled; moved to recovery · {_candidate_provenance_label(candidate)}")
                failed.append(result)
        self.presentation.mark_stalled(self.network.settings.max_stall_seconds)
        self._render_panels()
        # This is called from the Streamlit coroutine, never from a worker.
        # Rendering aggregated worker lifecycle events here keeps active queue
        # drains observable without permitting worker threads to call st.*.
        if callable(self.on_drain):
            self.on_drain()
        return failed

    async def drain_until_idle(self) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        while not self.queue.idle:
            failures.extend(self.drain())
            await asyncio.sleep(0.1)
        failures.extend(self.drain())
        return failures

    def close(self, *, cancel_pending: bool = False, wait: bool = True) -> None:
        self.drain()
        self.queue.close(cancel_pending=cancel_pending, wait=wait)

    def __del__(self) -> None:
        # Streamlit cancels the script coroutine on rerun/server shutdown.
        # If that happens before the normal close at the end of the pipeline,
        # discard only not-yet-started in-memory jobs so executor threads do not
        # keep the Python process alive processing a stale queue after Ctrl+C.
        try:
            self.queue.close(cancel_pending=True, wait=False)
        except Exception:
            pass


class _DashboardStatusSink:
    """Compatibility sink for existing discovery messages outside normal UI."""

    def __init__(self, debug) -> None:
        self._debug = debug

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def write(self, message: Any) -> None:
        self._debug(str(message))

    def update(self, **fields: Any) -> None:
        label = fields.get("label")
        if label:
            self._debug(f"Pipeline status: {label}")


def _search_cache_path(search_url: str) -> Path:
    """Return a stable cache file without putting query text in a filename."""
    digest = hashlib.sha256(search_url.encode("utf-8")).hexdigest()
    return SEARCH_CACHE_DIRECTORY / f"{digest}.html"


def _usable_search_html(html: str | None) -> bool:
    """Reject blank pages and common block/error responses before evaluation."""
    if not isinstance(html, str) or len(html.strip()) < 200:
        return False
    lowered = html.lower()
    return not any(marker in lowered for marker in (
        "access denied", "captcha", "temporarily unavailable", "error 502", "error 503",
    ))


def _safe_failure_detail(response_text: str, limit: int = 300) -> str:
    """Make an API failure observable without ever displaying credentials."""
    compact = re.sub(r"\s+", " ", response_text or "").strip()
    compact = re.sub(r"(?i)(apikey=)[^&\s]+", r"\1[redacted]", compact)
    return compact[:limit] or "no response body"


async def _fetch_zenrows_search(search_url: str, api_key: str, report) -> str | None:
    """Fetch through ZenRows and report the actionable response reason."""
    params = {
        "apikey": api_key,
        "url": search_url,
        "premium_proxy": "true",
        "custom_headers": "true",
    }
    headers = {**BROWSER_HEADERS, "Referer": "https://www.google.com/"}
    for attempt in range(1, SEARCH_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=SEARCH_TIMEOUT_SECONDS, headers=headers) as client:
                response = await client.get("https://api.zenrows.com/v1/", params=params)
            if response.status_code == 200 and _usable_search_html(response.text):
                report(f"ZenRows succeeded (HTTP 200, attempt {attempt}).")
                return response.text
            reason = _safe_failure_detail(response.text)
            report(f"ZenRows HTTP {response.status_code} on attempt {attempt}: {reason}")
            # 4xx responses are request/account/target errors, not transient network errors.
            if 400 <= response.status_code < 500:
                break
        except Exception as exc:
            report(f"ZenRows transport failure on attempt {attempt}: {type(exc).__name__}: {_safe_failure_detail(str(exc), 160)}")
        if attempt < SEARCH_RETRIES:
            await asyncio.sleep(attempt)
    return None


async def _fetch_direct_search(search_url: str, report, *, doh_url: str | None = None) -> str | None:
    """Use curl_cffi browser impersonation; optional DoH is a separate fallback."""
    label = "direct DoH" if doh_url else "direct browser"
    session_options = {"impersonate": "chrome120", "verify": True}
    if doh_url:
        session_options["doh_url"] = doh_url
    for attempt in range(1, SEARCH_RETRIES + 1):
        try:
            async with c_requests.AsyncSession(**session_options) as session:
                response = await session.get(search_url, headers=BROWSER_HEADERS, timeout=SEARCH_TIMEOUT_SECONDS)
            if response.status_code == 200 and _usable_search_html(response.text):
                report(f"{label} succeeded (HTTP 200, attempt {attempt}).")
                return response.text
            detail = "empty or blocked HTML" if response.status_code == 200 else f"HTTP {response.status_code}"
            report(f"{label} failed on attempt {attempt}: {detail}.")
            if 400 <= response.status_code < 500:
                break
        except Exception as exc:
            report(f"{label} transport failure on attempt {attempt}: {type(exc).__name__}: {_safe_failure_detail(str(exc), 160)}")
        if attempt < SEARCH_RETRIES:
            await asyncio.sleep(attempt)
    return None


def _read_cached_search(search_url: str, report) -> str | None:
    cache_path = _search_cache_path(search_url)
    try:
        if cache_path.exists():
            html = cache_path.read_text(encoding="utf-8")
            if _usable_search_html(html):
                report("Local search cache succeeded.")
                return html
            report("Local search cache was invalid and was ignored.")
    except OSError as exc:
        report(f"Local search cache could not be read: {type(exc).__name__}.")
    return None


def _write_search_cache(search_url: str, html: str, report) -> None:
    try:
        SEARCH_CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
        _search_cache_path(search_url).write_text(html, encoding="utf-8")
    except OSError as exc:
        report(f"Search cache write skipped: {type(exc).__name__}.")

async def run_scraping_pipeline(
    query: str, max_pages: int, mode: str = "research", target_documents: int = 500,
) -> dict[str, int]:
    """Present search, evaluation, and file stages as cascading status cards."""
    zenrows_api_key = os.getenv("ZENROWS_API_KEY")
    if not query.strip():
        raise ValueError("Search query cannot be empty")
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")
    if target_documents < 1:
        raise ValueError("target_documents must be at least 1")

    pages_processed = 0
    documents_approved = 0
    documents_downloaded = _completed_download_count()
    # A run identifier lets worker threads publish only data while this main
    # Streamlit invocation decides what to render.  It also prevents a later
    # rerun from consuming another run's still-pending events.
    run_id = f"run-{time.time_ns():x}"
    live_metrics = RunMetrics(existing_downloads=documents_downloaded)
    if hasattr(st, "session_state"):
        st.session_state["_download_debug"] = []
    failed_urls: list[str] = []
    debug_events: deque[str] = deque(maxlen=500)
    network = NetworkManager(PROJECT_ROOT)
    restored_stage2_jobs = _restore_pending_validation_jobs()
    scheduled_filenames: set[str] = set()
    scheduled_document_ids: set[str] = set()
    primary_jobs_queued = 0
    primary_failed_results: list[dict[str, Any]] = []
    queue_completions_seen = 0
    recovery_backlog = GlobalRecoveryBacklog()
    restored_recovery_jobs = _recovery_candidates(
        query,
        max_document_attempts=network.settings.max_document_attempts,
        limit=max(0, target_documents - documents_downloaded),
    )
    for job in restored_recovery_jobs:
        job.setdefault("run_id", run_id)
        recovery_backlog.add_restored(job)
    scheduled_filenames.update(job["filename"] for job in restored_recovery_jobs)
    scheduled_document_ids.update(
        str(job.get("document_id") or stable_document_id(job))
        for job in restored_recovery_jobs
    )

    def debug(message: str) -> None:
        debug_events.append(message)

    # Agent Activity is a view over structured events; it never dictates work
    # scheduling.  There is exactly one dashboard and no discovery transcript.
    pipeline_area = st.container()
    downloads_area = st.container()
    with downloads_area:
        st.markdown("#### Pipeline overview")
        overview_placeholders = [column.empty() for column in st.columns(5)]
        overview_footer = st.empty()
        st.markdown("#### Current search page")
        current_page_placeholder = st.empty()
        with st.expander("Search page details", expanded=False):
            page_details_placeholder = st.empty()
        st.markdown("#### Active downloads")
        worker_placeholder = st.empty()
        st.markdown("#### Stage 2")
        stage2_placeholder = st.empty()
        with st.expander("Recent pipeline activity", expanded=True):
            activity_placeholder = st.empty()
        with st.expander("Recovery details", expanded=False):
            recovery_placeholder = st.empty()
        with st.expander("Show detailed network/debug log", expanded=False):
            network_debug_placeholder = st.empty()
    current_page_state: dict[str, int] = {
        "page": 1, "found": 0, "approved": 0, "rejected": 0,
        "queued": 0, "stored": 0, "recovery": 0, "missing": 0,
    }
    page_details: list[dict[str, Any]] = []

    def human_document_name(value: dict[str, Any] | str) -> str:
        if isinstance(value, dict):
            title = str(value.get("document_title") or value.get("title") or "").strip()
            if title:
                return title[:90]
            value = str(value.get("filename") or "")
        name = str(value).rsplit("/", 1)[-1].removesuffix(".pdf")
        name = re.sub(r"^page_\d{3}_\d{3}_", "", name).replace("_", " ").strip()
        return name[:90] or "Unknown document"

    def render_table(placeholder, rows: list[dict[str, Any]], empty_message: str) -> None:
        if rows:
            dataframe = getattr(placeholder, "dataframe", None)
            if callable(dataframe):
                dataframe(rows, use_container_width=True, hide_index=True)
            else:  # Lightweight test doubles and older Streamlit adapters.
                placeholder.write(rows)
        else:
            placeholder.caption(empty_message)

    def dashboard_snapshot() -> PipelineDashboardSnapshot:
        return PipelineDashboardSnapshot.from_sources(
            metrics=live_metrics,
            presentation=coordinator.presentation,
            queue_active=coordinator.queue.active,
            queue_pending=coordinator.queue.pending,
            recovery_jobs=list(coordinator.recovery_jobs.values()),
            discovery_page=current_page_state["page"], max_pages=max_pages,
            target_documents=target_documents,
            max_document_attempts=network.settings.max_document_attempts,
        )

    def render_dashboard() -> None:
        """The sole Streamlit rendering path for live pipeline state."""
        snapshot = dashboard_snapshot()
        metric_values = (
            ("Discovery", f"Page {snapshot.discovery_page} / {snapshot.max_pages}", f"{snapshot.discovered} found"),
            ("Stage 1", f"{snapshot.stage1_approved} approved", f"{snapshot.stage1_rejected} rejected"),
            ("Downloads", f"{snapshot.downloaded} / {snapshot.target_documents}", f"{snapshot.queue_active} active · {snapshot.queue_pending} queued"),
            ("Recovery", f"{snapshot.recovery_backlog} waiting", f"{snapshot.recovery_near_limit} near limit · {snapshot.permanently_failed} final"),
            ("Stage 2", f"{snapshot.stage2_active} / {network.settings.validation_workers} active", f"{snapshot.stage2_queued} queued · {snapshot.stage2_pending} pending"),
        )
        for placeholder, (label, value, delta) in zip(overview_placeholders, metric_values):
            placeholder.metric(label, value, delta=delta)
        overview_footer.caption(f"LangSmith: {langsmith_status()} · Recovery active: {snapshot.recovery_active} · Invalid PDFs: {snapshot.stage2_invalid}")

        with current_page_placeholder.container():
            st.caption("CURRENT SEARCH PAGE")
            page_metrics = (
                ("Page", f"{current_page_state['page']} / {max_pages}"),
                ("Found", current_page_state["found"]),
                ("Approved", current_page_state["approved"]),
                ("Rejected", current_page_state["rejected"]),
                ("New downloads", current_page_state["queued"]),
                ("Stored", current_page_state["stored"]),
                ("Recovery", current_page_state["recovery"]),
                ("Missing", current_page_state["missing"]),
            )
            for column, (label, value) in zip(st.columns(8), page_metrics):
                column.metric(label, value)

        worker_rows = []
        for row in snapshot.worker_rows:
            state = str(row.get("state") or "IDLE")
            downloaded = float(row.get("bytes_downloaded", 0) or 0) / (1024 * 1024)
            total = float(row.get("total_bytes", 0) or 0) / (1024 * 1024)
            progress = "—" if state == "IDLE" else f"{downloaded:.2f} MB" + (f" / {total:.2f} MB" if total else "")
            if total:
                progress += f" ({min(100, round(downloaded / total * 100))}%)"
            source = "—"
            if row.get("source_page") and row.get("source_item"):
                source = f"P{row['source_page']} · #{row['source_item']}"
            worker_rows.append({
                "Worker": f"W{row.get('worker')}", "Mode": str(row.get("work_kind") or "—").title() if state != "IDLE" else "—",
                "State": state, "Progress": progress, "Source": source,
                "Document": human_document_name(row) if state != "IDLE" else "—",
                "Last event": str(row.get("last_event") or "—")[:140],
            })
        render_table(worker_placeholder, worker_rows, "No worker telemetry yet.")

        with stage2_placeholder.container():
            stage_metrics = (
                ("Workers active", f"{snapshot.stage2_active} / {network.settings.validation_workers}"),
                ("Queued", snapshot.stage2_queued), ("Approved", snapshot.stage2_approved),
                ("Rejected", snapshot.stage2_rejected), ("Pending/Error", snapshot.stage2_pending),
                ("Invalid", snapshot.stage2_invalid),
            )
            for column, (label, value) in zip(st.columns(6), stage_metrics):
                column.metric(label, value)
            active_stage2 = [
                {"Document": human_document_name(filename), "State": state}
                for filename, state in live_metrics.stage2_rows() if state == "running"
            ][:network.settings.validation_workers]
            if active_stage2:
                render_table(st.empty(), active_stage2, "")

        activity_rows = []
        for timestamp, message in list(zip(coordinator.presentation.activity_times, coordinator.presentation.activity))[-30:]:
            activity_rows.append(f"{time.strftime('%H:%M:%S', time.localtime(timestamp))}  {message}")
        activity_placeholder.markdown("  \n".join(activity_rows) or "No lifecycle activity yet.")

        # The recovery backlog can contain hundreds of durable records.  Read
        # downloads.json once per dashboard refresh instead of once per row,
        # and bound the collapsed table so UI rendering cannot starve the
        # Streamlit event loop.  The scheduler still retains every job.
        with _DOWNLOAD_STATE_LOCK:
            recovery_state = dict(_load_download_state().get("downloads", {}))
        recovery_rows = []
        visible_recovery_jobs = list(coordinator.recovery_jobs.values())[:50]
        for candidate in visible_recovery_jobs:
            filename = str(candidate.get("filename") or "")
            entry = recovery_state.get(filename)
            entry = entry if isinstance(entry, dict) else {}
            partial_mb = float(entry.get("size", _download_size(str(candidate.get("filename") or ""))) or 0) / (1024 * 1024)
            recovery_rows.append({
                "Document": human_document_name(candidate),
                "Source": f"P{candidate.get('source_page')} · #{candidate.get('source_item')}" if candidate.get("source_page") else "—",
                "Attempt": f"{candidate.get('_document_attempt', 1)}/{network.settings.max_document_attempts}",
                "Partial MB": f"{partial_mb:.2f}", "Last failure": str(entry.get("last_error") or "Waiting")[:140],
                "State": str(entry.get("status") or "RECOVERY_WAIT"),
            })
        if len(coordinator.recovery_jobs) > len(visible_recovery_jobs):
            recovery_rows.append({
                "Document": f"… {len(coordinator.recovery_jobs) - len(visible_recovery_jobs)} more queued records",
                "Source": "—", "Attempt": "—", "Partial MB": "—",
                "Last failure": "Full count remains visible in Pipeline overview", "State": "QUEUED",
            })
        render_table(recovery_placeholder, recovery_rows, "No documents in the recovery backlog.")

        detail_events: deque[str] = deque(maxlen=1000)
        detail_events.extend(debug_events)
        detail_events.extend(coordinator.presentation.debug)
        if detail_events:
            network_debug_placeholder.code("\n\n".join(detail_events), language=None)
        else:
            network_debug_placeholder.caption(PipelineDashboardSnapshot.empty_debug_message())
        render_table(page_details_placeholder, page_details, "No processed search-page decisions yet.")

    def refresh_live_metrics() -> None:
        """Consume worker events from the Streamlit thread only."""
        for event in _PIPELINE_EVENTS.drain(run_id):
            live_metrics.consume(event)
            kind = str(event.get("kind") or "")
            count = int(event.get("count", 1) or 1)
            filename = str(event.get("filename") or "")
            lifecycle_message = {
                "DOCUMENT_DISCOVERED": f"🔎 Discovery: found {count} document(s)",
                "STAGE1_STARTED": f"🧠 Stage 1 evaluating {count} document(s)",
                "STAGE1_APPROVED": f"✓ Stage 1 approved {count} document(s)",
                "STAGE1_REJECTED": f"− Stage 1 rejected {count} document(s)",
                "STAGE1_FAILED": f"⚠ Stage 1 evaluation failed for {count} document(s)",
                "STAGE2_STARTED": f"📄 Stage 2 started {filename[:70]}",
                "STAGE2_APPROVED": f"✓ Stage 2 approved {filename[:70]}",
                "STAGE2_REJECTED": f"− Stage 2 rejected {filename[:70]}",
                "STAGE2_PENDING": f"⚠ Stage 2 pending capacity for {filename[:70]}",
                "STAGE2_INVALID": f"⚠ Technical PDF validation failed for {filename[:70]}",
            }.get(kind)
            if lifecycle_message:
                coordinator.presentation.add_activity(lifecycle_message, timestamp=float(event.get("timestamp", time.time()) or time.time()))
        render_dashboard()

    coordinator = _StreamlitDownloadCoordinator(
        network, debug, on_drain=refresh_live_metrics,
    )
    coordinator.dashboard_renderer = render_dashboard
    coordinator.set_recovery_backlog(restored_recovery_jobs)
    pipeline_task = asyncio.current_task()
    if pipeline_task is not None:
        def release_workers_after_aborted_run(task) -> None:
            """Do not drain queued work after a cancelled/failed script run."""
            aborted = task.cancelled()
            if not aborted:
                try:
                    aborted = task.exception() is not None
                except (asyncio.CancelledError, RuntimeError):
                    aborted = True
            if aborted:
                coordinator.queue.close(cancel_pending=True, wait=False)
                _stop_validation_queue(cancel_pending=True, wait=False)
                network.close()

        pipeline_task.add_done_callback(release_workers_after_aborted_run)

    def collect_primary_outcomes() -> None:
        """Keep discovery productive while workers finish in the background."""
        nonlocal documents_downloaded, queue_completions_seen
        primary_failed_results.extend(coordinator.drain())
        new_completions = coordinator.completed - queue_completions_seen
        if new_completions:
            documents_downloaded += new_completions
            queue_completions_seen = coordinator.completed
        refresh_live_metrics()
    with pipeline_area:
        with _DashboardStatusSink(debug) as main_status:
            log = main_status.write
            refresh_live_metrics()
            if recovery_backlog:
                log(
                    f"♻️ Restored {len(recovery_backlog)} interrupted download(s); "
                    "they are queued for the global recovery round after new primary work."
                )
            for page_number in range(1, max_pages + 1):
                collect_primary_outcomes()
                queued_primary_remaining = max(primary_jobs_queued - coordinator.completed, 0)
                if documents_downloaded + queued_primary_remaining >= target_documents:
                    debug(
                        f"Target of {target_documents} is satisfied by completed files and the global primary queue; "
                        "stopping new page collection without discarding recovery backlog."
                    )
                    break
                current_page_state.update({
                    "page": page_number, "found": 0, "approved": 0, "rejected": 0,
                    "queued": 0, "stored": 0, "recovery": 0, "missing": 0,
                })
                coordinator.presentation.add_activity(f"🔎 Discovery started page {page_number} / {max_pages}")
                render_dashboard()
                main_status.update(
                    label=f"🔎 Discovery: Search page {page_number} / {max_pages}",
                    state="running", expanded=True,
                )
                log(f"——— Page {page_number} of {max_pages} ———")
                if page_number > 1:
                    await asyncio.sleep(random.uniform(9.0, 15.0))
                encoded_query = quote(query.strip(), safe="")
                log(
                    "🌐 ZenRows API: Bypassing anti-bot protection and fetching search results..."
                    if zenrows_api_key else
                    "🌐 curl_cffi: Fetching search results via the direct browser fallback (ZenRows API key not configured)..."
                )
                raw_html = None
                search_url = (
                    f"https://libgen.bz/index.php?req={encoded_query}"
                    "&columns[]=t&columns[]=a&columns[]=s&columns[]=y&columns[]=p&columns[]=i"
                    "&objects[]=f&objects[]=e&objects[]=s&objects[]=a&objects[]=p&objects[]=w"
                    "&topics[]=l&topics[]=c&topics[]=f&topics[]=a&topics[]=m&topics[]=r&topics[]=s"
                    f"&res=25&filesuns=all&page={page_number}"
                )
                if zenrows_api_key:
                    debug(f"Page {page_number}: trying ZenRows for {search_url}")
                    raw_html = await _fetch_zenrows_search(search_url, zenrows_api_key, debug)
                else:
                    debug(f"Page {page_number}: ZenRows skipped because no API key is configured.")

                if not raw_html:
                    debug(f"Page {page_number}: trying direct browser fallback.")
                    log("🔄 curl_cffi (chrome120): Trying direct browser search fallback...")
                    raw_html = await _fetch_direct_search(search_url, debug)
                if not raw_html:
                    # Keep the pre-existing local DoH route, but only after a normal browser
                    # request has had a chance to use the machine's configured DNS.
                    debug(f"Page {page_number}: trying local DoH fallback (Cloudflare).")
                    log("🔄 curl_cffi: Retrying search with Cloudflare DNS-over-HTTPS...")
                    raw_html = await _fetch_direct_search(
                        search_url, debug, doh_url="https://cloudflare-dns.com/dns-query",
                    )
                if not raw_html:
                    debug(f"Page {page_number}: trying local DoH fallback (Google).")
                    log("🔄 curl_cffi: Retrying search with Google DNS-over-HTTPS...")
                    raw_html = await _fetch_direct_search(
                        search_url, debug, doh_url="https://dns.google/dns-query",
                    )
                if not raw_html:
                    debug(f"Page {page_number}: trying cached/local search source.")
                    log("📂 Search cache: Checking saved local HTML after live providers failed...")
                    raw_html = _read_cached_search(search_url, debug)
                if not raw_html:
                    failed_urls.append(search_url)
                    log("All retrieval providers failed for this page; continuing.")
                    log(f"Page {page_number} is unavailable. Continuing the search.")
                    continue
                _write_search_cache(search_url, raw_html, debug)
                log("🕷️ BeautifulSoup: Parsing search-result table rows and extracting titles, metadata and document mirror URLs...")

                try:
                    search_rows = _extract_search_mirror_rows(raw_html, search_url)
                except Exception:
                    search_rows = []
                    log("Could not associate row mirrors; using the evaluator's source links.")
                    debug(f"Page {page_number}: mirror-row parsing failed:\n{traceback.format_exc()}")
                try:
                    search_documents = _extract_search_documents(raw_html, search_url)
                    _emit_pipeline_event(
                        "DOCUMENT_DISCOVERED", run_id=run_id, count=len(search_documents),
                        message=f"Page {page_number}: parsed {len(search_documents)} document records",
                    )
                    parsed_document_count = len(search_documents)
                    current_page_state["found"] = parsed_document_count
                    render_dashboard()
                    log(f"📋 Extraction complete: {len(search_documents)} document record(s), {len(search_rows)} row-level mirror group(s).")
                    debug(f"Page {page_number}: extracted {len(search_documents)} search-result document(s).")
                    debug(
                        "First 5 extracted titles:\n"
                        + "\n".join(
                            f"{index}. {document['title'][:180]}"
                            for index, document in enumerate(search_documents[:5], start=1)
                        )
                    )
                    debug(
                        "First 5 extracted URLs:\n"
                        + "\n".join(
                            f"{index}. {document['link']}"
                            for index, document in enumerate(search_documents[:5], start=1)
                        )
                    )
                    for index, document in enumerate(search_documents, start=1):
                        debug(
                            f"Search-result document {index} text length: "
                            f"{len(document['text'])} characters ({document['link']})"
                        )
                except Exception:
                    search_documents = []
                    parsed_document_count = 0
                    debug(f"Page {page_number}: search-result document parsing failed:\n{traceback.format_exc()}")

                completed_stage1_reader = globals().get("_stage1_completed_document_ids")
                identity_builder = globals().get("stable_document_id")
                if callable(completed_stage1_reader) and callable(identity_builder):
                    known_stage1_ids = completed_stage1_reader()
                    if known_stage1_ids:
                        before = len(search_documents)
                        search_documents = [
                            document for document in search_documents
                            if identity_builder({"source_url": document.get("link", "")}) not in known_stage1_ids
                        ]
                        if before != len(search_documents):
                            debug(
                                f"Page {page_number}: skipped {before - len(search_documents)} document(s) "
                                "whose Stage 1 lifecycle is already complete."
                            )

                if not search_documents:
                    if parsed_document_count:
                        debug(
                            f"Page {page_number}: all {parsed_document_count} parsed document(s) already have "
                            "a terminal Stage 1 lifecycle; continuing discovery."
                        )
                        log(f"📋 Page {page_number}: all parsed documents were already evaluated; continuing.")
                        continue
                    debug(f"Page {page_number}: no document records available for evaluation; stopping collection.")
                    log(f"📋 Page {page_number}: 0 document records — stopping further page collection.")
                    break
                document_batch = [
                    {"id": index, **document}
                    for index, document in enumerate(search_documents, start=1)
                ]

                try:
                    log("🕷️ BeautifulSoup: Converting fetched HTML to link-preserving text for semantic evaluation...")
                    markdown = await extract_markdown(raw_html)
                except Exception:
                    debug(f"Page {page_number}: HTML-to-text extraction failed:\n{traceback.format_exc()}")
                    log("This page could not be prepared for review. Continuing...")
                    continue
                if not markdown.strip():
                    debug(f"Page {page_number}: HTML-to-text extraction returned 0 characters.")
                    log("No readable document content found on this page.")
                    continue

                evaluator_content = _evaluator_document_context(search_documents) or markdown
                debug(
                    f"Page {page_number}: sending {len(search_documents)} document record(s) "
                    f"to the evaluator; extracted search text is {len(markdown)} characters; "
                    f"evaluator input is {len(evaluator_content)} characters."
                )

                log(f"🧠 Stage 1: Sending {len(search_documents)} document record(s) to CrewAI/OpenRouter...")
                _emit_pipeline_event("STAGE1_STARTED", run_id=run_id, count=len(document_batch))
                refresh_live_metrics()
                try:
                    graph_state = await invoke_scraper_graph(
                        {
                            "markdown_content": evaluator_content,
                            "raw_html": raw_html,
                            "document_batch": document_batch,
                            "extracted_documents": [],
                        },
                        on_status=log,
                    )
                    extracted_documents: list[dict[str, Any]] = graph_state.get(
                        "extracted_documents", []
                    )
                    approved_docs = [
                        document for document in extracted_documents
                        if document.get("approved") is not False
                    ]
                    approved_ids = {
                        int(document["id"]) for document in approved_docs
                        if isinstance(document.get("id"), int)
                    }
                    # Stage 1 rejects are terminal semantic decisions too.  Keep
                    # them in the same filename-keyed ledger with a stable source
                    # identity so a later Streamlit run does not call the LLM for
                    # the exact same Libgen record again.
                    for document in document_batch:
                        if document["id"] in approved_ids:
                            continue
                        rejected_title = str(document.get("title") or "Untitled document")
                        rejected_mirrors = _document_mirror_links(document, search_rows, search_url)
                        rejected_filename = _safe_pdf_filename(rejected_title, page_number, document["id"])
                        rejected_candidate = {
                            "filename": rejected_filename,
                            "source_url": str(document.get("link") or ""),
                            "mirrors": rejected_mirrors,
                            "document_id": stable_document_id({
                                "source_url": str(document.get("link") or ""),
                                "mirrors": rejected_mirrors,
                                "title": rejected_title,
                            }),
                            "title": rejected_title,
                            "query": query.strip(),
                            "run_id": run_id,
                            "source_page": page_number,
                            "source_item": int(document["id"]),
                            "stage1_status": "REJECTED",
                        }
                        _update_download_state(
                            rejected_filename, rejected_candidate["source_url"], "STAGE1_REJECTED",
                            candidate=rejected_candidate, completed=False,
                        )
                    _emit_pipeline_event("STAGE1_APPROVED", run_id=run_id, count=len(approved_docs))
                    _emit_pipeline_event(
                        "STAGE1_REJECTED", run_id=run_id,
                        count=max(0, len(document_batch) - len(approved_docs)),
                    )
                    current_page_state["approved"] = len(approved_docs)
                    current_page_state["rejected"] = max(0, len(document_batch) - len(approved_docs))
                    approved_detail_ids = {int(document["id"]) for document in approved_docs if isinstance(document.get("id"), int)}
                    for document in document_batch:
                        page_details.append({
                            "Page": page_number, "Item": document["id"], "Document": human_document_name(document),
                            "Stage 1": "Approved" if document["id"] in approved_detail_ids else "Rejected",
                            "Storage state": "—", "Action": "Awaiting queue" if document["id"] in approved_detail_ids else "Skip",
                        })
                    render_dashboard()
                    debug(
                        f"Page {page_number}: evaluator returned {len(extracted_documents)} document(s); "
                        f"{len(approved_docs)} approved for download."
                    )
                    debug(
                        "First 5 evaluator results:\n"
                        + "\n".join(
                            f"{index}. {document.get('title', 'Untitled')} -> "
                            f"{document.get('link', 'missing link')}"
                            for index, document in enumerate(extracted_documents[:5], start=1)
                        )
                    )
                except Exception:
                    _emit_pipeline_event("STAGE1_FAILED", run_id=run_id, count=len(document_batch))
                    refresh_live_metrics()
                    debug(f"Page {page_number}: document evaluation failed:\n{traceback.format_exc()}")
                    log("Document evaluation could not finish for this page. Continuing...")
                    continue

                pages_processed += 1
                documents_approved += len(approved_docs)
                if not approved_docs:
                    log("No relevant documents were approved on this page.")
                    continue

                main_status.update(
                    label=f"✅ AI Approved {len(approved_docs)} documents on page {page_number}.",
                    state="running", expanded=True,
                )
                collect_primary_outcomes()
                remaining = max(target_documents - documents_downloaded - max(primary_jobs_queued - coordinator.completed, 0), 0)
                already_stored = 0
                recovery_retained = 0
                queued_now = 0
                missing_links = 0
                log(f"📦 Page {page_number} queue decisions ({len(approved_docs[:remaining])} candidate(s)):")
                for document in approved_docs[:remaining]:
                    title = str(document.get("title") or "Untitled document")
                    mirror_links = _document_mirror_links(document, search_rows, search_url)
                    filename = _safe_pdf_filename(title, page_number, document["id"])
                    candidate_identity = stable_document_id({
                        "source_url": str(document.get("link") or ""),
                        "mirrors": mirror_links,
                        "title": title,
                    })
                    _, destination, _ = _download_paths(filename)
                    if destination.exists():
                        already_stored += 1
                        current_page_state["stored"] = already_stored
                        for row in reversed(page_details):
                            if row["Page"] == page_number and row["Item"] == document["id"]:
                                row.update({"Storage state": "Stored", "Action": "Skip"})
                                break
                        log(f"  📂 Already stored: {filename}; skipping download.")
                    elif filename in scheduled_filenames or candidate_identity in scheduled_document_ids:
                        recovery_retained += 1
                        current_page_state["recovery"] = recovery_retained
                        for row in reversed(page_details):
                            if row["Page"] == page_number and row["Item"] == document["id"]:
                                row.update({"Storage state": "Partial", "Action": "Recovery"})
                                break
                        log(f"  ♻️ Existing lifecycle state retained for: {filename}; avoiding a duplicate worker assignment.")
                    elif not mirror_links:
                        missing_links += 1
                        current_page_state["missing"] = missing_links
                        for row in reversed(page_details):
                            if row["Page"] == page_number and row["Item"] == document["id"]:
                                row.update({"Storage state": "No link", "Action": "Skip"})
                                break
                        failed_urls.append(str(document.get("link") or "missing document link"))
                        coordinator.presentation.add_activity(
                            f"✕ Search page {page_number} · item {document['id']}: no source link for {filename[:60]}"
                        )
                        coordinator._render_panels()
                        log(f"  ❌ Missing source link: {filename}")
                    else:
                        try:
                            stage1_score = int(document.get("score", 100) or 100)
                        except (TypeError, ValueError):
                            stage1_score = 100
                        candidate = {
                            "filename": filename,
                            "mirrors": mirror_links,
                            "source_url": str(document.get("link") or ""),
                            "stage1_score": stage1_score,
                            "document_id": candidate_identity,
                            "title": title,
                            "query": query.strip(),
                            "run_id": run_id,
                            "source_page": page_number,
                            "source_item": int(document["id"]),
                        }
                        scheduled_filenames.add(filename)
                        scheduled_document_ids.add(candidate_identity)
                        queued_now += 1
                        primary_jobs_queued += 1
                        current_page_state["queued"] = queued_now
                        for row in reversed(page_details):
                            if row["Page"] == page_number and row["Item"] == document["id"]:
                                row.update({"Storage state": "Queued", "Action": "Download"})
                                break
                        with downloads_area:
                            await coordinator.enqueue(candidate)
                        _emit_pipeline_event("DOWNLOAD_QUEUED", candidate=candidate, filename=filename)
                        log(f"  ⬇️ Queued for download: {filename}")
                refresh_live_metrics()
                log(
                    f"📊 Page {page_number} summary: queued {queued_now} · already stored {already_stored} · "
                    f"recovery retained {recovery_retained} · missing links {missing_links}"
                )

            if primary_jobs_queued:
                log(
                    f"⬇️ Global primary queue is draining {primary_jobs_queued} approved document(s) "
                    f"with up to {network.settings.max_workers} workers."
                )
            if restored_stage2_jobs:
                log(
                    f"📄 Restored {restored_stage2_jobs} completed PDF(s) into the bounded Stage 2 queue; "
                    "no download will be repeated."
                )
                primary_failed_results.extend(await coordinator.drain_until_idle())
                new_completions = coordinator.completed - queue_completions_seen
                documents_downloaded += new_completions
                queue_completions_seen = coordinator.completed
            for result in primary_failed_results:
                next_job, terminal = recovery_backlog.add_failed_attempt(
                    result, max_attempts=network.settings.max_document_attempts,
                )
                if terminal:
                    failed_urls.extend(dict(result.get("candidate") or {}).get("mirrors", []))
            coordinator.set_recovery_backlog(recovery_backlog.snapshot())

            recovery_round = 1
            while recovery_backlog:
                valid_recovery_jobs = recovery_backlog.take_round(
                    max_attempts=network.settings.max_document_attempts,
                )
                coordinator.set_recovery_backlog(recovery_backlog.snapshot())
                if not valid_recovery_jobs:
                    break
                delay = max(
                    network.settings.recovery_round_delay_seconds,
                    recovery_backlog.not_before - time.time(),
                )
                if delay:
                    log(
                        f"♻️ Recovery queue ready: {len(valid_recovery_jobs)} document(s). Waiting {delay:g}s before round {recovery_round}."
                    )
                    await asyncio.sleep(delay)
                else:
                    log(
                        f"♻️ Global recovery round {recovery_round}: {len(valid_recovery_jobs)} document(s)."
                    )
                with downloads_area:
                    for candidate in valid_recovery_jobs:
                        candidate.setdefault("run_id", run_id)
                        await coordinator.enqueue(candidate, recovery=True)
                        _emit_pipeline_event("RECOVERY_QUEUED", candidate=candidate, filename=candidate["filename"])
                refresh_live_metrics()
                failed_recovery = await coordinator.drain_until_idle()
                new_completions = coordinator.completed - queue_completions_seen
                documents_downloaded += new_completions
                queue_completions_seen = coordinator.completed
                for result in failed_recovery:
                    next_job, terminal = recovery_backlog.add_failed_attempt(
                        result, max_attempts=network.settings.max_document_attempts,
                    )
                    if terminal:
                        failed_urls.extend(dict(result.get("candidate") or {}).get("mirrors", []))
                coordinator.set_recovery_backlog(recovery_backlog.snapshot())
                recovery_round += 1

            refresh_live_metrics()
            if pages_processed or documents_downloaded >= target_documents:
                final_label = (
                    f"✅ Collection finished — {documents_downloaded} / {target_documents} stored; {documents_approved} AI approved."
                )
            elif documents_approved:
                final_label = f"✅ AI Approved {documents_approved} documents."
            elif pages_processed:
                final_label = "✅ Review complete — no relevant documents found."
            else:
                final_label = "❌ Research could not be completed."
            main_status.update(
                label=final_label,
                state="complete" if pages_processed or documents_downloaded >= target_documents else "error",
                expanded=False,
            )

    coordinator.close()
    network.close()
    stage2_totals = validation_summary(DATA_DIRECTORY)
    return {
        "pages_processed": pages_processed,
        "documents_approved": documents_approved,
        "documents_downloaded": documents_downloaded,
        "stage2_approved": stage2_totals["new_download_approved"],
    }


st.set_page_config(
    page_title="ASHRAE Intelligent Scraper",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .block-container {
            max-width: 1180px;
            padding-top: 3rem;
            padding-bottom: 3rem;
        }
        [data-testid="stSidebar"] {
            border-right: 1px solid rgba(128, 128, 128, 0.18);
        }
        .eyebrow {
            color: #2f80ed;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
        }
        .hero-copy {
            color: #7b8496;
            font-size: 1.08rem;
            line-height: 1.65;
            margin-bottom: 1.75rem;
            max-width: 760px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    search_query = st.text_input(
        "Search Query",
        value="ASHRAE standard chiller HVAC",
        placeholder="Enter a topic or document name",
    )
    max_pages = st.number_input(
        "Max Pages to Scrape",
        min_value=1,
        max_value=100,
        value=3,
        step=1,
    )
    target_documents = st.number_input(
        "Target Documents",
        min_value=1,
        max_value=500,
        value=50,
        step=1,
    )

st.markdown(
    '<div class="eyebrow">Agentic research pipeline</div>',
    unsafe_allow_html=True,
)
st.title("ASHRAE Intelligent Scraper")
st.markdown(
    '<div class="hero-copy">Discover, parse, evaluate, and organize relevant '
    "ASHRAE and HVAC documents with a supervised agent workflow.</div>",
    unsafe_allow_html=True,
)

summary_col, action_col = st.columns([3, 1], vertical_alignment="bottom")
with summary_col:
    st.subheader("Pipeline control")
    st.caption(
        f'Query: "{search_query or "Not set"}" · Page limit: {max_pages} · '
        f"Target: {target_documents}"
    )
    st.caption(f"LangSmith tracing: {langsmith_status()}")
with action_col:
    start_scraping = st.button(
        "Start Scraping",
        type="primary",
        use_container_width=True,
    )
    scan_existing = st.button("Scan Existing PDFs", use_container_width=True)
    force_revalidation = st.checkbox("Re-validate completed PDFs", value=False)

st.divider()
stage2_counts = validation_summary(DATA_DIRECTORY)
pending_existing = len(scan_existing_pdfs(DATA_DIRECTORY, revalidate=force_revalidation))
st.subheader("Library status")
st.caption(
    f"Existing PDF Scan: Found {pending_existing} pending PDF(s) · "
    f"Stage 2 Validation: Approved {stage2_counts['approved']} · Rejected {stage2_counts['rejected']} · "
    f"Pending/Error {stage2_counts['pending']}"
)
st.subheader("Agent activity")

if scan_existing:
    try:
        with st.status("📂 Existing PDF Scan initialized...", expanded=True) as scan_status:
            scan_result = scan_and_validate_existing_pdfs(
                scan_status.write, revalidate=force_revalidation,
            )
            scan_status.update(
                label=(
                    f"✅ Stage 2 complete — {scan_result['approved']} approved, "
                    f"{scan_result['rejected']} rejected, {scan_result['pending']} pending."
                ),
                state="complete", expanded=False,
            )
        st.success(
            f"Existing PDF Scan: Found {scan_result['found']} · "
            f"Processed {scan_result['processed']} / {scan_result['found']} · "
            f"Approved {scan_result['approved']} · Rejected {scan_result['rejected']} · Pending/Error {scan_result['pending']}"
        )
    except Exception as exc:
        st.error(f"Existing PDF validation could not finish: {exc}")
elif start_scraping:
    try:
        result_summary = asyncio.run(
            run_scraping_pipeline(
                search_query, int(max_pages), DEFAULT_MODE, int(target_documents),
            )
        )
        st.success(
            "Run finished: "
            f"{result_summary['pages_processed']} page(s) evaluated, "
            f"{result_summary['documents_downloaded']} PDF(s) downloaded."
        )
        st.caption(
            f"New Pipeline: Stage 1 approved {result_summary['documents_approved']} · "
            f"Downloaded {result_summary['documents_downloaded']} · "
            f"Final accepted {result_summary['stage2_approved']}"
        )
    except Exception as exc:
        st.error(f"The pipeline stopped unexpectedly: {exc}")
else:
    st.info("Configure the run and select **Start Scraping** to begin.", icon="ℹ️")
