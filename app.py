"""Streamlit entry point for the complete ASHRAE scraping pipeline."""

import asyncio
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
from app.pdf_validation import (
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
VALIDATION_WORKERS = 2
_DOWNLOAD_STATE_LOCK = threading.Lock()
_VALIDATION_EXECUTOR = ThreadPoolExecutor(max_workers=VALIDATION_WORKERS, thread_name_prefix="pdf-validation")
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
            if document_attempt is not None:
                entry["document_attempt"] = int(document_attempt)
            if request_attempt is not None:
                entry["request_attempt"] = int(request_attempt)
            if completed is not None:
                entry["completed"] = bool(completed)
            elif status.upper() == "COMPLETED":
                entry["completed"] = True
            elif status.upper() in {"QUEUED", "DOWNLOADING", "FAILED_FOR_ROUND", "RECOVERING", "PERMANENTLY_FAILED"}:
                entry["completed"] = False
            if retry_after_seconds is not None:
                entry["retry_after_until"] = time.time() + max(0.0, float(retry_after_seconds))
            elif status.upper() == "COMPLETED":
                entry.pop("retry_after_until", None)
            if validation_status is not None:
                entry["validation_status"] = validation_status
            if reason:
                entry["reason"] = reason
                entry["last_error"] = reason
            else:
                entry.pop("reason", None)
                entry.pop("last_error", None)
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
    completed = 0
    for path in DOWNLOAD_DIRECTORY.glob("*.pdf"):
        entry = _download_state_entry(path.name)
        if not entry or str(entry.get("status", "")).upper() != "COMPLETED":
            _update_download_state(path.name, "", "COMPLETED", size=path.stat().st_size, completed=True)
        completed += 1
    return completed


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
            st.session_state.setdefault("_download_debug", []).append(message)
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


def parse_mirror_and_download(mirror_link, filename, needs_page_check=True, *, on_status=None, on_progress=None, _visited=None, network=None, worker_id=0, proxy=None):
    """Port of the baseline mirror parser and alternative-gateway loop."""
    status_logger = globals().get("log_status")
    report = (
        (lambda message: status_logger(message, on_status))
        if status_logger else (on_status or logging.getLogger(__name__).info)
    )
    try:
        visited = _visited if _visited is not None else set()
        mirror_link = urllib.parse.urldefrag(mirror_link)[0]
        if mirror_link in visited or len(visited) >= 8:
            return "retry_next"
        visited.add(mirror_link)
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
        # A result row can already contain a direct Libgen GET/CDN route.  Do
        # not fetch it as HTML first: send it to the proven resumable transfer.
        if (
            "get.php" in mirror_parts.path.lower()
            or mirror_parts.path.lower().endswith(".pdf")
            or is_ipfs_gateway(mirror_link)
        ):
            report(f"Final download candidates:\n1. {mirror_link}")
            return download_file(
                mirror_link, filename, mirror_link,
                on_status=on_status, on_progress=on_progress,
                network=network, worker_id=worker_id, proxy=proxy,
            )
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
            if any(route in path for route in ("edition.php", "file.php", "ads.php")):
                if (host.startswith("libgen.") or ".libgen." in host) and link not in visited and link not in discovery_links:
                    discovery_links.append(link)
                return
            if not ("get.php" in path or (host == "library.lol" and path.startswith("/main/")) or path.endswith(".pdf") or is_ipfs_gateway(link)):
                return
            if link not in download_links and link not in visited:
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
                if parse_mirror_and_download(discovery_link, filename, False, on_status=on_status, on_progress=on_progress, _visited=visited, network=network, worker_id=worker_id, proxy=proxy) is True:
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
                result = parse_mirror_and_download(direct_link, filename, False, on_status=on_status, on_progress=on_progress, _visited=visited, network=network, worker_id=worker_id, proxy=proxy)
            else:
                result = download_file(
                    direct_link, filename, mirror_link, on_status=on_status, on_progress=on_progress,
                    network=network, worker_id=worker_id, proxy=proxy,
                )
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
                if parse_mirror_and_download(discovery_link, filename, False, on_status=on_status, on_progress=on_progress, _visited=visited, network=network, worker_id=worker_id, proxy=proxy) is True:
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
                        report(
                            f"📄 PyPDF: Queuing first-page ASHRAE/HVAC keyword verification in the background; "
                            f"validation taking over {VALIDATION_TIMEOUT_SECONDS}s is marked pending while downloads continue."
                        )
                        _schedule_validation(filename)
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
            candidates.append({"filename": filename, "mirrors": mirrors, "source_url": str(document.get("link") or "")})
    return candidates, skipped_completed


def _validate_pdf_stage2(
    file_path: Path, source: str, *, stage1_score: int | None = None, on_status=None, force: bool = False,
) -> dict[str, Any]:
    """Run conservative Stage 2 validation and preserve inconclusive files as pending."""
    report = on_status or logging.getLogger(__name__).info
    existing_report = completed_validation_report(file_path, DATA_DIRECTORY)
    if existing_report and not force:
        return existing_report

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
            report("🧠 LangGraph: Routing representative PDF evidence to the Stage 2 CrewAI validator...")
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
    if status not in {"APPROVED", "REJECTED", "PENDING"}:
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
        "stage2_completed": status in {"APPROVED", "REJECTED"},
    }
    write_validation_report(file_path, DATA_DIRECTORY, validation_report)
    report(
        f"Stage 2 {status.lower()}: {file_path.name} (score {validation_report['stage2_score']}) → {stored_path.name}"
    )
    if source == "new_download":
        _update_download_state(
            file_path.name, "", "COMPLETED", size=original_size,
            reason=validation_report["reason"], completed=True, validation_status=status,
        )
    return validation_report


def _schedule_validation(filename: str, *, stage1_score: int | None = 100) -> None:
    """Queue Stage 2 after a new download so transfer workers remain non-blocking."""
    _, destination, _ = _download_paths(filename)
    if not destination.exists():
        return
    _update_download_state(
        filename, "", "COMPLETED", size=destination.stat().st_size,
        completed=True, validation_status="PENDING",
    )
    logging.getLogger(__name__).info("Validation queued for %s", filename)

    def validate() -> None:
        try:
            _validate_pdf_stage2(destination, "new_download", stage1_score=stage1_score)
        except Exception as exc:
            _update_download_state(
                filename, "", "COMPLETED", size=_download_size(filename), completed=True,
                validation_status="PENDING", reason=f"validation error: {type(exc).__name__}",
            )
            logging.getLogger(__name__).warning("Validation pending for %s: %s", filename, type(exc).__name__)

    future = _VALIDATION_EXECUTOR.submit(validate)

    def mark_pending_if_slow() -> None:
        if not future.done():
            _update_download_state(
                filename, "", "COMPLETED", size=_download_size(filename), completed=True,
                validation_status="PENDING", reason="validation exceeded timeout",
            )
            logging.getLogger(__name__).info("Validation timed out for %s; left pending", filename)

    timer = threading.Timer(VALIDATION_TIMEOUT_SECONDS, mark_pending_if_slow)
    timer.daemon = True
    timer.start()


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


def _recovery_candidates(query: str) -> list[dict[str, Any]]:
    """Rebuild interrupted approved jobs without repeating search or Stage 1."""
    normalized_query = query.strip().lower()
    recovered: list[dict[str, Any]] = []
    with _DOWNLOAD_STATE_LOCK:
        with _download_state_process_lock() as acquired:
            if not acquired:
                return recovered
            state = _load_download_state()
            changed = False
            for filename, entry in state["downloads"].items():
                if not isinstance(entry, dict):
                    continue
                status = str(entry.get("status", "")).upper()
                candidate = entry.get("candidate")
                if status == "DOWNLOADING":
                    # A process cannot retain a live worker across a Streamlit/app
                    # restart.  Its on-disk .part remains the recovery source of truth.
                    entry["status"] = "FAILED_FOR_ROUND"
                    entry["last_error"] = "stale DOWNLOADING state recovered after restart"
                    status = "FAILED_FOR_ROUND"
                    changed = True
                if status not in {"QUEUED", "FAILED_FOR_ROUND", "RECOVERING", "PARTIAL", "FAILED"}:
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
                job = dict(candidate)
                job["filename"] = filename
                job["_document_attempt"] = max(1, int(entry.get("document_attempt", 0) or 0) + 1)
                recovered.append(job)
            if changed:
                _save_download_state(state)
    return recovered


def _download_worker(candidate: dict[str, Any], network: NetworkManager, worker_id: int) -> dict[str, Any]:
    """Perform one bounded document-level attempt; never render Streamlit here."""
    filename = candidate["filename"]
    document_attempt = int(candidate.get("_document_attempt", 1) or 1)
    max_attempts = network.settings.max_document_attempts
    state_candidate = {key: value for key, value in candidate.items() if not key.startswith("_")}
    _update_download_state(
        filename, candidate.get("source_url", ""), "DOWNLOADING", size=_download_size(filename),
        candidate=state_candidate, document_attempt=document_attempt, request_attempt=0, completed=False,
    )
    proxy = network.begin_download(worker_id, filename)

    def status(message: str) -> None:
        network.emit("status", worker_id, filename, message)

    def progress(downloaded: int, total: int | None) -> None:
        _update_download_state(
            filename, candidate.get("source_url", ""), "DOWNLOADING", size=downloaded,
            candidate=state_candidate, document_attempt=document_attempt,
        )
        network.emit("progress", worker_id, filename, downloaded=downloaded, total=total or 0)

    status(
        f"⬇️ {'Recovery' if document_attempt > 1 else 'Primary'} download attempt "
        f"{document_attempt}/{max_attempts}; existing partial: {_download_size(filename) / (1024 * 1024):.2f} MB."
    )
    last_reason = "all valid gateways failed"
    try:
        for mirror_attempt, mirror in enumerate(candidate["mirrors"], start=1):
            result = parse_mirror_and_download(
                mirror, filename, needs_page_check=False, on_status=status,
                on_progress=progress, network=network, worker_id=worker_id, proxy=proxy,
            )
            if result is True:
                _update_download_state(
                    filename, candidate.get("source_url", ""), "COMPLETED", size=_download_size(filename),
                    candidate=state_candidate, document_attempt=document_attempt, completed=True,
                )
                _schedule_validation(filename, stage1_score=candidate.get("stage1_score", 100))
                network.emit("success", worker_id, filename, "Downloaded successfully; Stage 2 PDF validation queued.")
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
    network.emit(
        "failure", worker_id, filename,
        f"⚠️ Download attempt ended\nDocument: {filename}\nAttempt: {document_attempt}/{max_attempts}\n"
        f"Downloaded: {_download_size(filename) / (1024 * 1024):.2f} MB\nReason: {last_reason}\nAction: {action}",
    )
    state_entry = _download_state_entry(filename) or {}
    return {
        "success": False, "candidate": candidate, "attempt": document_attempt,
        "permanent": final_status == "PERMANENTLY_FAILED",
        "retry_after_until": float(state_entry.get("retry_after_until", 0) or 0),
    }


async def _download_concurrent_cards(
    candidates: list[dict[str, Any]], network: NetworkManager, debug, *, executor: ThreadPoolExecutor | None = None,
) -> tuple[int, list[str]]:
    """Run bounded primary/recovery rounds using one fixed worker pool.

    Request-level retries happen inside ``download_file``.  This scheduler only
    retries a document after every current-round document has released its worker.
    """
    if not candidates:
        return 0, []
    unique_candidates: list[dict[str, Any]] = []
    seen_filenames: set[str] = set()
    for candidate in candidates:
        filename = candidate["filename"]
        if filename not in seen_filenames:
            seen_filenames.add(filename)
            unique_candidates.append(dict(candidate))
    cards = {}
    for candidate in unique_candidates:
        filename = candidate["filename"]
        with st.status(f"⬇️ Processing: {filename[:40]}...", expanded=False) as card:
            card.write("⏳ Queued for a controlled curl_cffi worker...")
            cards[filename] = (card, st.empty())

    completed, failed, recovered_successfully, permanent_failures = 0, [], 0, 0
    round_number = 1
    current_round = unique_candidates
    executor_context = nullcontext(executor) if executor else ThreadPoolExecutor(
        max_workers=network.settings.max_workers, thread_name_prefix="pdf-download",
    )
    with executor_context as active_executor:
        while current_round:
            for candidate in current_round:
                candidate.setdefault("_document_attempt", 1 if round_number == 1 else 2)
                _update_download_state(
                    candidate["filename"], candidate.get("source_url", ""),
                    "RECOVERING" if round_number > 1 else "QUEUED", size=_download_size(candidate["filename"]),
                    candidate={key: value for key, value in candidate.items() if not key.startswith("_")},
                    document_attempt=int(candidate["_document_attempt"]), completed=False,
                )
            st.caption(
                f"Download Round: {round_number} · Configured Workers: {network.settings.max_workers} · "
                f"Active Workers: {min(network.settings.max_workers, len(current_round))} · "
                f"Completed: {completed} · Recovery queue: {len(current_round) if round_number > 1 else 0} · "
                f"Recovered successfully: {recovered_successfully} · Permanent failures: {permanent_failures}"
            )
            pending = {
                active_executor.submit(
                    _download_worker, candidate, network, (index % network.settings.max_workers) + 1,
                ): candidate
                for index, candidate in enumerate(current_round)
            }
            failed_for_round: list[dict[str, Any]] = []
            recovery_not_before = 0.0
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
                        total = event["total"]
                        transferred = event["downloaded"]
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
                        result = {"success": False, "candidate": candidate, "attempt": int(candidate.get("_document_attempt", 1)), "permanent": False}
                        debug(f"Filename: {filename}\nFailure reason:\n{traceback.format_exc()}")
                    if result["success"]:
                        completed += 1
                        if int(result["attempt"]) > 1:
                            recovered_successfully += 1
                        card.update(label=f"✅ Secured: {filename[:40]}", state="complete", expanded=False)
                        debug(f"Filename: {filename}\nDownloaded successfully")
                    elif result.get("permanent"):
                        _update_download_state(
                            filename, candidate.get("source_url", ""), "PERMANENTLY_FAILED",
                            size=_download_size(filename), candidate={key: value for key, value in candidate.items() if not key.startswith("_")},
                            document_attempt=int(result["attempt"]), completed=False,
                        )
                        permanent_failures += 1
                        failed.extend(candidate.get("mirrors", []))
                        card.update(label=f"❌ Failed: {filename[:40]}", state="error", expanded=False)
                    else:
                        _update_download_state(
                            filename, candidate.get("source_url", ""), "FAILED_FOR_ROUND",
                            size=_download_size(filename), candidate={key: value for key, value in candidate.items() if not key.startswith("_")},
                            document_attempt=int(result["attempt"]), completed=False,
                        )
                        failed.extend(candidate.get("mirrors", []))
                        next_candidate = dict(result["candidate"])
                        next_candidate["_document_attempt"] = int(result["attempt"]) + 1
                        failed_for_round.append(next_candidate)
                        recovery_not_before = max(recovery_not_before, float(result.get("retry_after_until", 0) or 0))
                        card.update(label=f"⚠️ Pending recovery: {filename[:40]}", state="running", expanded=False)
                await asyncio.sleep(0)

            # DOCUMENT-LEVEL RECOVERY: never recycle a failed file until every
            # file in this round has had an opportunity to use a worker.
            if not failed_for_round:
                break
            delay = max(network.settings.recovery_round_delay_seconds, recovery_not_before - time.time())
            st.caption(
                f"♻️ Recovery queue ready: {len(failed_for_round)} document(s). "
                f"Waiting {delay:g}s before the next round."
            )
            await asyncio.sleep(delay)
            current_round = failed_for_round
            round_number += 1
    st.caption(
        f"Download run finished · Completed: {completed} · Recovered: {recovered_successfully} · "
        f"Permanently failed: {permanent_failures}"
    )
    return completed, failed


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
    if hasattr(st, "session_state"):
        st.session_state["_download_debug"] = []
    failed_urls: list[str] = []
    debug_events: list[str] = []
    network = NetworkManager(PROJECT_ROOT)
    # One fixed pool serves primary and automatic recovery rounds for this run.
    # Individual document failures release their future; the pool is not rebuilt.
    download_executor = ThreadPoolExecutor(
        max_workers=network.settings.max_workers, thread_name_prefix="pdf-download",
    )
    scheduled_filenames: set[str] = set()

    def debug(message: str) -> None:
        debug_events.append(message)
    # Sibling containers keep the document cards visible when the pipeline closes.
    pipeline_area = st.container()
    downloads_area = st.container()

    with pipeline_area:
        with st.status("🤖 Agentic Pipeline Initialized...", expanded=True) as main_status:
            restart_recovery_jobs = _recovery_candidates(query)
            if restart_recovery_jobs:
                main_status.write(
                    f"♻️ Restored {len(restart_recovery_jobs)} interrupted download(s); "
                    "resuming their existing .part files before new retrieval."
                )
                scheduled_filenames.update(job["filename"] for job in restart_recovery_jobs)
                with downloads_area:
                    completed_now, failed_now = await _download_concurrent_cards(
                        restart_recovery_jobs, network, debug, executor=download_executor,
                    )
                documents_downloaded += completed_now
                failed_urls.extend(failed_now)
            for page_number in range(1, max_pages + 1):
                if documents_downloaded >= target_documents:
                    debug(f"Target of {target_documents} completed files reached; stopping page collection.")
                    break
                main_status.update(
                    label=f"🤖 Researching page {page_number} of {max_pages}...",
                    state="running", expanded=True,
                )
                if page_number > 1:
                    await asyncio.sleep(random.uniform(9.0, 15.0))
                encoded_query = quote(query.strip(), safe="")
                st.write(
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
                    main_status.write("🔄 curl_cffi (chrome120): Trying direct browser search fallback...")
                    raw_html = await _fetch_direct_search(search_url, debug)
                if not raw_html:
                    # Keep the pre-existing local DoH route, but only after a normal browser
                    # request has had a chance to use the machine's configured DNS.
                    debug(f"Page {page_number}: trying local DoH fallback (Cloudflare).")
                    main_status.write("🔄 curl_cffi: Retrying search with Cloudflare DNS-over-HTTPS...")
                    raw_html = await _fetch_direct_search(
                        search_url, debug, doh_url="https://cloudflare-dns.com/dns-query",
                    )
                if not raw_html:
                    debug(f"Page {page_number}: trying local DoH fallback (Google).")
                    main_status.write("🔄 curl_cffi: Retrying search with Google DNS-over-HTTPS...")
                    raw_html = await _fetch_direct_search(
                        search_url, debug, doh_url="https://dns.google/dns-query",
                    )
                if not raw_html:
                    debug(f"Page {page_number}: trying cached/local search source.")
                    main_status.write("📂 Search cache: Checking saved local HTML after live providers failed...")
                    raw_html = _read_cached_search(search_url, debug)
                if not raw_html:
                    failed_urls.append(search_url)
                    main_status.write("All retrieval providers failed for this page; continuing.")
                    st.write(f"Page {page_number} is unavailable. Continuing the search.")
                    continue
                _write_search_cache(search_url, raw_html, debug)
                main_status.write("🕷️ BeautifulSoup: Parsing search-result table rows and extracting titles, metadata and document mirror URLs...")

                try:
                    search_rows = _extract_search_mirror_rows(raw_html, search_url)
                except Exception:
                    search_rows = []
                    main_status.write("Could not associate row mirrors; using the evaluator's source links.")
                    debug(f"Page {page_number}: mirror-row parsing failed:\n{traceback.format_exc()}")
                try:
                    search_documents = _extract_search_documents(raw_html, search_url)
                    main_status.write(f"📋 Extraction complete: {len(search_documents)} document record(s), {len(search_rows)} row-level mirror group(s).")
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
                    debug(f"Page {page_number}: search-result document parsing failed:\n{traceback.format_exc()}")

                if not search_documents:
                    debug(f"Page {page_number}: no document records available for evaluation; stopping collection.")
                    break
                document_batch = [
                    {"id": index, **document}
                    for index, document in enumerate(search_documents, start=1)
                ]

                try:
                    st.write("🕷️ BeautifulSoup: Converting fetched HTML to link-preserving text for semantic evaluation...")
                    markdown = await extract_markdown(raw_html)
                except Exception:
                    debug(f"Page {page_number}: HTML-to-text extraction failed:\n{traceback.format_exc()}")
                    st.write("This page could not be prepared for review. Continuing...")
                    continue
                if not markdown.strip():
                    debug(f"Page {page_number}: HTML-to-text extraction returned 0 characters.")
                    st.write("No readable document content found on this page.")
                    continue

                evaluator_content = _evaluator_document_context(search_documents) or markdown
                debug(
                    f"Page {page_number}: sending {len(search_documents)} document record(s) "
                    f"to the evaluator; extracted search text is {len(markdown)} characters; "
                    f"evaluator input is {len(evaluator_content)} characters."
                )

                st.write(f"🧠 LangGraph: Routing {len(search_documents)} parsed document record(s) to the CrewAI orchestrator...")
                st.write("🤖 CrewAI: Requesting HVAC/ASHRAE semantic relevance evaluation through OpenRouter's free router (openrouter/openrouter/free)...")
                try:
                    graph_state = await invoke_scraper_graph(
                        {
                            "markdown_content": evaluator_content,
                            "raw_html": raw_html,
                            "document_batch": document_batch,
                            "extracted_documents": [],
                        },
                        on_status=st.write,
                    )
                    extracted_documents: list[dict[str, Any]] = graph_state.get(
                        "extracted_documents", []
                    )
                    approved_docs = [
                        document for document in extracted_documents
                        if document.get("approved") is not False
                    ]
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
                    debug(f"Page {page_number}: document evaluation failed:\n{traceback.format_exc()}")
                    st.write("Document evaluation could not finish for this page. Continuing...")
                    continue

                pages_processed += 1
                documents_approved += len(approved_docs)
                if not approved_docs:
                    st.write("No relevant documents were approved on this page.")
                    continue

                main_status.update(
                    label=f"✅ AI Approved {len(approved_docs)} documents.",
                    state="complete", expanded=False,
                )
                download_jobs = []
                remaining = max(target_documents - documents_downloaded, 0)
                for document in approved_docs[:remaining]:
                    title = str(document.get("title") or "Untitled document")
                    mirror_links = _document_mirror_links(document, search_rows, search_url)
                    filename = _safe_pdf_filename(title, page_number, document["id"])
                    _, destination, _ = _download_paths(filename)
                    if destination.exists():
                        main_status.write(f"📂 Already stored: {filename}; skipping download.")
                    elif filename in scheduled_filenames:
                        main_status.write(f"♻️ Existing recovery state retained for: {filename}; avoiding a duplicate worker assignment.")
                    elif not mirror_links:
                        failed_urls.append(str(document.get("link") or "missing document link"))
                        with st.status(f"⬇️ Processing: {filename[:40]}...", expanded=False) as missing_status:
                            st.write("No source link was available for this document.")
                            missing_status.update(label=f"❌ Failed: {filename[:40]}", state="error", expanded=False)
                    else:
                        try:
                            stage1_score = int(document.get("score", 100) or 100)
                        except (TypeError, ValueError):
                            stage1_score = 100
                        download_jobs.append({
                            "filename": filename,
                            "mirrors": mirror_links,
                            "source_url": str(document.get("link") or ""),
                            "stage1_score": stage1_score,
                            "document_id": str(document.get("id") or filename),
                            "title": title,
                            "query": query.strip(),
                        })
                        scheduled_filenames.add(filename)
                with downloads_area:
                    completed_now, failed_now = await _download_concurrent_cards(
                        download_jobs, network, debug, executor=download_executor,
                    )
                    documents_downloaded += completed_now
                    failed_urls.extend(failed_now)

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

    with st.expander("Retrieval debug", expanded=False):
        st.caption(
            f"Pages evaluated: {pages_processed} · Documents approved: {documents_approved} · "
            f"Documents downloaded: {documents_downloaded}"
        )
        if failed_urls:
            st.write("Failed URLs (up to 20):")
            st.code("\n".join(failed_urls[:20]))
        if debug_events:
            st.write("Provider attempts and responses:")
            st.code("\n".join(debug_events))
        else:
            st.write("No retrieval events were recorded.")
        download_events = getattr(st, "session_state", {}).get("_download_debug", [])
        if download_events:
            st.write("Document download outcomes:")
            st.code("\n\n".join(download_events))

    download_executor.shutdown(wait=True)
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
with action_col:
    start_scraping = st.button(
        "Start Scraping",
        type="primary",
        use_container_width=True,
    )
    scan_existing = st.button("Scan Existing PDFs", use_container_width=True)
    force_revalidation = st.checkbox("Re-validate completed PDFs", value=False)

st.divider()
st.subheader("Agent activity")

stage2_counts = validation_summary(DATA_DIRECTORY)
pending_existing = len(scan_existing_pdfs(DATA_DIRECTORY, revalidate=force_revalidation))
st.caption(
    f"Existing PDF Scan: Found {pending_existing} pending PDF(s) · "
    f"Stage 2 Validation: Approved {stage2_counts['approved']} · Rejected {stage2_counts['rejected']} · "
    f"Pending/Error {stage2_counts['pending']}"
)

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
