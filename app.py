"""Streamlit entry point for the complete ASHRAE scraping pipeline."""

import asyncio
import hashlib
import logging
import os
import random
import re
import shutil
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

from app.agents.orchestrator import invoke_scraper_graph
from app.semantic_extractor import extract_markdown


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOAD_DIRECTORY = PROJECT_ROOT / "data" / "ASHRAE_Files"
LOW_RELEVANCE_DIRECTORY = PROJECT_ROOT / "data" / "Low_Relevance_Files"
SEARCH_CACHE_DIRECTORY = PROJECT_ROOT / "data" / "search_cache"
SEARCH_TIMEOUT_SECONDS = 45
SEARCH_RETRIES = 2
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
        row_links, mirror_links = [], []
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
            if label.isdigit() or any(token in link.lower() for token in ("ads.php", "book/", "main/", "libgen")):
                if link not in mirror_links:
                    mirror_links.append(link)
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


def download_file(direct_link, filename, referer_url, *, on_status=None, on_progress=None):
    """Synchronous baseline transfer; return True or 'retry_next' for another gateway."""
    report = on_status or logging.getLogger(__name__).info
    _, destination, part_path = _download_paths(filename)
    download_headers = {
        "Referer": referer_url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    max_attempts = 5
    for attempt in range(max_attempts):
        existing_size = part_path.stat().st_size if part_path.exists() else 0
        # Match the baseline: restart very small partial files.
        if existing_size > 2048:
            download_headers["Range"] = f"bytes={existing_size}-"
        else:
            existing_size = 0
            download_headers.pop("Range", None)
        try:
            with c_requests.Session() as s:
                r = s.get(
                    direct_link, headers=download_headers, stream=True,
                    timeout=180, impersonate="chrome120",
                )
                try:
                    if r.status_code >= 500:
                        report(f"🔄 Server returned {r.status_code} (Attempt {attempt + 1}/{max_attempts}). Cooling down for 10s...")
                        time.sleep(10)
                        continue
                    if r.status_code == 416:
                        report("🔄 Resume range unavailable. Restarting the partial download...")
                        part_path.unlink(missing_ok=True)
                        continue
                    if r.status_code not in (200, 206):
                        report(f"Gateway returned {r.status_code}. Trying another link...")
                        return "retry_next"
                    if "text/html" in r.headers.get("Content-Type", "").lower():
                        report("🔄 Gateway returned an HTML challenge. Trying another link...")
                        time.sleep(5)
                        return "retry_next"

                    length = r.headers.get("content-length")
                    expected_received = int(length) if length else None
                    if r.status_code == 200:
                        existing_size = 0
                        mode = "wb"
                        total_size = expected_received
                    else:
                        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", r.headers.get("Content-Range", ""))
                        if not match:
                            raise RuntimeError("Missing resume byte range")
                        start, end, total_size = map(int, match.groups())
                        if start != existing_size or end < start or end >= total_size:
                            raise RuntimeError("Inconsistent resume byte range")
                        if expected_received is not None and expected_received != end - start + 1:
                            raise RuntimeError("Inconsistent resume length")
                        expected_received = end - start + 1
                        mode = "ab"

                    received = 0
                    if on_progress:
                        on_progress(existing_size, total_size)
                    with part_path.open(mode) as pdf_file:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                pdf_file.write(chunk)
                                received += len(chunk)
                                if on_progress:
                                    on_progress(existing_size + received, total_size)
                    if not received or (expected_received is not None and received != expected_received):
                        raise RuntimeError("Incomplete stream; retaining partial bytes")
                    if total_size is not None and part_path.stat().st_size != total_size:
                        raise RuntimeError("Incomplete file; retaining partial bytes")
                finally:
                    r.close()

            if part_path.stat().st_size < 2048:
                report("Gateway returned a file smaller than the baseline minimum. Trying another link...")
                part_path.unlink()
                return "retry_next"
            with part_path.open("rb") as pdf_file:
                if b"%PDF-" not in pdf_file.read(1024):
                    report("Gateway did not return a PDF. Trying another link...")
                    part_path.unlink()
                    return "retry_next"
            os.replace(part_path, destination)
            return True
        except Exception as exc:
            report(f"🔄 Transfer interrupted ({type(exc).__name__}, Attempt {attempt + 1}/{max_attempts}).")
            if attempt + 1 < max_attempts:
                time.sleep(5)
    return "retry_next"


def _check_download_relevance(filename, on_status=None) -> bool:
    """Retain baseline relevance rules and move rejected files without overwriting."""
    report = on_status or logging.getLogger(__name__).info
    safe_filename, destination, _ = _download_paths(filename)
    os.makedirs(LOW_RELEVANCE_DIRECTORY, exist_ok=True)
    report("🔎 Checking first-page ASHRAE/HVAC relevance...")
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


def parse_mirror_and_download(mirror_link, filename, needs_page_check=True, *, on_status=None, on_progress=None):
    """Parse a mirror synchronously, then exhaust its baseline download candidates."""
    report = on_status or logging.getLogger(__name__).info
    try:
        # Search providers may return a direct PDF rather than a Libgen mirror page.
        # Validate the bytes in download_file before accepting either route.
        parsed_mirror = urlparse(mirror_link)
        if parsed_mirror.path.lower().endswith(".pdf"):
            result = download_file(
                mirror_link, filename, mirror_link, on_status=report, on_progress=on_progress,
            )
            if result is True:
                return _check_download_relevance(filename, report) if needs_page_check else True
            return False
        with c_requests.Session() as s:
            res = s.get(
                mirror_link, timeout=globals().get("SEARCH_TIMEOUT_SECONDS", 45), impersonate="chrome120",
                headers=globals().get("BROWSER_HEADERS", {}),
            )
            try:
                if res.status_code != 200:
                    report(f"Mirror page returned {res.status_code}. Trying another mirror...")
                    return False
                soup = BeautifulSoup(res.text, "html.parser")
            finally:
                res.close()
        download_links = []
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href", "")).strip()
            label = anchor.get_text(strip=True).upper()
            if not href or "ads.php" in href.lower():
                continue
            if (any(word in label for word in ("GET", "DOWNLOAD", "IPFS"))
                    or "get.php" in href.lower() or "md5=" in href.lower()):
                full_link = urllib.parse.urldefrag(urljoin(mirror_link, href))[0]
                if urlparse(full_link).scheme in ("http", "https") and full_link not in download_links:
                    download_links.append(full_link)
        if not download_links:
            report("No download links found. Trying another mirror...")
            return False
        for direct_link in download_links:
            result = download_file(
                direct_link, filename, mirror_link, on_status=report, on_progress=on_progress,
            )
            if result is True:
                return _check_download_relevance(filename, report) if needs_page_check else True
            if result == "retry_next":
                report("🔄 Switching to an alternative gateway...")
                time.sleep(5)
                continue
            break
    except Exception as exc:
        report(f"Mirror processing failed ({type(exc).__name__}). Trying another mirror...")
    return False


def download_pdf(url: str | list[str], filename: str) -> bool:
    """Synchronous Streamlit bridge for all mirrors belonging to one book."""
    with st.status(f"⬇️ Processing: {filename[:40]}...", expanded=True) as dl_status:
        st.write("🛡️ Analyzing mirror gateways...")
        progress_placeholder = st.empty()

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
                if parse_mirror_and_download(
                    mirror, filename, needs_page_check=False, on_status=st.write, on_progress=progress,
                ):
                    # Check once after download; rejection must not re-download the same book.
                    relevant = _check_download_relevance(filename, st.write)
                    progress_placeholder.empty()
                    dl_status.update(
                        label=f"✅ Secured: {filename[:40]}" if relevant else f"⚠️ Low relevance: {filename[:40]}",
                        state="complete", expanded=False,
                    )
                    return relevant
        except Exception as exc:
            st.write(f"Download or relevance filing could not finish ({type(exc).__name__}).")
        progress_placeholder.empty()
        dl_status.update(label=f"❌ Failed: {filename[:40]}", state="error", expanded=False)
        return False


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

async def run_scraping_pipeline(query: str, max_pages: int) -> dict[str, int]:
    """Present search, evaluation, and file stages as cascading status cards."""
    zenrows_api_key = os.getenv("ZENROWS_API_KEY")
    if not query.strip():
        raise ValueError("Search query cannot be empty")
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")

    pages_processed = 0
    documents_approved = 0
    documents_downloaded = 0
    failed_urls: list[str] = []
    debug_events: list[str] = []

    def debug(message: str) -> None:
        logging.getLogger(__name__).info("retrieval: %s", message)
        debug_events.append(message)
    # Sibling containers keep downloads visible when the pipeline card collapses.
    pipeline_area = st.container()
    downloads_area = st.container()

    with pipeline_area:
        with st.status("🤖 Agentic Pipeline Initialized...", expanded=True) as main_status:
            for page_number in range(1, max_pages + 1):
                main_status.update(
                    label=f"🤖 Researching page {page_number} of {max_pages}...",
                    state="running", expanded=True,
                )
                if page_number > 1:
                    await asyncio.sleep(random.uniform(9.0, 15.0))
                encoded_query = quote(query.strip(), safe="")
                st.write("⏳ Fetching live search results...")
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
                    raw_html = await _fetch_direct_search(search_url, debug)
                if not raw_html:
                    # Keep the pre-existing local DoH route, but only after a normal browser
                    # request has had a chance to use the machine's configured DNS.
                    debug(f"Page {page_number}: trying local DoH fallback (Cloudflare).")
                    raw_html = await _fetch_direct_search(
                        search_url, debug, doh_url="https://cloudflare-dns.com/dns-query",
                    )
                if not raw_html:
                    debug(f"Page {page_number}: trying local DoH fallback (Google).")
                    raw_html = await _fetch_direct_search(
                        search_url, debug, doh_url="https://dns.google/dns-query",
                    )
                if not raw_html:
                    debug(f"Page {page_number}: trying cached/local search source.")
                    raw_html = _read_cached_search(search_url, debug)
                if not raw_html:
                    failed_urls.append(search_url)
                    main_status.write("All retrieval providers failed for this page; continuing.")
                    st.write(f"Page {page_number} is unavailable. Continuing the search.")
                    continue
                _write_search_cache(search_url, raw_html, debug)
                main_status.write("✅ Search results collected. Preparing document content for review...")

                try:
                    search_rows = _extract_search_mirror_rows(raw_html, search_url)
                except Exception:
                    search_rows = []
                    main_status.write("Could not associate row mirrors; using the evaluator's source links.")
                    debug(f"Page {page_number}: mirror-row parsing failed:\n{traceback.format_exc()}")
                try:
                    search_documents = _extract_search_documents(raw_html, search_url)
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
                try:
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

                st.write("🧠 AI Evaluator is semantically analyzing documents...")
                try:
                    graph_state = await invoke_scraper_graph(
                        {
                            "markdown_content": evaluator_content,
                            "raw_html": raw_html,
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
                # Place each file card below, rather than inside, the approval card.
                with downloads_area:
                    for item_number, document in enumerate(approved_docs, start=1):
                        title = str(document.get("title") or "Untitled document")
                        mirror_links = _document_mirror_links(document, search_rows, search_url)
                        filename = _safe_pdf_filename(title, page_number, item_number)
                        if not mirror_links:
                            failed_urls.append(str(document.get("link") or "missing document link"))
                            with st.status(
                                f"⬇️ Processing: {filename[:40]}...", expanded=True
                            ) as missing_status:
                                st.write("No source link was available for this document.")
                                missing_status.update(
                                    label=f"❌ Failed: {filename[:40]}",
                                    state="error", expanded=False,
                                )
                            continue
                        try:
                            if download_pdf(mirror_links, filename):
                                documents_downloaded += 1
                            else:
                                failed_urls.extend(mirror_links)
                        except Exception:
                            failed_urls.extend(mirror_links)
                            # Preserve a visible file outcome even for unexpected UI failures.
                            with st.status(
                                f"⬇️ Processing: {filename[:40]}...", expanded=True
                            ) as failed_status:
                                st.write("This document could not be retrieved.")
                                failed_status.update(
                                    label=f"❌ Failed: {filename[:40]}",
                                    state="error", expanded=False,
                                )

            if documents_approved:
                final_label = f"✅ AI Approved {documents_approved} documents."
            elif pages_processed:
                final_label = "✅ Review complete — no relevant documents found."
            else:
                final_label = "❌ Research could not be completed."
            main_status.update(
                label=final_label,
                state="complete" if pages_processed else "error",
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

    return {
        "pages_processed": pages_processed,
        "documents_approved": documents_approved,
        "documents_downloaded": documents_downloaded,
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
    st.header("Configuration")
    st.caption("Choose the search scope for this run.")
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
    st.divider()
    st.caption("Downloads are saved to data/ASHRAE_Files.")

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
    st.caption(f'Query: "{search_query or "Not set"}" · Page limit: {max_pages}')
with action_col:
    start_scraping = st.button(
        "Start Scraping",
        type="primary",
        use_container_width=True,
    )

st.divider()
st.subheader("Agent activity")

if start_scraping:
    try:
        result_summary = asyncio.run(
            run_scraping_pipeline(search_query, int(max_pages))
        )
        st.success(
            "Run finished: "
            f"{result_summary['pages_processed']} page(s) evaluated, "
            f"{result_summary['documents_downloaded']} PDF(s) downloaded."
        )
    except Exception as exc:
        st.error(f"The pipeline stopped unexpectedly: {exc}")
else:
    st.info("Configure the run and select **Start Scraping** to begin.", icon="ℹ️")
