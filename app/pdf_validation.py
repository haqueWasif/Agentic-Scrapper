"""Small, file-oriented helpers for the second PDF-content validation stage."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any
import warnings

from pypdf import PdfReader


DEFAULT_MAX_PREVIEW_PAGES = 10
DEFAULT_MIN_PREVIEW_CHARACTERS = 2500
DEFAULT_MAX_PREVIEW_CHARACTERS = 12000
MIN_USEFUL_PAGE_CHARACTERS = 80
MINIMUM_PDF_BYTES = 128


def check_pdf_integrity(file_path: Path, *, minimum_size: int = MINIMUM_PDF_BYTES) -> dict[str, Any]:
    """Perform the bounded technical gate required before semantic Stage 2 work.

    It reads only the signature and PDF cross-reference/page metadata.  It does
    not extract page text and never calls an LLM.
    """
    try:
        if not file_path.is_file():
            return {"valid": False, "error_type": "PDF_MISSING", "error_message": "file does not exist", "size": 0, "page_count": 0}
        size = file_path.stat().st_size
    except OSError as exc:
        return {"valid": False, "error_type": "PDF_PARSE_ERROR", "error_message": str(exc)[:240], "size": 0, "page_count": 0}
    if size == 0:
        return {"valid": False, "error_type": "EMPTY_PDF", "error_message": "file is zero bytes", "size": size, "page_count": 0}
    if size < max(1, int(minimum_size)):
        return {"valid": False, "error_type": "PDF_TRUNCATED", "error_message": f"file is only {size} bytes", "size": size, "page_count": 0}
    try:
        with file_path.open("rb") as source:
            signature = source.read(8)
            if not signature.startswith(b"%PDF-"):
                return {
                    "valid": False, "error_type": "INVALID_PDF_HEADER",
                    "error_message": f"expected %PDF- signature, found {signature[:8]!r}", "size": size, "page_count": 0,
                }
            source.seek(0)
            # PyPDF may issue recoverable warnings for malformed input.  This
            # gate is intentionally strict: unreadable files do not proceed to
            # the semantic Stage 2 workflow.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                reader = PdfReader(source, strict=True)
                page_count = len(reader.pages)
    except Exception as exc:
        message = str(exc)[:240]
        lowered = message.lower()
        error_type = "PDF_TRUNCATED" if any(marker in lowered for marker in ("eof", "startxref", "truncated")) else "PDF_PARSE_ERROR"
        return {"valid": False, "error_type": error_type, "error_message": message, "size": size, "page_count": 0}
    if page_count < 1:
        return {"valid": False, "error_type": "ZERO_PAGE_PDF", "error_message": "PDF contains no pages", "size": size, "page_count": 0}
    return {"valid": True, "error_type": None, "error_message": None, "size": size, "page_count": page_count}

def validation_directories(data_directory: Path) -> dict[str, Path]:
    """Create the three explicit Stage 2 storage destinations."""
    directories = {
        "approved": data_directory / "approved",
        "rejected": data_directory / "rejected",
        "reports": data_directory / "validation_reports",
    }
    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)
    return directories


def _report_key(file_path: Path) -> str:
    return hashlib.sha256(str(file_path.resolve()).encode("utf-8")).hexdigest()


def _matching_report_path(file_path: Path, data_directory: Path) -> Path | None:
    """Find a report after a validated PDF has moved into approved/rejected."""
    target = file_path.resolve()
    for report_path in validation_directories(data_directory)["reports"].glob("*.json"):
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for key in ("filepath", "stored_path"):
            value = report.get(key)
            if value:
                try:
                    if Path(str(value)).resolve() == target:
                        return report_path
                except OSError:
                    continue
    return None


def validation_report_path(file_path: Path, data_directory: Path) -> Path:
    return _matching_report_path(file_path, data_directory) or (
        validation_directories(data_directory)["reports"] / f"{_report_key(file_path)}.json"
    )


def completed_validation_report(file_path: Path, data_directory: Path) -> dict[str, Any] | None:
    report_path = validation_report_path(file_path, data_directory)
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return report if isinstance(report, dict) and report.get("stage2_completed") is True else None


def scan_existing_pdfs(data_directory: Path, *, revalidate: bool = False) -> list[dict[str, Any]]:
    """List unmanaged PDFs under data/, excluding Stage 2 destination folders."""
    directories = validation_directories(data_directory)
    excluded = {directory.resolve() for name, directory in directories.items() if name != "reports"}
    results: list[dict[str, Any]] = []
    for file_path in data_directory.rglob("*.pdf"):
        if not file_path.is_file() or (not revalidate and any(parent.resolve() in excluded for parent in file_path.parents)):
            continue
        if not revalidate and completed_validation_report(file_path, data_directory):
            continue
        stat = file_path.stat()
        results.append({
            "filename": file_path.name,
            "filepath": str(file_path),
            "size": stat.st_size,
            "created_time": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat(),
        })
    return sorted(results, key=lambda item: item["filepath"].lower())


def extract_pdf_preview(
    file_path: Path, *, max_pages: int = DEFAULT_MAX_PREVIEW_PAGES,
    min_characters: int = DEFAULT_MIN_PREVIEW_CHARACTERS,
    max_characters: int = DEFAULT_MAX_PREVIEW_CHARACTERS,
    start_page: int = 0,
) -> dict[str, Any]:
    """Adaptively sample useful PDF text without reading an entire technical book."""
    try:
        with file_path.open("rb") as source:
            reader = PdfReader(source)
            metadata = {
                str(key).lstrip("/"): str(value)
                for key, value in (reader.metadata or {}).items()
                if value is not None
            }
            page_count = len(reader.pages)
            text_parts, pages_sampled, toc_pages = [], [], []
            characters = 0
            limit = min(page_count, start_page + max(1, max_pages))
            for page_index in range(start_page, limit):
                extracted = (reader.pages[page_index].extract_text() or "").strip()
                pages_sampled.append(page_index + 1)
                if len(extracted) < MIN_USEFUL_PAGE_CHARACTERS:
                    continue
                if "table of contents" in extracted.lower() or "contents" in extracted.lower()[:300]:
                    toc_pages.append(page_index + 1)
                remaining = max_characters - characters
                if remaining <= 0:
                    break
                text_parts.append(extracted[:remaining])
                characters += min(len(extracted), remaining)
                # Always inspect the initial three pages; then stop as soon as a
                # useful representative sample is available.
                if page_index >= start_page + 2 and characters >= min_characters:
                    break
        text = "\n\n".join(text_parts).strip()
        pages_with_text = len(text_parts)
        quality = "good" if characters >= min_characters and pages_with_text >= 2 else "weak" if characters else "empty"
        return {
            "text": text,
            "metadata": metadata,
            "page_count": page_count,
            "pages_sampled": pages_sampled,
            "pages_with_text": pages_with_text,
            "characters": characters,
            "metadata_title_available": bool(metadata.get("Title")),
            "toc_pages": toc_pages,
            "extraction_quality": quality,
            "error": None,
        }
    except Exception as exc:
        return {
            "text": "", "metadata": {}, "page_count": 0, "pages_sampled": [],
            "pages_with_text": 0, "characters": 0, "metadata_title_available": False,
            "toc_pages": [], "extraction_quality": "empty",
            "error": f"{type(exc).__name__}: {exc}",
        }


def write_validation_report(file_path: Path, data_directory: Path, report: dict[str, Any]) -> Path:
    """Atomically persist a per-file Stage 2 result without overwriting another PDF's report."""
    report_path = validation_report_path(file_path, data_directory)
    payload = {
        **report,
        "stage2_completed": bool(report.get("stage2_completed", False)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    temporary_path = report_path.with_suffix(".tmp")
    temporary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary_path.replace(report_path)
    return report_path


def store_validated_pdf(file_path: Path, data_directory: Path, approved: bool) -> Path:
    """Move a Stage 2 result into the requested approved/rejected data folder."""
    destination_directory = validation_directories(data_directory)["approved" if approved else "rejected"]
    destination = destination_directory / file_path.name
    suffix = 1
    while destination.exists() and destination.resolve() != file_path.resolve():
        destination = destination_directory / f"{file_path.stem}_{suffix}{file_path.suffix}"
        suffix += 1
    if destination.resolve() != file_path.resolve():
        shutil.move(str(file_path), str(destination))
    return destination


def validation_summary(data_directory: Path) -> dict[str, int]:
    """Count persisted Stage 2 outcomes for compact Streamlit telemetry."""
    approved = rejected = pending = new_download_approved = existing_pdf_approved = 0
    reports_directory = validation_directories(data_directory)["reports"]
    for report_path in reports_directory.glob("*.json"):
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        status = str(report.get("status") or "").upper()
        if not status:
            status = "APPROVED" if report.get("approved") else "REJECTED" if report.get("stage2_completed") else "PENDING"
        if status == "APPROVED":
            approved += 1
            if report.get("source") == "new_download":
                new_download_approved += 1
            elif report.get("source") == "existing_pdf":
                existing_pdf_approved += 1
        elif status == "REJECTED":
            rejected += 1
        else:
            pending += 1
    return {
        "approved": approved,
        "rejected": rejected,
        "pending": pending,
        "new_download_approved": new_download_approved,
        "existing_pdf_approved": existing_pdf_approved,
    }
