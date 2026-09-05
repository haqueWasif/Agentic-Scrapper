"""Canonical, backward-compatible lifecycle views for document download records."""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import parse_qs, urlparse


def stable_document_id(candidate: dict[str, Any]) -> str:
    """Return a durable source identity without using a presentation filename.

    Libgen MD5 values are preferred when present.  For other providers, a hash
    of stable source metadata keeps one record associated with the same source
    across Streamlit reruns while retaining legacy filename-keyed state.
    """
    source_url = str(candidate.get("source_url") or candidate.get("url") or "").strip()
    mirrors = [str(item).strip() for item in candidate.get("mirrors", []) if str(item).strip()]
    for value in [source_url, *mirrors]:
        parsed = urlparse(value)
        query_values = parse_qs(parsed.query)
        values = query_values.get("md5", []) + query_values.get("MD5", []) + parsed.path.split("/")
        for item in values:
            normalized = str(item).strip().lower()
            if re.fullmatch(r"[a-f0-9]{32}", normalized):
                return f"libgen-md5:{normalized}"
    identity = "\n".join((
        source_url,
        str(candidate.get("title") or "").strip().casefold(),
        "\n".join(sorted(mirrors)),
    ))
    return "source-sha256:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def lifecycle_view(entry: dict[str, Any], *, status: str, candidate: dict[str, Any] | None = None,
                   validation_status: str | None = None) -> dict[str, Any]:
    """Build a normalized lifecycle view while keeping the flat legacy fields."""
    candidate = candidate or entry.get("candidate") or {}
    previous = entry.get("lifecycle") if isinstance(entry.get("lifecycle"), dict) else {}
    previous_discovery = previous.get("discovery") if isinstance(previous.get("discovery"), dict) else {}
    previous_stage1 = previous.get("stage1") if isinstance(previous.get("stage1"), dict) else {}
    previous_download = previous.get("download") if isinstance(previous.get("download"), dict) else {}
    previous_stage2 = previous.get("stage2") if isinstance(previous.get("stage2"), dict) else {}
    normalized = status.upper()
    stage1_status = candidate.get("stage1_status") or previous_stage1.get("status") or ("APPROVED" if candidate else "PENDING")
    stage2_status = validation_status or entry.get("validation_status") or previous_stage2.get("status") or "PENDING"
    final_status = (
        "ACCEPTED" if stage2_status == "APPROVED" else
        "REJECTED" if stage2_status == "REJECTED" else
        "STAGE1_REJECTED" if stage1_status == "REJECTED" else
        "DOWNLOAD_FAILED" if normalized == "PERMANENTLY_FAILED" else
        "IN_PROGRESS"
    )
    return {
        "discovery": {**previous_discovery, "status": previous_discovery.get("status", "DISCOVERED")},
        "stage1": {
            **previous_stage1,
            "status": stage1_status,
            "score": candidate.get("stage1_score", previous_stage1.get("score")),
            "completed": stage1_status in {"APPROVED", "REJECTED"},
        },
        "download": {
            **previous_download,
            "status": normalized,
            "document_attempt": int(entry.get("document_attempt", previous_download.get("document_attempt", 0)) or 0),
            "bytes_downloaded": int(entry.get("bytes_downloaded", previous_download.get("bytes_downloaded", 0)) or 0),
            "part_path": entry.get("part_path", previous_download.get("part_path")),
            "last_error": entry.get("last_error", previous_download.get("last_error")),
        },
        "stage2": {
            **previous_stage2,
            "status": str(stage2_status).upper(),
            "completed": str(stage2_status).upper() in {"APPROVED", "REJECTED"},
        },
        "final_status": final_status,
    }
