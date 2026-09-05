"""Optional, bounded LangSmith observability for existing agent workflows."""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar


T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LangSmithSettings:
    enabled: bool
    project: str
    workspace_id: str | None
    api_key: str | None


def langsmith_settings() -> LangSmithSettings:
    """Read optional tracing configuration without exposing the API key."""
    requested = os.getenv("LANGSMITH_TRACING", "false").strip().lower() == "true"
    api_key = os.getenv("LANGSMITH_API_KEY", "").strip() or None
    return LangSmithSettings(
        enabled=requested and bool(api_key),
        project=os.getenv("LANGSMITH_PROJECT", "ashrae-intelligent-scraper").strip() or "ashrae-intelligent-scraper",
        workspace_id=os.getenv("LANGSMITH_WORKSPACE_ID", "").strip() or None,
        api_key=api_key,
    )


def langsmith_status() -> str:
    """Return a UI-safe status label that never includes credentials."""
    settings = langsmith_settings()
    if settings.enabled:
        return "Enabled"
    if os.getenv("LANGSMITH_TRACING", "false").strip().lower() == "true" and not settings.api_key:
        return "Disabled — API key not configured"
    return "Disabled"


def trace_operation(
    name: str,
    *,
    inputs: dict[str, Any],
    operation: Callable[[], T],
    summarize_output: Callable[[T], dict[str, Any]],
) -> T:
    """Run an existing operation while isolating all observability failures.

    Inputs and summaries are provided by the caller and intentionally contain
    only bounded metadata.  PDF bytes, document text, URLs, and credentials are
    never accepted here by the agent workflows.
    """
    settings = langsmith_settings()
    if not settings.enabled:
        return operation()

    span = None
    try:
        from langsmith import Client
        from langsmith.run_helpers import trace

        client = Client(
            api_key=settings.api_key,
            workspace_id=settings.workspace_id,
            auto_batch_tracing=True,
        )
        span = trace(
            name,
            run_type="chain",
            inputs=inputs,
            project_name=settings.project,
            tags=["ashrae-scraper", "crewai", "openrouter"],
            # Core exceptions must not result in trace payloads containing a
            # provider message that could expose credentials or signed URLs.
            exceptions_to_handle=(Exception,),
            client=client,
        )
        span.__enter__()
    except Exception as exc:
        _LOGGER.warning("LangSmith tracing unavailable for %s: %s", name, type(exc).__name__)
        return operation()

    started = time.monotonic()
    try:
        result = operation()
    except BaseException as exc:
        try:
            span.end(outputs={
                "error_status": True,
                "error_type": type(exc).__name__,
                "duration_seconds": round(time.monotonic() - started, 3),
            })
        except Exception as trace_exc:
            _LOGGER.warning("LangSmith trace finalization unavailable for %s: %s", name, type(trace_exc).__name__)
        raise
    else:
        try:
            outputs = dict(summarize_output(result))
            outputs["duration_seconds"] = round(time.monotonic() - started, 3)
            span.end(outputs=outputs)
        except Exception as trace_exc:
            _LOGGER.warning("LangSmith trace finalization unavailable for %s: %s", name, type(trace_exc).__name__)
        return result
    finally:
        try:
            span.__exit__(*sys.exc_info())
        except Exception as trace_exc:
            _LOGGER.warning("LangSmith trace close unavailable for %s: %s", name, type(trace_exc).__name__)
