"""Optional LangSmith tracing must never become scraper control flow."""

import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.observability import langsmith_settings, langsmith_status, trace_operation


class ObservabilityTests(unittest.TestCase):
    def test_disabled_tracing_runs_operation_without_loading_langsmith(self):
        with patch.dict(os.environ, {"LANGSMITH_TRACING": "false", "LANGSMITH_API_KEY": ""}, clear=False):
            self.assertFalse(langsmith_settings().enabled)
            self.assertEqual(langsmith_status(), "Disabled")
            self.assertEqual(
                trace_operation("test", inputs={"count": 1}, operation=lambda: "core-result", summarize_output=lambda value: {"result": value}),
                "core-result",
            )

    def test_enabled_tracing_creates_bounded_span(self):
        events = []

        class Client:
            def __init__(self, **kwargs):
                events.append(("client", kwargs))

        class Span:
            def __enter__(self):
                events.append(("enter",))
                return self

            def end(self, **kwargs):
                events.append(("end", kwargs))

            def __exit__(self, *args):
                events.append(("exit",))

        def trace(*args, **kwargs):
            events.append(("trace", args, kwargs))
            return Span()

        fake_langsmith = types.ModuleType("langsmith")
        fake_langsmith.Client = Client
        fake_helpers = types.ModuleType("langsmith.run_helpers")
        fake_helpers.trace = trace
        with patch.dict(sys.modules, {"langsmith": fake_langsmith, "langsmith.run_helpers": fake_helpers}):
            with patch.dict(os.environ, {
                "LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key",
                "LANGSMITH_PROJECT": "test-project", "LANGSMITH_WORKSPACE_ID": "workspace-1",
            }, clear=False):
                result = trace_operation(
                    "ASHRAE-Stage1-Metadata-Evaluation",
                    inputs={"document_count": 25}, operation=lambda: {"approved": 4},
                    summarize_output=lambda value: {"approved_document_count": value["approved"]},
                )
        self.assertEqual(result, {"approved": 4})
        trace_event = next(event for event in events if event[0] == "trace")
        self.assertEqual(trace_event[1][0], "ASHRAE-Stage1-Metadata-Evaluation")
        self.assertEqual(trace_event[2]["inputs"], {"document_count": 25})
        self.assertEqual(events[-2][0], "end")
        self.assertEqual(events[-2][1]["outputs"]["approved_document_count"], 4)

    def test_status_reports_enabled_and_missing_key_without_exposure(self):
        with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key"}, clear=False):
            self.assertEqual(langsmith_status(), "Enabled")
        with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": ""}, clear=False):
            self.assertEqual(langsmith_status(), "Disabled — API key not configured")

    def test_observability_setup_failure_does_not_block_core_operation(self):
        class BrokenClient:
            def __init__(self, **kwargs):
                raise RuntimeError("LangSmith unavailable")

        fake_langsmith = types.ModuleType("langsmith")
        fake_langsmith.Client = BrokenClient
        fake_helpers = types.ModuleType("langsmith.run_helpers")
        fake_helpers.trace = lambda *args, **kwargs: None
        with patch.dict(sys.modules, {"langsmith": fake_langsmith, "langsmith.run_helpers": fake_helpers}):
            with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key"}, clear=False):
                self.assertEqual(
                    trace_operation("test", inputs={}, operation=lambda: "still-runs", summarize_output=lambda value: {}),
                    "still-runs",
                )


if __name__ == "__main__":
    unittest.main()
