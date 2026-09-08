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
        calls = []
        with patch.dict(os.environ, {"LANGSMITH_TRACING": "false", "LANGSMITH_API_KEY": ""}, clear=False):
            self.assertFalse(langsmith_settings().enabled)
            self.assertEqual(langsmith_status(), "Disabled")
            self.assertEqual(
                trace_operation("test", inputs={"count": 1}, operation=lambda: calls.append(True) or "core-result", summarize_output=lambda value: {"result": value}),
                "core-result",
            )
        self.assertEqual(calls, [True])

    def test_enabled_tracing_creates_bounded_span(self):
        events = []
        operation_calls = []

        class Client:
            def __init__(self, **kwargs):
                events.append(("client", kwargs))

        class FakeRun:
            def end(self, **kwargs):
                events.append(("end", kwargs))

        class FakeTraceContext:
            def __enter__(self):
                events.append(("enter",))
                return FakeRun()

            def __exit__(self, *args):
                events.append(("exit",))

        def trace(*args, **kwargs):
            events.append(("trace", args, kwargs))
            return FakeTraceContext()

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
                    inputs={"document_count": 25}, operation=lambda: operation_calls.append(True) or {"approved": 4},
                    summarize_output=lambda value: {"approved_document_count": value["approved"]},
                )
        self.assertEqual(result, {"approved": 4})
        trace_event = next(event for event in events if event[0] == "trace")
        self.assertEqual(trace_event[1][0], "ASHRAE-Stage1-Metadata-Evaluation")
        self.assertEqual(trace_event[2]["inputs"], {"document_count": 25})
        self.assertEqual(events[-2][0], "end")
        self.assertEqual(events[-2][1]["outputs"]["approved_document_count"], 4)
        self.assertEqual(operation_calls, [True])

    def test_status_reports_enabled_and_missing_key_without_exposure(self):
        with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key"}, clear=False):
            self.assertEqual(langsmith_status(), "Enabled")
        with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": ""}, clear=False):
            self.assertEqual(langsmith_status(), "Disabled — API key not configured")

    def test_observability_setup_failure_does_not_block_core_operation(self):
        calls = []
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
                    trace_operation("test", inputs={}, operation=lambda: calls.append(True) or "still-runs", summarize_output=lambda value: {}),
                    "still-runs",
                )
        self.assertEqual(calls, [True])

    def test_finalization_failure_returns_result_without_repeating_operation(self):
        calls = []

        class Client:
            def __init__(self, **kwargs):
                pass

        class Run:
            def end(self, **kwargs):
                raise RuntimeError("trace backend unavailable")

        class TraceContext:
            def __enter__(self):
                return Run()

            def __exit__(self, *args):
                return False

        fake_langsmith = types.ModuleType("langsmith")
        fake_langsmith.Client = Client
        fake_helpers = types.ModuleType("langsmith.run_helpers")
        fake_helpers.trace = lambda *args, **kwargs: TraceContext()
        with patch.dict(sys.modules, {"langsmith": fake_langsmith, "langsmith.run_helpers": fake_helpers}):
            with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key"}, clear=False):
                self.assertEqual(
                    trace_operation("test", inputs={}, operation=lambda: calls.append(True) or "core-result", summarize_output=lambda value: {}),
                    "core-result",
                )
        self.assertEqual(calls, [True])

    def test_operation_exception_is_preserved_when_tracing_is_enabled(self):
        class Client:
            def __init__(self, **kwargs):
                pass

        class Run:
            def end(self, **kwargs):
                pass

        class TraceContext:
            def __enter__(self):
                return Run()

            def __exit__(self, *args):
                return False

        fake_langsmith = types.ModuleType("langsmith")
        fake_langsmith.Client = Client
        fake_helpers = types.ModuleType("langsmith.run_helpers")
        fake_helpers.trace = lambda *args, **kwargs: TraceContext()
        with patch.dict(sys.modules, {"langsmith": fake_langsmith, "langsmith.run_helpers": fake_helpers}):
            with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key"}, clear=False):
                with self.assertRaisesRegex(ValueError, "original core error"):
                    trace_operation(
                        "test", inputs={}, operation=lambda: (_ for _ in ()).throw(ValueError("original core error")),
                        summarize_output=lambda value: {},
                    )

    def test_stage2_trace_name_finalizes_on_the_entered_run(self):
        names, finalized = [], []

        class Client:
            def __init__(self, **kwargs):
                pass

        class Run:
            def end(self, **kwargs):
                finalized.append(kwargs)

        class TraceContext:
            def __enter__(self):
                return Run()

            def __exit__(self, *args):
                return False

        fake_langsmith = types.ModuleType("langsmith")
        fake_langsmith.Client = Client
        fake_helpers = types.ModuleType("langsmith.run_helpers")
        fake_helpers.trace = lambda name, **kwargs: names.append(name) or TraceContext()
        with patch.dict(sys.modules, {"langsmith": fake_langsmith, "langsmith.run_helpers": fake_helpers}):
            with patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key"}, clear=False):
                self.assertEqual(
                    trace_operation(
                        "ASHRAE-Stage2-PDF-Validation", inputs={"filename": "bounded.pdf"},
                        operation=lambda: {"status": "APPROVED"}, summarize_output=lambda value: value,
                    ),
                    {"status": "APPROVED"},
                )
        self.assertEqual(names, ["ASHRAE-Stage2-PDF-Validation"])
        self.assertEqual(finalized[0]["outputs"]["status"], "APPROVED")


if __name__ == "__main__":
    unittest.main()
