"""Windows-safe atomic ledger tests without starting Streamlit."""

import ast
from contextlib import contextmanager
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from typing import Any
from unittest.mock import patch


class DownloadStateWriterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        wanted = {
            "_download_paths", "_load_download_state", "_save_download_state", "_download_state_process_lock",
            "_update_download_state", "_download_state_entry", "_download_size",
        }
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in wanted]
        self.assertEqual({node.name for node in nodes}, wanted)
        self.ns = {
            "Any": Any, "Path": Path, "json": json, "logging": logging, "os": os,
            "tempfile": tempfile, "threading": threading, "time": time, "contextmanager": contextmanager,
            "DOWNLOAD_DIRECTORY": root / "ASHRAE_Files",
            "DOWNLOAD_STATE_FILE": root / "downloads.json",
            "_DOWNLOAD_STATE_LOCK": threading.Lock(),
        }
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), self.ns)

    def update(self, filename, status="QUEUED"):
        return self.ns["_update_download_state"](
            filename, f"https://example.test/{filename}", status,
            size=123, candidate={"filename": filename, "mirrors": ["https://example.test/mirror"]},
        )

    def test_many_threads_leave_valid_json_and_every_record(self):
        def worker(worker_id):
            for iteration in range(12):
                self.assertTrue(self.update(f"worker-{worker_id}-{iteration}.pdf", "DOWNLOADING"))

        threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        state = json.loads(self.ns["DOWNLOAD_STATE_FILE"].read_text(encoding="utf-8"))
        self.assertEqual(len(state["downloads"]), 60)
        self.assertTrue(all(entry["status"] == "DOWNLOADING" for entry in state["downloads"].values()))

    def test_same_document_updates_are_serialized(self):
        statuses = ["QUEUED", "DOWNLOADING", "FAILED_FOR_ROUND", "RECOVERING", "COMPLETED"]

        def worker():
            for status in statuses:
                self.assertTrue(self.update("same.pdf", status))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        state = json.loads(self.ns["DOWNLOAD_STATE_FILE"].read_text(encoding="utf-8"))
        self.assertIn(state["downloads"]["same.pdf"]["status"], statuses)

    def test_permission_error_retries_and_then_succeeds(self):
        original_replace = os.replace
        calls = 0

        def flaky_replace(source, destination):
            nonlocal calls
            calls += 1
            if calls <= 2:
                raise PermissionError(13, "Access is denied")
            return original_replace(source, destination)

        with patch.object(os, "replace", side_effect=flaky_replace):
            self.assertTrue(self.update("retry.pdf"))
        self.assertEqual(calls, 3)
        self.assertEqual(self.ns["_download_state_entry"]("retry.pdf")["status"], "QUEUED")

    def test_exhausted_replace_failure_keeps_old_json_and_cleans_own_temp(self):
        self.assertTrue(self.update("existing.pdf", "COMPLETED"))
        before = self.ns["DOWNLOAD_STATE_FILE"].read_text(encoding="utf-8")
        with patch.object(os, "replace", side_effect=PermissionError(13, "Access is denied")):
            self.assertFalse(self.update("not-persisted.pdf", "QUEUED"))
        self.assertEqual(self.ns["DOWNLOAD_STATE_FILE"].read_text(encoding="utf-8"), before)
        self.assertEqual(list(self.ns["DOWNLOAD_STATE_FILE"].parent.glob("downloads_*.tmp")), [])

    def test_atomic_replacement_preserves_existing_records(self):
        self.assertTrue(self.update("first.pdf", "COMPLETED"))
        self.assertTrue(self.update("second.pdf", "QUEUED"))
        state = json.loads(self.ns["DOWNLOAD_STATE_FILE"].read_text(encoding="utf-8"))
        self.assertEqual(set(state["downloads"]), {"first.pdf", "second.pdf"})


if __name__ == "__main__":
    unittest.main()
