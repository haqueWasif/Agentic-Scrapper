"""Ensure byte-identical PDFs bypass duplicate Stage 2 processing."""

import ast
import logging
from pathlib import Path
import tempfile
import threading
import unittest
from typing import Any


class Stage2DedupHandoffTests(unittest.TestCase):
    def _run_job(self, hash_result):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        node = next(
            node for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef) and node.name == "_run_validation_job"
        )
        updates, calls, propagations = [], [], []
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "duplicate.pdf"
            destination.write_bytes(b"%PDF")
            namespace = {
                "Any": Any, "threading": threading, "logging": logging,
                "VALIDATION_TIMEOUT_SECONDS": 60,
                "_download_paths": lambda filename: (filename, destination, destination.with_suffix(".part")),
                "_download_size": lambda filename: destination.stat().st_size,
                "_update_download_state": lambda *args, **kwargs: updates.append((args, kwargs)),
                "_record_completed_content_hash": lambda filename: hash_result,
                "_validate_pdf_stage2": lambda *args, **kwargs: calls.append((args, kwargs)) or {"status": "APPROVED"},
                "_propagate_duplicate_validation": lambda *args: propagations.append(args),
            }
            exec(compile(ast.Module(body=[node], type_ignores=[]), "app.py", "exec"), namespace)
            namespace["_run_validation_job"]({"filename": "duplicate.pdf", "stage1_score": 88}, 1)
        return updates, calls, propagations

    def test_duplicate_with_pending_canonical_skips_pypdf_and_crewai(self):
        updates, calls, propagations = self._run_job({
            "sha256": "same", "duplicate_of": "libgen-md5:canonical",
            "canonical_filename": "canonical.pdf", "canonical_validation_status": "RUNNING",
        })
        self.assertEqual(calls, [])
        self.assertEqual(propagations, [])
        self.assertTrue(any(kwargs.get("validation_status") == "DUPLICATE_PENDING" for _, kwargs in updates))

    def test_duplicate_with_terminal_canonical_inherits_result_without_evaluation(self):
        updates, calls, propagations = self._run_job({
            "sha256": "same", "duplicate_of": "libgen-md5:canonical",
            "canonical_filename": "canonical.pdf", "canonical_validation_status": "APPROVED",
        })
        self.assertEqual(calls, [])
        self.assertEqual(propagations, [])
        self.assertTrue(any(kwargs.get("validation_status") == "APPROVED" for _, kwargs in updates))

    def test_unique_content_runs_validation_and_propagates_terminal_result(self):
        updates, calls, propagations = self._run_job({
            "sha256": "unique", "duplicate_of": None,
            "canonical_filename": None, "canonical_validation_status": None,
        })
        self.assertEqual(len(calls), 1)
        self.assertEqual(propagations, [("duplicate.pdf", "APPROVED")])


if __name__ == "__main__":
    unittest.main()
