"""Offline checks for canonical identity and lifecycle compatibility."""

import ast
from contextlib import contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from typing import Any

from app.pipeline_state import lifecycle_view, stable_document_id


class PipelineStateTests(unittest.TestCase):
    def test_libgen_md5_is_stable_across_mirror_routes(self):
        first = stable_document_id({
            "source_url": "https://libgen.bz/ads.php?md5=0123456789abcdef0123456789abcdef",
            "mirrors": ["https://library.lol/main/0123456789abcdef0123456789abcdef"],
            "title": "ASHRAE Handbook",
        })
        second = stable_document_id({
            "source_url": "https://library.lol/main/0123456789abcdef0123456789abcdef",
            "mirrors": ["https://libgen.bz/get.php?md5=0123456789abcdef0123456789abcdef"],
            "title": "Different presentation title",
        })
        self.assertEqual(first, "libgen-md5:0123456789abcdef0123456789abcdef")
        self.assertEqual(first, second)

    def test_legacy_download_record_gains_a_lifecycle_without_losing_flat_fields(self):
        legacy = {
            "filename": "legacy.pdf", "status": "DOWNLOADING", "bytes_downloaded": 2048,
            "document_attempt": 2, "part_path": "legacy.pdf.part", "last_error": "reset",
        }
        lifecycle = lifecycle_view(legacy, status="DOWNLOADING", candidate={
            "title": "ASHRAE", "stage1_score": 91,
        })
        self.assertEqual(legacy["filename"], "legacy.pdf")
        self.assertEqual(lifecycle["discovery"]["status"], "DISCOVERED")
        self.assertEqual(lifecycle["stage1"]["status"], "APPROVED")
        self.assertEqual(lifecycle["download"]["document_attempt"], 2)
        self.assertEqual(lifecycle["download"]["bytes_downloaded"], 2048)
        self.assertEqual(lifecycle["stage2"]["status"], "PENDING")
        self.assertEqual(lifecycle["final_status"], "IN_PROGRESS")

    def test_completed_stage2_decision_sets_terminal_status(self):
        lifecycle = lifecycle_view(
            {"bytes_downloaded": 8192, "document_attempt": 1}, status="COMPLETED",
            candidate={"title": "ASHRAE"}, validation_status="APPROVED",
        )
        self.assertTrue(lifecycle["stage2"]["completed"])
        self.assertEqual(lifecycle["final_status"], "ACCEPTED")

    def test_identical_completed_content_is_linked_without_deleting_either_pdf(self):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        wanted = {
            "_download_paths", "_load_download_state", "_download_state_process_lock",
            "_save_download_state", "_pdf_sha256", "_record_completed_content_hash",
        }
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in wanted]
        self.assertEqual({node.name for node in nodes}, wanted)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            namespace = {
                "Path": Path, "Any": object, "hashlib": hashlib, "json": json,
                "logging": logging, "os": os, "tempfile": tempfile, "threading": threading,
                "time": time, "contextmanager": contextmanager,
                "DOWNLOAD_DIRECTORY": root / "ASHRAE_Files",
                "DOWNLOAD_STATE_FILE": root / "downloads.json",
                "_DOWNLOAD_STATE_LOCK": threading.Lock(),
            }
            exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), namespace)
            for filename, document_id in (("A.pdf", "first"), ("B.pdf", "second")):
                _, path, _ = namespace["_download_paths"](filename)
                path.write_bytes(b"same pdf bytes")
                state = namespace["_load_download_state"]()
                state["downloads"][filename] = {
                    "filename": filename, "status": "COMPLETED", "document_id": document_id,
                }
                namespace["_save_download_state"](state)
                namespace["_record_completed_content_hash"](filename)
            state = namespace["_load_download_state"]()["downloads"]
            self.assertEqual(state["A.pdf"]["content_sha256"], state["B.pdf"]["content_sha256"])
            self.assertEqual(state["B.pdf"]["duplicate_of"], "first")
            self.assertTrue((root / "ASHRAE_Files" / "A.pdf").exists())
            self.assertTrue((root / "ASHRAE_Files" / "B.pdf").exists())

    def test_terminal_canonical_result_is_propagated_without_revalidating_duplicate(self):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        wanted = {
            "_download_paths", "_load_download_state", "_save_download_state", "_download_state_process_lock",
            "_update_download_state", "_download_state_entry", "_download_size", "_propagate_duplicate_validation",
        }
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in wanted]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            namespace = {
                "Any": Any, "Path": Path, "hashlib": hashlib, "json": json, "logging": logging,
                "os": os, "tempfile": tempfile, "threading": threading, "time": time,
                "contextmanager": contextmanager, "DOWNLOAD_DIRECTORY": root / "ASHRAE_Files",
                "DOWNLOAD_STATE_FILE": root / "downloads.json", "_DOWNLOAD_STATE_LOCK": threading.Lock(),
            }
            exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), namespace)
            for filename in ("canonical.pdf", "duplicate.pdf"):
                _, path, _ = namespace["_download_paths"](filename)
                path.write_bytes(b"%PDF")
            namespace["_save_download_state"]({"downloads": {
                "canonical.pdf": {
                    "filename": "canonical.pdf", "status": "COMPLETED", "document_id": "canonical-id",
                    "validation_status": "APPROVED",
                },
                "duplicate.pdf": {
                    "filename": "duplicate.pdf", "status": "COMPLETED", "document_id": "duplicate-id",
                    "duplicate_of": "canonical-id", "validation_status": "DUPLICATE_PENDING",
                },
            }})
            self.assertEqual(namespace["_propagate_duplicate_validation"]("canonical.pdf", "APPROVED"), 1)
            state = namespace["_load_download_state"]()["downloads"]
            self.assertEqual(state["duplicate.pdf"]["validation_status"], "APPROVED")


if __name__ == "__main__":
    unittest.main()
