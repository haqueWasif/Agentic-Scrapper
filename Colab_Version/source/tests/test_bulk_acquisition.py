"""Offline checks for bulk state, skipped completed files, and partial resumption."""

import ast
from contextlib import contextmanager
import json
import os
import re
import shutil
import tempfile
import threading
import time
import unittest
import urllib.parse
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse


class BulkAcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        names = {
            "_safe_pdf_filename", "_document_mirror_links", "_download_paths",
            "_load_download_state", "_save_download_state", "_update_download_state",
            "_download_state_entry", "_download_size", "_download_state_process_lock", "_bulk_keyword_match",
            "_bulk_candidates_from_page",
        }
        nodes = [
            node for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef) and node.name in names
        ]
        self.assertEqual({node.name for node in nodes}, names)
        self.ns = {
            "Any": Any, "Path": Path, "os": os, "re": re, "json": json,
            "threading": threading, "time": time, "tempfile": tempfile, "contextmanager": contextmanager, "logging": __import__("logging"), "urllib": urllib,
            "urljoin": urljoin, "urlparse": urlparse,
            "PROJECT_ROOT": root,
            "DOWNLOAD_DIRECTORY": root / "ASHRAE_Files",
            "DOWNLOAD_STATE_FILE": root / "downloads.json",
            "_DOWNLOAD_STATE_LOCK": threading.Lock(),
            "QUERY_TERMS": ["ashrae", "hvac", "chiller"],
        }
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), self.ns)

    def test_target_50_skips_completed_and_keeps_partial_for_range_resume(self):
        documents = [
            {"title": f"ASHRAE HVAC Handbook {index}", "link": f"https://libgen.bz/ads.php?md5={'a' * 31}{index % 10}", "text": "ASHRAE HVAC"}
            for index in range(1, 51)
        ]
        rows = [{"links": [document["link"]], "mirror_links": [document["link"]]} for document in documents]
        completed_filename = self.ns["_safe_pdf_filename"](documents[0]["title"], 1, 1)
        partial_filename = self.ns["_safe_pdf_filename"](documents[1]["title"], 1, 2)
        _, completed_path, _ = self.ns["_download_paths"](completed_filename)
        _, _, partial_path = self.ns["_download_paths"](partial_filename)
        completed_path.write_bytes(b"%PDF- completed")
        partial_path.write_bytes(b"x" * 4096)
        self.ns["_update_download_state"](completed_filename, documents[0]["link"], "completed", size=completed_path.stat().st_size)
        self.ns["_update_download_state"](partial_filename, documents[1]["link"], "partial", size=partial_path.stat().st_size)

        candidates, skipped = self.ns["_bulk_candidates_from_page"](
            documents, rows, "https://libgen.bz/index.php", "ASHRAE HVAC", 50, 1,
        )
        names = {candidate["filename"] for candidate in candidates}
        self.assertEqual(skipped, 1)
        self.assertEqual(len(candidates), 49)
        self.assertIn(partial_filename, names)
        self.assertNotIn(completed_filename, names)
        self.assertEqual(self.ns["_download_size"](partial_filename), 4096)
        self.assertEqual(self.ns["_download_state_entry"](partial_filename)["status"], "partial")


if __name__ == "__main__":
    unittest.main()
