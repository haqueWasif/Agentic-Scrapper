"""Offline checks for bounded document-level recovery rounds.

These tests deliberately exercise the scheduler independently of Streamlit and
the network.  The existing sync downloader tests cover the curl_cffi Range
transfer itself; this file covers how completed/partial attempts are sequenced.
"""

import ast
import asyncio
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext
import json
from pathlib import Path
from queue import Empty, Queue
import tempfile
import threading
import time
import traceback
import unittest
from types import SimpleNamespace
from typing import Any


class _Progress:
    def progress(self, *args, **kwargs):
        pass

    def empty(self):
        pass


class _Status:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def write(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass


class _Streamlit:
    def status(self, *args, **kwargs):
        return _Status()

    def empty(self):
        return _Progress()

    def caption(self, *args, **kwargs):
        pass


async def _no_sleep(seconds):
    pass


class DownloadRoundSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        nodes = [
            node for node in ast.parse(source).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_download_concurrent_cards"
        ]
        self.assertEqual(len(nodes), 1)
        self.calls = []
        self.states = []
        self.settings = SimpleNamespace(
            max_workers=5, max_document_attempts=5, recovery_round_delay_seconds=0,
        )
        self.network = SimpleNamespace(settings=self.settings, events=Queue())

        def update(filename, url, status, **kwargs):
            self.states.append((filename, status, kwargs))

        namespace = {
            "Any": Any,
            "asyncio": SimpleNamespace(sleep=_no_sleep),
            "Empty": Empty,
            "FIRST_COMPLETED": FIRST_COMPLETED,
            "ThreadPoolExecutor": ThreadPoolExecutor,
            "wait": wait,
            "nullcontext": nullcontext,
            "st": _Streamlit(),
            "time": time,
            "traceback": traceback,
            "_download_size": lambda filename: 10 * 1024 * 1024 if filename.startswith("A") else 0,
            "_update_download_state": update,
        }
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), namespace)
        self.scheduler = namespace["_download_concurrent_cards"]

    def _install_worker(self, outcome):
        def worker(candidate, network, worker_id):
            attempt = int(candidate.get("_document_attempt", 1))
            self.calls.append((candidate["filename"], attempt))
            success = outcome(candidate["filename"], attempt)
            return {
                "success": success,
                "candidate": candidate,
                "attempt": attempt,
                "permanent": not success and attempt >= network.settings.max_document_attempts,
            }

        self.scheduler.__globals__["_download_worker"] = worker

    @staticmethod
    def _jobs(names):
        return [
            {"filename": name, "mirrors": [f"https://example.test/{name}"], "source_url": "https://example.test/book"}
            for name in names
        ]

    async def test_five_workers_complete_ten_documents_in_one_round(self):
        self._install_worker(lambda filename, attempt: True)
        completed, failed = await self.scheduler(self._jobs([f"doc-{index}.pdf" for index in range(10)]), self.network, lambda message: None)
        self.assertEqual(completed, 10)
        self.assertEqual(failed, [])
        self.assertEqual(set(self.calls), {(f"doc-{index}.pdf", 1) for index in range(10)})

    async def test_partial_failure_waits_for_round_then_recovers(self):
        self._install_worker(lambda filename, attempt: filename != "A.pdf" or attempt == 2)
        completed, _ = await self.scheduler(self._jobs(["A.pdf", "B.pdf", "C.pdf"]), self.network, lambda message: None)
        self.assertEqual(completed, 3)
        self.assertEqual(self.calls[:3], [("A.pdf", 1), ("B.pdf", 1), ("C.pdf", 1)])
        self.assertEqual(self.calls[3:], [("A.pdf", 2)])
        self.assertTrue(any(filename == "A.pdf" and status == "FAILED_FOR_ROUND" for filename, status, _ in self.states))

    async def test_repeated_failures_stop_at_configured_document_limit(self):
        self._install_worker(lambda filename, attempt: False)
        completed, _ = await self.scheduler(self._jobs(["A.pdf"]), self.network, lambda message: None)
        self.assertEqual(completed, 0)
        self.assertEqual(self.calls, [("A.pdf", attempt) for attempt in range(1, 6)])
        self.assertTrue(any(filename == "A.pdf" and status == "PERMANENTLY_FAILED" for filename, status, _ in self.states))

    async def test_slow_but_successful_worker_is_not_cut_off_by_scheduler(self):
        def slow_success(filename, attempt):
            time.sleep(0.05)
            return True

        self._install_worker(slow_success)
        completed, _ = await self.scheduler(self._jobs(["slow.pdf"]), self.network, lambda message: None)
        self.assertEqual(completed, 1)
        self.assertEqual(self.calls, [("slow.pdf", 1)])


class PersistentRecoveryStateTests(unittest.TestCase):
    def test_stale_downloading_state_becomes_recoverable_with_part_preserved(self):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        wanted = {"_download_paths", "_load_download_state", "_save_download_state", "_recovery_candidates"}
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in wanted]
        self.assertEqual({node.name for node in nodes}, wanted)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            namespace = {
                "Any": Any, "Path": Path, "json": json, "os": __import__("os"), "time": time,
                "threading": threading, "logging": __import__("logging"),
                "DOWNLOAD_DIRECTORY": root / "ASHRAE_Files", "DOWNLOAD_STATE_FILE": root / "downloads.json",
                "_DOWNLOAD_STATE_LOCK": threading.Lock(),
            }
            exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), namespace)
            _, _, part = namespace["_download_paths"]("A.pdf")
            part.write_bytes(b"partial bytes")
            namespace["_save_download_state"]({"downloads": {
                "A.pdf": {
                    "filename": "A.pdf", "status": "DOWNLOADING", "document_attempt": 1,
                    "candidate": {"filename": "A.pdf", "mirrors": ["https://example.test/A"], "query": "ASHRAE"},
                    "query": "ASHRAE",
                },
            }})
            jobs = namespace["_recovery_candidates"]("ASHRAE")
            self.assertEqual(jobs[0]["_document_attempt"], 2)
            self.assertEqual(part.read_bytes(), b"partial bytes")
            persisted = namespace["_load_download_state"]()["downloads"]["A.pdf"]
            self.assertEqual(persisted["status"], "FAILED_FOR_ROUND")


class DownloadWorkerHandoffTests(unittest.TestCase):
    def test_successful_transfer_marks_completed_and_enqueues_stage2_once(self):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        nodes = [
            node for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef) and node.name == "_download_worker"
        ]
        self.assertEqual(len(nodes), 1)
        updates, validation_jobs = [], []
        settings = SimpleNamespace(max_document_attempts=5)

        class Network:
            def begin_download(self, worker_id, filename):
                return None

            def emit(self, *args, **kwargs):
                pass

        Network.settings = settings

        namespace = {
            "Any": Any, "NetworkManager": Network,
            "_download_size": lambda filename: 4096,
            "_update_download_state": lambda *args, **kwargs: updates.append((args, kwargs)),
            "_download_state_entry": lambda filename: {},
            "_schedule_validation": lambda filename, stage1_score: validation_jobs.append((filename, stage1_score)),
            "parse_mirror_and_download": lambda *args, **kwargs: True,
        }
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), namespace)
        result = namespace["_download_worker"](
            {"filename": "A.pdf", "mirrors": ["https://example.test/mirror"], "source_url": "https://example.test/source", "stage1_score": 91},
            Network(), 1,
        )
        self.assertTrue(result["success"])
        self.assertEqual(validation_jobs, [("A.pdf", 91)])
        self.assertTrue(any(args[2] == "COMPLETED" for args, kwargs in updates))


if __name__ == "__main__":
    unittest.main()
