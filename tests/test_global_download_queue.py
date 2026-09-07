"""Offline tests for the bounded global producer/consumer download queue."""

from __future__ import annotations

import threading
import time
import unittest

from app.pipeline_scheduler import BoundedWorkQueue, GlobalDownloadQueue, GlobalRecoveryBacklog


class GlobalDownloadQueueTests(unittest.TestCase):
    def test_bounded_queue_applies_backpressure(self):
        gate = threading.Event()

        def worker(candidate, worker_id):
            gate.wait(1)
            return {"success": True, "candidate": candidate}

        queue = GlobalDownloadQueue(worker, max_workers=1, maxsize=1)
        self.addCleanup(queue.close)
        self.assertTrue(queue.try_enqueue({"filename": "A.pdf"}))
        deadline = time.monotonic() + 1
        while queue.active == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(queue.try_enqueue({"filename": "B.pdf"}))
        self.assertFalse(queue.try_enqueue({"filename": "C.pdf"}))
        gate.set()

    def test_early_download_runs_before_producer_finishes(self):
        started = threading.Event()
        release = threading.Event()
        completed = []

        def worker(candidate, worker_id):
            started.set()
            release.wait(1)
            completed.append(candidate["filename"])
            return {"success": True, "candidate": candidate, "attempt": 1}

        queue = GlobalDownloadQueue(worker, max_workers=2, maxsize=4)
        self.addCleanup(queue.close)
        self.assertTrue(queue.try_enqueue({"filename": "page-1.pdf"}))
        self.assertTrue(started.wait(1), "page-1 worker did not start while producer remained active")
        # A later discovery result can still be admitted while page-1 transfers.
        self.assertTrue(queue.try_enqueue({"filename": "page-2.pdf"}))
        release.set()
        deadline = time.monotonic() + 2
        while not queue.idle and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(queue.idle)
        self.assertEqual({outcome.candidate["filename"] for outcome in queue.drain_outcomes()}, {"page-1.pdf", "page-2.pdf"})
        self.assertEqual(set(completed), {"page-1.pdf", "page-2.pdf"})

    def test_never_exceeds_fixed_worker_limit(self):
        active = maximum = 0
        lock = threading.Lock()

        def worker(candidate, worker_id):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.02)
            with lock:
                active -= 1
            return {"success": True, "candidate": candidate}

        queue = GlobalDownloadQueue(worker, max_workers=3, maxsize=12)
        self.addCleanup(queue.close)
        for index in range(12):
            self.assertTrue(queue.try_enqueue({"filename": f"{index}.pdf"}))
        deadline = time.monotonic() + 2
        while not queue.idle and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(queue.idle)
        self.assertLessEqual(maximum, 3)

    def test_cancelled_close_discards_jobs_that_never_started(self):
        started = threading.Event()
        release = threading.Event()
        completed = []

        def worker(candidate, worker_id):
            started.set()
            release.wait(1)
            completed.append(candidate["filename"])
            return {"success": True, "candidate": candidate}

        queue = GlobalDownloadQueue(worker, max_workers=1, maxsize=4)
        self.assertTrue(queue.try_enqueue({"filename": "active.pdf"}))
        self.assertTrue(started.wait(1))
        self.assertTrue(queue.try_enqueue({"filename": "never-started-1.pdf"}))
        self.assertTrue(queue.try_enqueue({"filename": "never-started-2.pdf"}))
        queue.close(cancel_pending=True, wait=False)
        release.set()
        deadline = time.monotonic() + 1
        while len(completed) < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(completed, ["active.pdf"])

    def test_bounded_stage2_handoff_never_executes_more_than_its_worker_limit(self):
        active = maximum = 0
        lock = threading.Lock()

        def validate(job, worker_id):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.02)
            with lock:
                active -= 1

        queue = BoundedWorkQueue(validate, max_workers=2, maxsize=4, thread_name_prefix="test-validation")
        self.addCleanup(queue.close)
        for index in range(4):
            self.assertTrue(queue.try_enqueue({"filename": f"{index}.pdf", "_queue_key": str(index)}, key=str(index)))
        self.assertFalse(queue.try_enqueue({"filename": "overflow.pdf", "_queue_key": "overflow"}, key="overflow"))
        deadline = time.monotonic() + 2
        while queue.pending and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertLessEqual(maximum, 2)


class GlobalRecoveryBacklogTests(unittest.TestCase):
    def test_failed_attempt_n_creates_only_n_plus_one(self):
        backlog = GlobalRecoveryBacklog()
        candidate = {"filename": "A.pdf", "document_id": "source-A", "_document_attempt": 2}
        job, terminal = backlog.add_failed_attempt({"candidate": candidate, "attempt": 2}, max_attempts=5)
        self.assertFalse(terminal)
        self.assertEqual(job["_document_attempt"], 3)
        self.assertEqual(backlog.take_round(max_attempts=5), [job])

    def test_terminal_attempt_never_produces_six_of_five(self):
        backlog = GlobalRecoveryBacklog()
        job, terminal = backlog.add_failed_attempt(
            {"candidate": {"filename": "A.pdf", "document_id": "source-A"}, "attempt": 5},
            max_attempts=5,
        )
        self.assertTrue(terminal)
        self.assertIsNone(job)
        self.assertFalse(backlog)

    def test_restored_downloading_job_keeps_its_existing_attempt(self):
        backlog = GlobalRecoveryBacklog()
        self.assertTrue(backlog.add_restored({
            "filename": "A.pdf", "document_id": "source-A", "_document_attempt": 3,
        }))
        self.assertEqual(backlog.take_round(max_attempts=5)[0]["_document_attempt"], 3)

    def test_duplicate_failure_is_present_once_at_the_highest_valid_attempt(self):
        backlog = GlobalRecoveryBacklog()
        one = {"filename": "A.pdf", "document_id": "source-A"}
        backlog.add_failed_attempt({"candidate": one, "attempt": 1}, max_attempts=5)
        backlog.add_failed_attempt({"candidate": one, "attempt": 2}, max_attempts=5)
        jobs = backlog.take_round(max_attempts=5)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["_document_attempt"], 3)


if __name__ == "__main__":
    unittest.main()
