import unittest

from app.pipeline_events import PipelineEventBus, RunMetrics


class PipelineEventBusTests(unittest.TestCase):
    def test_drain_isolates_runs_and_keeps_deferred_events(self):
        bus = PipelineEventBus()
        bus.emit("DOCUMENT_DISCOVERED", run_id="run-a", count=2)
        bus.emit("DOCUMENT_DISCOVERED", run_id="run-b", count=3)

        self.assertEqual([event["count"] for event in bus.drain("run-a")], [2])
        self.assertEqual([event["count"] for event in bus.drain("run-b")], [3])


class RunMetricsTests(unittest.TestCase):
    def test_metrics_follow_one_document_through_stage2(self):
        metrics = RunMetrics(existing_downloads=2)
        events = [
            {"kind": "DOCUMENT_DISCOVERED", "filename": "one.pdf", "count": 25},
            {"kind": "STAGE1_APPROVED", "filename": "one.pdf", "count": 4},
            {"kind": "STAGE1_REJECTED", "filename": "one.pdf", "count": 21},
            {"kind": "DOWNLOAD_QUEUED", "filename": "one.pdf"},
            {"kind": "DOWNLOAD_STARTED", "filename": "one.pdf"},
            {"kind": "DOWNLOAD_COMPLETED", "filename": "one.pdf"},
            {"kind": "STAGE2_QUEUED", "filename": "one.pdf"},
            {"kind": "STAGE2_STARTED", "filename": "one.pdf"},
            {"kind": "STAGE2_APPROVED", "filename": "one.pdf"},
        ]
        for event in events:
            metrics.consume(event)

        self.assertEqual(metrics.discovered, 25)
        self.assertEqual(metrics.total_downloaded, 3)
        self.assertEqual(metrics.downloading, 0)
        self.assertEqual(metrics.stage2_approved, 1)
        self.assertEqual(metrics.stage2_queued + metrics.stage2_running + metrics.stage2_pending, 0)
        self.assertIn("complete 3/10", metrics.summary(10))

    def test_duplicate_pending_leaves_pending_when_canonical_finishes(self):
        metrics = RunMetrics()
        for event in (
            {"kind": "STAGE2_STARTED", "filename": "duplicate.pdf"},
            {"kind": "STAGE2_DUPLICATE_PENDING", "filename": "duplicate.pdf"},
            {"kind": "STAGE2_APPROVED", "filename": "duplicate.pdf"},
        ):
            metrics.consume(event)

        self.assertEqual(metrics.stage2_pending, 0)
        self.assertEqual(metrics.stage2_approved, 1)

    def test_queue_and_recovery_counts_are_current_not_historical(self):
        metrics = RunMetrics()
        for event in (
            {"kind": "DOWNLOAD_QUEUED", "filename": "retry.pdf"},
            {"kind": "DOWNLOAD_STARTED", "filename": "retry.pdf"},
            {"kind": "DOWNLOAD_FAILED_FOR_ROUND", "filename": "retry.pdf"},
            {"kind": "DOWNLOAD_RECOVERY_QUEUED", "filename": "retry.pdf"},
            {"kind": "DOWNLOAD_STARTED", "filename": "retry.pdf", "recovery": True},
        ):
            metrics.consume(event)

        self.assertEqual(metrics.download_queued, 0)
        self.assertEqual(metrics.downloading, 1)
        self.assertEqual(metrics.recovery_queued, 0)


if __name__ == "__main__":
    unittest.main()
