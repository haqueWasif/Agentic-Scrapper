"""Presentation-only tests for worker telemetry consumed by Streamlit's main thread."""

import time
import unittest
from pathlib import Path

from app.pipeline_events import DownloadPresentation, PipelineDashboardSnapshot, RunMetrics


class DownloadPresentationTests(unittest.TestCase):
    def test_worker_reuse_clears_previous_sizes_and_stall_timestamp(self):
        presentation = DownloadPresentation(max_workers=1)
        presentation.consume(dict(kind='progress', worker=1, filename='old.pdf',
                                  document_attempt=1, downloaded=100, total=200, timestamp=1))
        presentation.consume(dict(kind='worker_state', worker=1, filename='new.pdf',
                                  document_attempt=1, state='DOWNLOADING', bytes_downloaded=300))
        row = presentation.worker_rows()[0]
        self.assertEqual(row['bytes_downloaded'], 300)
        self.assertEqual(row['total_bytes'], 0)
        self.assertGreater(row['last_progress_at'], 1)
        presentation.consume(dict(kind='progress', worker=1, filename='new.pdf',
                                  document_attempt=1, downloaded=400, total=500))
        presentation.consume(dict(kind='worker_state', worker=1, filename='new.pdf',
                                  document_attempt=2, state='STARTING'))
        self.assertEqual(row['total_bytes'], 0)
        self.assertNotIn('last_progress_at', row)

    def test_explicit_assignment_resets_same_file_and_attempt(self):
        presentation = DownloadPresentation(max_workers=1)
        presentation.consume(dict(kind='progress', worker=1, filename='A.pdf', downloaded=100, total=200))
        presentation.consume(dict(kind='worker_started', worker=1, filename='A.pdf', bytes_downloaded=100))
        self.assertEqual(presentation.worker_rows()[0]['total_bytes'], 0)

    def test_success_replaces_throttled_sample_with_final_size(self):
        presentation = DownloadPresentation(max_workers=1)
        presentation.consume(dict(kind='progress', worker=1, filename='A.pdf', downloaded=0, total=0))
        presentation.consume(dict(kind='success', worker=1, filename='A.pdf',
                                  bytes_downloaded=8192, total_bytes=8192))
        row = presentation.worker_rows()[0]
        self.assertEqual((row['state'], row['bytes_downloaded'], row['total_bytes']), ('COMPLETED', 8192, 8192))

    def test_inconsistent_total_is_unknown(self):
        presentation = DownloadPresentation(max_workers=1)
        presentation.consume(dict(kind='progress', worker=1, downloaded=300, total=100))
        self.assertEqual(presentation.worker_rows()[0]['total_bytes'], 0)

    @staticmethod
    def snapshot(*, queue_active=0, queue_pending=0, recovery_jobs=None, presentation=None):
        metrics = RunMetrics(existing_downloads=12)
        metrics.discovered = 49
        metrics.stage1_approved = 25
        metrics.stage1_rejected = 1
        metrics.stage2_queued = 2
        metrics.stage2_pending = 9
        return PipelineDashboardSnapshot.from_sources(
            metrics=metrics, presentation=presentation or DownloadPresentation(max_workers=5),
            queue_active=queue_active, queue_pending=queue_pending,
            recovery_jobs=recovery_jobs or [], discovery_page=2, max_pages=50,
            target_documents=500, max_document_attempts=5,
        )

    def test_snapshot_uses_queue_as_authoritative_download_counts(self):
        snapshot = self.snapshot(queue_active=1, queue_pending=4)
        self.assertEqual(snapshot.queue_active, 1)
        self.assertEqual(snapshot.queue_pending, 4)
        self.assertEqual(len(snapshot.worker_rows), 5)
        self.assertEqual(snapshot.worker_rows[0]["state"], "STARTING")
        self.assertEqual(snapshot.worker_rows[0]["last_event"], "Worker active; awaiting first telemetry event")

    def test_snapshot_uses_actual_recovery_backlog_not_event_counter(self):
        jobs = [{"filename": f"{index}.pdf", "_document_attempt": 4} for index in range(4)]
        snapshot = self.snapshot(recovery_jobs=jobs)
        self.assertEqual(snapshot.recovery_backlog, 4)
        self.assertEqual(snapshot.recovery_queued, 4)
        self.assertEqual(snapshot.recovery_near_limit, 4)

    def test_snapshot_does_not_show_stale_active_worker_when_queue_is_idle(self):
        presentation = DownloadPresentation(max_workers=5)
        presentation.consume({
            "kind": "progress", "level": "DEBUG", "worker_id": 1, "filename": "A.pdf",
            "state": "MAKING_PROGRESS", "downloaded": 100, "total": 200,
        })
        snapshot = self.snapshot(queue_active=0, presentation=presentation)
        self.assertEqual(snapshot.queue_active, 0)
        self.assertEqual(snapshot.worker_rows[0]["state"], "IDLE")

    def test_empty_debug_message_is_explicit(self):
        self.assertEqual(PipelineDashboardSnapshot.empty_debug_message(), "No network debug events yet.")

    def test_app_has_one_dashboard_heading_and_no_main_status_transcript(self):
        source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        self.assertEqual(source.count('st.subheader("Agent activity")'), 1)
        self.assertNotIn('st.markdown("### Agent activity")', source)
        self.assertNotIn('st.status("🤖 Agentic Pipeline Initialized..."', source)
        self.assertIn('with st.expander("Search page details"', source)

    def test_normal_activity_excludes_debug_and_trace_but_retains_them(self):
        presentation = DownloadPresentation(max_workers=5)
        common = {"worker_id": 1, "filename": "A.pdf", "run_id": "run-1"}
        presentation.consume({**common, "kind": "trace", "level": "TRACE", "message": "full URL https://example.test/a?secret=no"})
        presentation.consume({**common, "kind": "debug", "level": "DEBUG", "message": "Range: bytes=123-"})
        presentation.consume({**common, "kind": "worker_state", "level": "STATUS", "short_message": "↓ Worker 1 started A.pdf", "state": "DOWNLOADING"})
        presentation.consume({**common, "kind": "success", "level": "LIFECYCLE", "short_message": "✓ Worker 1 completed PDF", "state": "COMPLETED"})

        self.assertEqual(list(presentation.activity), ["↓ Worker 1 started A.pdf", "✓ Worker 1 completed PDF"])
        self.assertEqual(len(presentation.debug), 2)
        self.assertEqual(presentation.worker_rows()[0]["state"], "COMPLETED")

    def test_one_worker_row_is_updated_for_progress_and_stall(self):
        presentation = DownloadPresentation(max_workers=5)
        started = time.time() - 120
        base = {
            "worker_id": 1, "filename": "A.pdf", "source_page": 1, "source_item": 9,
            "work_kind": "recovery", "document_attempt": 1, "max_attempts": 5,
            "gateway_host": "libgen.bz", "level": "STATUS", "timestamp": started,
        }
        presentation.consume({**base, "kind": "worker_state", "state": "DOWNLOADING", "short_message": "↓ Worker 1 started"})
        presentation.consume({**base, "kind": "progress", "level": "DEBUG", "state": "MAKING_PROGRESS", "downloaded": 10 * 1024 * 1024, "total": 40 * 1024 * 1024, "timestamp": started})
        presentation.consume({**base, "kind": "progress", "level": "DEBUG", "state": "MAKING_PROGRESS", "downloaded": 20 * 1024 * 1024, "total": 40 * 1024 * 1024, "timestamp": started})
        presentation.mark_stalled(1)

        rows = presentation.worker_rows()
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0]["state"], "STALLED")
        self.assertEqual(rows[0]["bytes_downloaded"], 20 * 1024 * 1024)
        self.assertEqual(rows[0]["work_kind"], "recovery")
        self.assertEqual(rows[0]["source_page"], 1)

    def test_terminal_failure_is_a_lifecycle_event_not_pending_recovery(self):
        presentation = DownloadPresentation(max_workers=5)
        presentation.consume({
            "kind": "failure", "level": "LIFECYCLE", "worker_id": 2,
            "filename": "A.pdf", "work_kind": "recovery", "document_attempt": 5,
            "max_attempts": 5, "state": "PERMANENTLY_FAILED",
            "short_message": "✕ Worker 2 final failure · attempt 5/5 exhausted",
        })
        self.assertEqual(presentation.worker_rows()[1]["state"], "PERMANENTLY_FAILED")
        self.assertEqual(
            list(presentation.activity),
            ["✕ Worker 2 final failure · attempt 5/5 exhausted"],
        )


if __name__ == "__main__":
    unittest.main()
