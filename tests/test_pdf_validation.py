"""Offline checks for Stage 2 scanning, duplicate prevention, and report storage."""

import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pdf_validation import (
    completed_validation_report,
    scan_existing_pdfs,
    store_validated_pdf,
    validation_summary,
    write_validation_report,
)


class PdfValidationStorageTests(unittest.TestCase):
    def test_existing_pdf_is_scanned_once_then_reported_and_stored(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            data_directory = Path(temporary_directory) / "data"
            data_directory.mkdir()
            source = data_directory / "manual-hvac.pdf"
            source.write_bytes(b"%PDF-1.7 placeholder")

            scanned = scan_existing_pdfs(data_directory)
            self.assertEqual(scanned[0]["filename"], source.name)
            self.assertEqual(scanned[0]["size"], source.stat().st_size)

            write_validation_report(source, data_directory, {
                "filename": source.name,
                "source": "existing_pdf",
                "stage1_score": None,
                "stage2_score": 95,
                "approved": True,
                "categories": ["HVAC"],
                "reason": "Technical HVAC content",
                "status": "APPROVED",
                "stage2_completed": True,
            })
            self.assertEqual(scan_existing_pdfs(data_directory), [])

            stored = store_validated_pdf(source, data_directory, approved=True)
            self.assertTrue(stored.is_file())
            self.assertFalse(source.exists())
            write_validation_report(source, data_directory, {
                "filename": stored.name, "source": "existing_pdf", "status": "APPROVED",
                "approved": True, "stage2_completed": True, "stored_path": str(stored),
            })
            summary = validation_summary(data_directory)
            self.assertEqual(summary["approved"], 1)
            self.assertEqual(summary["existing_pdf_approved"], 1)
            self.assertIsNotNone(completed_validation_report(stored, data_directory))

    def test_pending_report_remains_eligible_for_a_later_validation(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            data_directory = Path(temporary_directory) / "data"
            data_directory.mkdir()
            source = data_directory / "scanned.pdf"
            source.write_bytes(b"%PDF-1.7 placeholder")
            write_validation_report(source, data_directory, {
                "filename": source.name, "source": "existing_pdf", "status": "PENDING",
                "approved": False, "stage2_completed": False, "stage2_score": 0,
            })
            self.assertEqual(len(scan_existing_pdfs(data_directory)), 1)
            self.assertEqual(validation_summary(data_directory)["pending"], 1)

    def test_adaptive_preview_extends_past_front_matter(self):
        class Page:
            def __init__(self, text):
                self.text = text

            def extract_text(self):
                return self.text

        class Reader:
            metadata = {"/Title": "ASHRAE Handbook Refrigeration", "/Author": "ASHRAE"}
            pages = [Page(""), Page("copyright"), Page("front matter and publication details " * 4), Page("refrigeration systems " * 180)]

        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "handbook.pdf"
            source.write_bytes(b"placeholder")
            with patch("app.pdf_validation.PdfReader", return_value=Reader()):
                from app.pdf_validation import extract_pdf_preview
                preview = extract_pdf_preview(source, min_characters=2500)
            self.assertEqual(preview["pages_sampled"], [1, 2, 3, 4])
            self.assertEqual(preview["extraction_quality"], "good")
            self.assertTrue(preview["metadata_title_available"])
