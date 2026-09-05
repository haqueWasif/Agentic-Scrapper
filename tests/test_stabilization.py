"""Focused stabilization tests without importing the Streamlit entry module."""

import ast
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from pypdf import PdfWriter

from app.pdf_validation import check_pdf_integrity
from app.pipeline_state import lifecycle_view


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_SOURCE = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")


def app_functions(*names):
    tree = ast.parse(APP_SOURCE)
    return [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]


class FilenameAndProvenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        namespace = {"re": __import__("re"), "Any": object}
        exec(compile(ast.Module(body=app_functions("_safe_pdf_filename"), type_ignores=[]), "app.py", "exec"), namespace)
        cls.safe_filename = staticmethod(namespace["_safe_pdf_filename"])

    def test_filename_prefix_preserves_source_page_and_item(self):
        self.assertEqual(self.safe_filename("Example", 1, 1), "page_001_001_Example.pdf")
        self.assertEqual(self.safe_filename("Example", 2, 1), "page_002_001_Example.pdf")
        self.assertEqual(self.safe_filename("Example", 3, 1), "page_003_001_Example.pdf")

    def test_queue_builder_persists_page_three_item_one_provenance(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            namespace = {
                "Any": object,
                "_bulk_keyword_match": lambda document, query: True,
                "_safe_pdf_filename": self.safe_filename,
                "_download_paths": lambda filename: (directory / filename, directory / filename, directory / f"{filename}.part"),
                "_download_state_entry": lambda filename: None,
                "_document_mirror_links": lambda document, rows, search_url: ["https://library.example/book"],
                "str": str,
            }
            exec(compile(ast.Module(body=app_functions("_bulk_candidates_from_page"), type_ignores=[]), "app.py", "exec"), namespace)
            candidates, skipped = namespace["_bulk_candidates_from_page"](
                [{"title": "Example", "link": "https://source.example/1"}], [], "https://search.example", "ashrae", 10, 3,
            )
        self.assertEqual(skipped, 0)
        self.assertEqual(candidates[0]["source_page"], 3)
        self.assertEqual(candidates[0]["source_item"], 1)
        self.assertTrue(candidates[0]["filename"].startswith("page_003_001_"))
        lifecycle = lifecycle_view({}, status="QUEUED", candidate=candidates[0])
        self.assertEqual(lifecycle["discovery"]["source_page"], 3)
        self.assertEqual(lifecycle["discovery"]["source_item"], 1)


class PdfIntegrityGateTests(unittest.TestCase):
    def test_non_pdf_header_is_technical_invalidity(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "carrier.pdf"
            path.write_bytes(b"AT&TF" + b"x" * 256)
            result = check_pdf_integrity(path)
        self.assertFalse(result["valid"])
        self.assertEqual(result["error_type"], "INVALID_PDF_HEADER")
        lifecycle = lifecycle_view(
            {"technical_error": {"error_type": result["error_type"], "timestamp": 1}},
            status="COMPLETED", candidate={"title": "carrier"}, validation_status="PDF_INVALID",
        )
        self.assertEqual(lifecycle["final_status"], "PDF_INVALID")
        self.assertTrue(lifecycle["stage2"]["completed"])

    def test_truncated_pdf_signature_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "truncated.pdf"
            path.write_bytes(b"%PDF-1.7\n" + b"x" * 256)
            result = check_pdf_integrity(path)
        self.assertFalse(result["valid"])
        self.assertIn(result["error_type"], {"PDF_TRUNCATED", "PDF_PARSE_ERROR"})

    def test_valid_pdf_passes_without_text_extraction(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "valid.pdf"
            writer = PdfWriter()
            writer.add_blank_page(width=72, height=72)
            with path.open("wb") as output:
                writer.write(output)
            result = check_pdf_integrity(path)
        self.assertTrue(result["valid"])
        self.assertEqual(result["page_count"], 1)

    def test_invalid_file_never_enters_stage2_queue(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "invalid.pdf"
            destination.write_bytes(b"AT&TF" + b"x" * 256)
            updates, events = [], []
            queue = Mock()
            namespace = {
                "_download_paths": lambda filename: (destination, destination, destination.with_suffix(".part")),
                "_download_state_entry": lambda filename: {"candidate": {"run_id": "run-1"}},
                "check_pdf_integrity": check_pdf_integrity,
                "_update_download_state": lambda *args, **kwargs: updates.append((args, kwargs)),
                "_validation_queue": lambda: queue,
                "logging": __import__("logging"),
                "_emit_pipeline_event": lambda *args, **kwargs: events.append((args, kwargs)),
            }
            exec(compile(ast.Module(body=app_functions("_schedule_validation"), type_ignores=[]), "app.py", "exec"), namespace)
            self.assertFalse(namespace["_schedule_validation"]("invalid.pdf"))
        queue.contains.assert_not_called()
        self.assertTrue(any(kwargs.get("validation_status") == "PDF_INVALID" for _, kwargs in updates))
        self.assertEqual(events[0][0][0], "STAGE2_INVALID")

    def test_valid_file_keeps_existing_queue_admission_path(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "valid.pdf"
            writer = PdfWriter()
            writer.add_blank_page(width=72, height=72)
            with destination.open("wb") as output:
                writer.write(output)
            updates = []

            class Queue:
                def contains(self, filename):
                    return False

                def try_enqueue(self, job, *, key):
                    return True

            namespace = {
                "_download_paths": lambda filename: (destination, destination, destination.with_suffix(".part")),
                "_download_state_entry": lambda filename: {"candidate": {"run_id": "run-1"}},
                "check_pdf_integrity": check_pdf_integrity,
                "_update_download_state": lambda *args, **kwargs: updates.append((args, kwargs)),
                "_validation_queue": lambda: Queue(),
                "logging": __import__("logging"),
            }
            exec(compile(ast.Module(body=app_functions("_schedule_validation"), type_ignores=[]), "app.py", "exec"), namespace)
            self.assertTrue(namespace["_schedule_validation"]("valid.pdf", stage1_score=77))
        self.assertTrue(any(kwargs.get("validation_status") == "QUEUED" for _, kwargs in updates))


if __name__ == "__main__":
    unittest.main()
