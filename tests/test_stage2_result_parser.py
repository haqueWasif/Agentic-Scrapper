"""Offline checks for forgiving, conservative Stage 2 result parsing."""

import ast
import json
import re
import unittest
from pathlib import Path
from typing import Any


class Stage2ResultParserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = (Path(__file__).resolve().parents[1] / "app" / "agents" / "orchestrator.py").read_text(encoding="utf-8")
        names = {"clean_llm_json_output", "_first_json_object", "_pdf_validation_result"}
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in names]
        cls.namespace = {"json": json, "re": re, "Any": Any}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "orchestrator.py", "exec"), cls.namespace)

    def test_parses_json_embedded_in_extra_text(self):
        output = "Here is the evaluation:\n```json\n{\"approved\": true, \"score\": 93, \"confidence\": \"high\", \"needs_more_text\": false, \"categories\": [\"ASHRAE\"], \"reason\": \"technical handbook\"}\n```"
        result = self.namespace["_pdf_validation_result"](output, extraction_quality="good")
        self.assertEqual(result["status"], "APPROVED")
        self.assertEqual(result["score"], 93)

    def test_inconsistent_or_midrange_result_stays_pending(self):
        output = '{"approved": true, "score": 20, "categories": [], "reason": "conflicting"}'
        result = self.namespace["_pdf_validation_result"](output, extraction_quality="good")
        self.assertEqual(result["status"], "PENDING")

    def test_clear_low_score_rejection_requires_good_extraction(self):
        output = '{"approved": false, "score": 20, "categories": ["fiction"], "reason": "unrelated novel"}'
        self.assertEqual(self.namespace["_pdf_validation_result"](output, extraction_quality="good")["status"], "REJECTED")
        self.assertEqual(self.namespace["_pdf_validation_result"](output, extraction_quality="weak")["status"], "PENDING")
