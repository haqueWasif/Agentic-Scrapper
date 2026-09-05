"""Verify page batching and safe ID-to-source mapping without live LLM calls."""

import json
import os
import unittest
from unittest.mock import patch

from test_llm_fallback import load_evaluator


class BatchEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.documents = [
            {"id": index, "title": f"HVAC {index}", "link": f"https://example.org/book/{index}", "text": "ASHRAE"}
            for index in range(1, 26)
        ]

    def test_one_task_for_25_documents_retains_original_sources(self):
        ns, models, crews = load_evaluator(['```json\n[1, 4, 7, 12, 4]\n```'])
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test"}):
            result = ns['evaluate_documents']({"document_batch": self.documents})
        self.assertEqual(len(models), 1)
        self.assertEqual(len(crews), 1)
        self.assertEqual(crews[0].calls, 1)
        self.assertEqual(len(crews[0].tasks), 1)
        self.assertIn(json.dumps(self.documents), crews[0].tasks[0].description)
        self.assertEqual(result['extracted_documents'], [
            dict(self.documents[index - 1], approved=True) for index in (1, 4, 7, 12)
        ])

    def test_wrappers_and_empty_approval(self):
        ns, _, _ = load_evaluator([])
        for text in ('[]', '```JSON\n[]\n```', 'Result:\n```json\n[]\n```\nDone.'):
            self.assertEqual(ns['_approved_batch_documents'](text, self.documents), [])

    def test_invalid_or_unknown_ids_are_not_approvals(self):
        ns, _, _ = load_evaluator([])
        for text in ('[1,', 'not json', '[26]', '[true]', '["1"]', '[1.0]', '[{"id":1}]', '{"ids":[1]}'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                ns['_approved_batch_documents'](text, self.documents)

    def test_empty_page_does_not_call_llm(self):
        ns, models, crews = load_evaluator([])
        self.assertEqual(ns['evaluate_documents']({"document_batch": []}), {"extracted_documents": []})
        self.assertFalse(models or crews)


if __name__ == '__main__':
    unittest.main()
