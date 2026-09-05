"""Exercise the ported baseline rules without launching Streamlit."""

import ast
import logging
import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import mock_open, patch


class RelevanceTests(unittest.TestCase):
    def verify(self, text, filename='document.pdf', broken=False, empty=False):
        source = (Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        nodes = [node for node in tree.body if (
            isinstance(node, ast.FunctionDef) and node.name == 'verify_ashrae_relevance'
        ) or (
            isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id in ('QUERY_TERMS', 'CORE_ASHRAE_STANDARDS')
                for target in node.targets
            )
        )]

        def reader(stream):
            if broken:
                raise ValueError('Unreadable PDF')
            return SimpleNamespace(pages=[] if empty else [SimpleNamespace(extract_text=lambda: text)])

        namespace = {'re': re, 'logging': logging, 'PdfReader': reader, '__name__': __name__}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'app.py', 'exec'), namespace)
        with patch('builtins.open', mock_open(read_data=b'PDF')):
            return namespace['verify_ashrae_relevance'](
                'test.pdf', filename, namespace['QUERY_TERMS'], namespace['CORE_ASHRAE_STANDARDS'],
            )

    def test_no_ashrae_is_rejected_even_with_hvac_terms(self):
        self.assertFalse(self.verify('HVAC refrigeration chiller cooling tower ' * 4))

    def test_core_standard_passes(self):
        self.assertTrue(self.verify('ASHRAE Standard 90.1 ' + 'reference publication ' * 5))

    def test_core_standard_in_filename_passes(self):
        self.assertTrue(self.verify('Reference publication ' * 5, 'ASHRAE standard-15.pdf'))

    def test_core_handbook_passes(self):
        self.assertTrue(self.verify('Reference publication ' * 5, 'ASHRAE Handbook Fundamentals.pdf'))

    def test_two_keyword_matches_pass(self):
        self.assertTrue(self.verify('ASHRAE HVAC ' + 'reference publication ' * 5))

    def test_ashrae_without_second_keyword_fails(self):
        self.assertFalse(self.verify('ASHRAE ' + 'reference publication ' * 5))

    def test_short_and_scanned_pages_pass_as_in_baseline(self):
        for text in ('short title', '', None):
            with self.subTest(text=text):
                self.assertTrue(self.verify(text))

    def test_empty_document_passes_as_in_baseline(self):
        self.assertTrue(self.verify(None, empty=True))

    def test_unreadable_document_passes_as_in_baseline(self):
        with self.assertLogs(__name__, level='WARNING'):
            self.assertTrue(self.verify(None, broken=True))


if __name__ == '__main__':
    unittest.main()
