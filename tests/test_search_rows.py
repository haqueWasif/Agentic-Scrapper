"""Use real BeautifulSoup to verify row-scoped mirror discovery."""

import ast
import re
import unittest
import urllib.parse
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


class SearchRowTests(unittest.TestCase):
    def setUp(self):
        source = (Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8')
        functions = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
                     and node.name in ('_extract_search_mirror_rows', '_document_mirror_links')]
        namespace = dict(BeautifulSoup=BeautifulSoup, urllib=urllib, re=re, urljoin=urljoin, urlparse=urlparse)
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'app.py', 'exec'), namespace)
        self.extract = namespace['_extract_search_mirror_rows']
        self.resolve = namespace['_document_mirror_links']
        self.base = 'https://libgen.bz/index.php?req=ASHRAE'
        self.md5 = 'a' * 32
        self.primary = f'https://libgen.bz/ads.php?md5={self.md5}'
        self.secondary = f'https://secondary.example/main/{self.md5}'
        self.third = 'https://third.example/book/123'
        self.html = f'''
            <a href="https://navigation.example/book/999">Menu</a>
            <table><tr><th>Mirrors</th></tr>
            <tr><td><a href="/edition.php?id=123">ASHRAE Handbook</a></td><td>
                <a href="/ads.php?md5={self.md5}">[1]</a>
                <a href="{self.secondary}">[2]</a>
                <a href="{self.third}">3</a>
                <a href="{self.third}#duplicate">[3]</a>
                <a href="https://numbered.example/file/123">[4]</a>
                <a href="/index.php?req=author">Author search</a>
                <a href="javascript:bad()">[5]</a>
                <a href="#top">[6]</a>
            </td></tr>
            <tr><td><a href="/ads.php?md5={'b' * 32}">ASHRAE Handbook</a></td><td>
                <a href="https://unrelated.example/book/456">[2]</a>
            </td></tr></table>
        '''

    def test_collects_every_column_without_cross_row_or_navigation_links(self):
        rows = self.extract(self.html, self.base)
        self.assertEqual(len(rows), 2)
        mirrors = self.resolve({'link': self.primary}, rows, self.base)
        self.assertEqual(set(mirrors), {
            'https://libgen.bz/edition.php?id=123', self.primary,
            self.secondary, self.third, 'https://numbered.example/file/123',
        })
        self.assertEqual(mirrors[0], self.primary)
        self.assertEqual(len(mirrors), len(set(mirrors)))

    def test_md5_matches_row_when_evaluator_changes_domain(self):
        rows = self.extract(self.html, self.base)
        mirrors = self.resolve({'link': f'https://elsewhere.example/main/{self.md5.upper()}'}, rows, self.base)
        self.assertIn(self.secondary, mirrors)
        self.assertNotIn('https://unrelated.example/book/456', mirrors)

    def test_title_similarity_never_borrows_another_books_mirrors(self):
        rows = self.extract(self.html, self.base)
        link = 'https://unknown.example/book/789'
        self.assertEqual(self.resolve({'link': link, 'title': 'ASHRAE Handbook'}, rows, self.base), [link])

    def test_nested_table_does_not_merge_books(self):
        html = '<table><tr><td>' + self.html + '</td></tr></table>'
        self.assertEqual(self.extract(html, self.base), self.extract(self.html, self.base))

    def test_ambiguous_shared_links_do_not_merge_rows(self):
        rows = [{'links': [self.primary], 'mirror_links': [self.primary, candidate]}
                for candidate in (self.secondary, self.third)]
        self.assertEqual(self.resolve({'link': self.primary}, rows, self.base), [self.primary])


if __name__ == '__main__':
    unittest.main()
