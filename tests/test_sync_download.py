"""Offline checks of the synchronous baseline networking and Streamlit bridge."""

import ast
import logging
import os
import re
import shutil
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from test_download_routes import Widget


PDF = b'%PDF-1.7\n' + b'x' * 6000


class Response:
    def __init__(self, status=200, text='', data=PDF, headers=None, fail=False):
        self.status_code, self.text, self.data = status, text, data
        raw = {'content-length': str(len(data))} if headers is None else headers
        self.headers = Headers({key.lower(): value for key, value in raw.items()})
        self.fail = fail
        self.closed = False

    def iter_content(self, chunk_size):
        assert chunk_size == 65536
        yield self.data
        if self.fail:
            raise OSError('connection dropped')

    def close(self):
        self.closed = True


class Headers(dict):
    def get(self, key, default=None):
        return super().get(key.lower(), default)


class SyncDownloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.widget = Widget()
        self.responses, self.calls, self.sessions, self.sleeps = [], [], [], []
        self.relevant = True
        self.verify_calls = 0
        owner = self

        class Session:
            def __enter__(self):
                self.closed = False
                owner.sessions.append(self)
                return self

            def __exit__(self, *args):
                self.closed = True

            def get(self, url, **kwargs):
                kwargs = {**kwargs, **({'headers': dict(kwargs['headers'])} if 'headers' in kwargs else {})}
                owner.calls.append((url, kwargs))
                if not owner.responses:
                    raise AssertionError('Unexpected additional network request')
                response = owner.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        def verify(path, *args):
            self.assertTrue(Path(path).is_file())
            self.verify_calls += 1
            return self.relevant

        source = (Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8')
        names = ('_download_paths', 'download_file', '_check_download_relevance',
                 'parse_mirror_and_download', 'download_pdf')
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in names]
        self.assertEqual(len(nodes), len(names))  # All network routines must be synchronous.
        self.ns = dict(
            os=os, re=re, Path=Path, shutil=shutil, logging=logging, __name__=__name__,
            urllib=urllib, urljoin=urljoin, urlparse=urlparse, BeautifulSoup=BeautifulSoup,
            c_requests=SimpleNamespace(Session=Session), time=SimpleNamespace(sleep=self.sleeps.append),
            st=self.widget, DOWNLOAD_DIRECTORY=self.root / 'ASHRAE_Files',
            LOW_RELEVANCE_DIRECTORY=self.root / 'Low_Relevance_Files',
            QUERY_TERMS=['ashrae', 'hvac'], CORE_ASHRAE_STANDARDS=['90.1'],
            verify_ashrae_relevance=verify,
        )
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'app.py', 'exec'), self.ns)
        self.destination = self.root / 'ASHRAE_Files' / 'test.pdf'
        self.part = self.destination.with_name('test.pdf.part')

    def download(self, mirrors='https://mirror.example/book/123'):
        return self.ns['download_pdf'](mirrors, 'test.pdf')

    def seed_part(self, data):
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(data)

    def test_sync_mirror_stream_headers_and_ui(self):
        mirror = Response(text='<a href="/get.php?md5=123">GET</a>')
        binary = Response()
        self.responses = [mirror, binary]
        self.assertTrue(self.download())
        self.assertEqual(self.calls[0], ('https://mirror.example/book/123', {
            'timeout': 15, 'impersonate': 'chrome120',
        }))
        self.assertEqual(self.calls[1], ('https://mirror.example/get.php?md5=123', {
            'headers': {
                'Referer': 'https://mirror.example/book/123',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }, 'stream': True, 'timeout': 180, 'impersonate': 'chrome120',
        }))
        self.assertEqual(self.destination.read_bytes(), PDF)
        self.assertFalse(self.part.exists())
        self.assertTrue(mirror.closed and binary.closed and all(s.closed for s in self.sessions))
        self.assertEqual(self.verify_calls, 1)
        self.assertEqual(self.widget.updates[-1]['label'], '✅ Secured: test.pdf')

    def test_five_server_errors_cool_down_then_try_next_gateway(self):
        failures = [Response(status=500) for _ in range(5)]
        self.responses = [Response(text='<a href="/bad">GET</a><a href="/good">IPFS</a>'), *failures, Response()]
        self.assertTrue(self.download())
        self.assertEqual([url for url, _ in self.calls[1:6]], ['https://mirror.example/bad'] * 5)
        self.assertEqual(self.calls[-1][0], 'https://mirror.example/good')
        self.assertEqual(self.sleeps.count(10), 5)
        self.assertTrue(all(response.closed for response in failures))

    def test_mirror_parser_deduplicates_and_never_streams_ads_php(self):
        self.responses = [Response(text='''
            <a href="/ads.php?md5=bad">GET</a>
            <a href="/ADS.PHP?md5=bad">Fallback</a>
            <a href="/one?md5=123">1</a><a href="/one?md5=123#same">DOWNLOAD</a>
            <a href="/two">IPFS</a>
        '''), Response(headers={'Content-Type': 'text/html'}), Response()]
        self.assertTrue(self.download())
        self.assertEqual([url for url, _ in self.calls[1:]], [
            'https://mirror.example/one?md5=123', 'https://mirror.example/two',
        ])

    def test_partial_connection_drop_resumes_same_url(self):
        prefix = PDF[:3000]
        self.responses = [Response(text='<a href="/file">GET</a>'), Response(data=prefix, fail=True),
                          Response(status=206, data=PDF[3000:], headers={
                              'Content-Range': f'bytes 3000-{len(PDF)-1}/{len(PDF)}',
                              'Content-Length': str(len(PDF)-3000),
                          })]
        self.assertTrue(self.download())
        self.assertEqual(self.calls[1][0], self.calls[2][0])
        self.assertEqual(self.calls[2][1]['headers']['Range'], 'bytes=3000-')
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_ignored_range_restarts_instead_of_appending(self):
        self.seed_part(PDF[:3000])
        self.responses = [Response()]
        self.assertIs(self.ns['download_file']('https://cdn.example/file', 'test.pdf', 'https://mirror.example'), True)
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_416_removes_partial_and_retries_without_range(self):
        self.seed_part(PDF[:3000])
        self.responses = [Response(status=416), Response()]
        self.assertIs(self.ns['download_file']('https://cdn.example/file', 'test.pdf', 'https://mirror.example'), True)
        self.assertIn('Range', self.calls[0][1]['headers'])
        self.assertNotIn('Range', self.calls[1][1]['headers'])
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_mirror_fallback_keeps_one_status_and_exact_urls(self):
        mirrors = ['https://one.example/book/123', 'https://two.example/main/123']
        self.responses = [Response(status=503), Response(text='<a href="/file">DOWNLOAD</a>'), Response()]
        self.assertTrue(self.download(mirrors))
        self.assertEqual([url for url, _ in self.calls[:2]], mirrors)
        self.assertEqual(self.calls[-1][1]['headers']['Referer'], mirrors[1])
        self.assertEqual(len(self.widget.status_calls), 1)

    def test_all_mirrors_fail_before_error_status(self):
        self.responses = [Response(text='no links'), Response(status=404)]
        self.assertFalse(self.download(['https://one.example/book/123', 'https://two.example/book/123']))
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.widget.updates[-1]['state'], 'error')
        self.assertFalse(self.destination.exists())

    def test_rejected_pdf_moves_once_without_retrying_other_mirrors(self):
        self.relevant = False
        rejected = self.root / 'Low_Relevance_Files' / 'test.pdf'
        rejected.parent.mkdir()
        rejected.write_bytes(b'older file')
        self.responses = [Response(text='<a href="/file">GET</a>'), Response()]
        self.assertFalse(self.download(['https://one.example/book/123', 'https://two.example/book/123']))
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(self.destination.exists())
        self.assertEqual(rejected.read_bytes(), b'older file')
        self.assertEqual(rejected.with_name('test_1.pdf').read_bytes(), PDF)
        self.assertEqual(self.widget.updates[-1]['label'], '⚠️ Low relevance: test.pdf')

    def test_tiny_partial_restarts_without_range(self):
        self.seed_part(b'tiny')
        self.responses = [Response()]
        self.assertIs(self.ns['download_file']('https://cdn.example/file', 'test.pdf', 'https://mirror.example'), True)
        self.assertNotIn('Range', self.calls[0][1]['headers'])
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_empty_or_incomplete_stream_does_not_finalize(self):
        self.responses = [Response(data=b'', headers={'content-length': '100'}) for _ in range(5)]
        self.assertEqual(self.ns['download_file']('https://cdn.example/file', 'test.pdf', 'https://mirror.example'), 'retry_next')
        self.assertFalse(self.destination.exists())
        self.assertEqual(len(self.calls), 5)


if __name__ == '__main__':
    unittest.main()
