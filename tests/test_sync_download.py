"""Offline checks of the synchronous baseline networking and Streamlit bridge."""

import ast
import logging
import os
import random
import re
import shutil
import tempfile
import time as wall_time
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
        assert chunk_size == 256 * 1024
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
            class Cookies:
                def __init__(self):
                    self.clear_calls = 0

                def clear(self):
                    self.clear_calls += 1

            def __enter__(self):
                self.closed = False
                self.cookies = self.Cookies()
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
            c_requests=SimpleNamespace(Session=Session), time=SimpleNamespace(sleep=self.sleeps.append, monotonic=wall_time.monotonic),
            random=SimpleNamespace(randint=lambda lower, upper: lower),
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
            }, 'stream': True, 'timeout': (15, 60), 'impersonate': 'chrome120', 'http_version': 'v1',
        }))
        self.assertEqual(self.destination.read_bytes(), PDF)
        self.assertFalse(self.part.exists())
        self.assertTrue(all(s.closed for s in self.sessions))
        self.assertEqual(self.verify_calls, 1)
        self.assertEqual(self.widget.updates[-1]['label'], '✅ Secured: test.pdf')

    def test_ashrae_gateway_ranking_excludes_ipfs(self):
        urls = [
            'https://cloudflare-ipfs.com/ipfs/book',
            'https://gateway.ipfs.io/ipfs/book',
            'https://gateway.pinata.cloud/ipfs/book',
            'https://dweb.link/ipfs/book',
            'http://localhost:8080/ipfs/book',
            'https://files.example/handbook.pdf',
            'https://library.lol/main/123',
            'https://libgen.bz/get.php?md5=123',
        ]
        self.responses = [Response(text='<title>ASHRAE Handbook Fundamentals</title>' + ''.join(
            f'<a href="{url}">GET</a>' for url in urls
        )), Response()]
        self.assertTrue(self.download())
        self.assertEqual(self.calls[1][0], urls[-3])
        selected = next(message for message in self.widget.logs if message.startswith('Final download candidates:'))
        self.assertTrue(selected.startswith('Final download candidates:\n1. ' + urls[-3] + '\n2. ' + urls[-2] + '\n3. ' + urls[-1]))
        print(selected)

    def test_ipfs_only_fallback_never_uses_localhost(self):
        self.responses = [Response(text='''
            <a href="http://localhost:8080/ipfs/book">IPFS</a>
            <a href="https://gateway.ipfs.io/ipfs/book">IPFS</a>
        '''), Response()]
        self.assertTrue(self.download())
        self.assertEqual(self.calls[1][0], 'https://gateway.ipfs.io/ipfs/book')

    def test_dead_routes_are_attempted_once(self):
        class DNSError(Exception):
            pass
        for failure in (DNSError('DNS failed'), Response(status=403), Response(headers={'Content-Type': 'text/html'})):
            with self.subTest(failure=failure):
                self.calls.clear()
                retry_response = failure if isinstance(failure, DNSError) else Response()
                self.responses = [Response(text='<a href="/bad.pdf">GET</a><a href="/good.pdf">DOWNLOAD</a>'), failure, retry_response, Response()]
                self.assertTrue(self.download())
                streamed = [url for url, args in self.calls if args.get('stream')]
                self.assertEqual(streamed[-1], 'https://mirror.example/good.pdf')

    def test_two_server_errors_mark_gateway_unhealthy_then_try_next_gateway(self):
        self.responses = [Response(text='<a href="/bad.pdf">GET</a><a href="/good.pdf">DOWNLOAD</a>'), Response(status=500), Response(status=500), Response()]
        self.assertTrue(self.download())
        self.assertEqual([url for url, _ in self.calls[1:]], [
            'https://mirror.example/bad.pdf', 'https://mirror.example/bad.pdf', 'https://mirror.example/good.pdf',
        ])
        self.assertEqual(self.sleeps, [8])
        self.assertTrue(all(session.closed for session in self.sessions))

    def test_mirror_parser_deduplicates_and_never_streams_ads_php(self):
        self.responses = [Response(text='''
            <a href="/ads.php?md5=bad">GET</a>
            <a href="/ADS.PHP?md5=bad">Fallback</a>
            <a href="/one.pdf?md5=123#same">DOWNLOAD</a>
            <a href="/two.pdf">IPFS</a>
        '''), Response(headers={'Content-Type': 'text/html'}), Response()]
        self.assertTrue(self.download())
        self.assertEqual([url for url, _ in self.calls[1:]], [
            'https://mirror.example/one.pdf?md5=123#same', 'https://mirror.example/two.pdf',
        ])

    def test_randombook_style_download_anchor_uses_fallback_parser(self):
        self.responses = [
            Response(text='<a class="btn" href="/download/08861ea48a810b70b4b315f9bdd60be.pdf">Open</a>'),
            Response(),
        ]
        self.assertTrue(self.download('https://randombook.org/book/08861ea48a810b70b4b315f9bdd60be'))
        self.assertEqual(self.calls[1][0], 'https://randombook.org/download/08861ea48a810b70b4b315f9bdd60be.pdf')

    def test_script_download_url_uses_fallback_parser(self):
        self.responses = [
            Response(text='<script>window.location = "/storage/handbook.pdf";</script>'),
            Response(),
        ]
        self.assertTrue(self.download('https://randombook.org/book/example'))
        self.assertEqual(self.calls[1][0], 'https://randombook.org/storage/handbook.pdf')

    def test_button_onclick_download_url_uses_fallback_parser(self):
        self.responses = [
            Response(text='<button onclick="window.open(\'/d/handbook.pdf\')">Get file</button>'),
            Response(),
        ]
        self.assertTrue(self.download('https://randombook.org/book/example'))
        self.assertEqual(self.calls[1][0], 'https://randombook.org/d/handbook.pdf')

    def test_partial_connection_drop_resumes_same_url(self):
        prefix = PDF[:3000]
        self.responses = [Response(text='<a href="/file.pdf">GET</a>'), Response(data=prefix, fail=True),
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
        self.responses = [Response(status=503), Response(text='<a href="/file.pdf">DOWNLOAD</a>'), Response()]
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
        self.responses = [Response(text='<a href="/file.pdf">GET</a>'), Response()]
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
        self.assertIn('Range', self.calls[0][1]['headers'])
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_empty_or_incomplete_stream_does_not_finalize(self):
        self.responses = [Response(data=b'', headers={'content-length': '100'}) for _ in range(2)]
        self.assertEqual(self.ns['download_file']('https://cdn.example/file', 'test.pdf', 'https://mirror.example'), 'retry_next')
        self.assertFalse(self.destination.exists())
        self.assertEqual(len(self.calls), 2)


if __name__ == '__main__':
    unittest.main()
