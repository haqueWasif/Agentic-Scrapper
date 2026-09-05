"""Offline route tests; load just the downloader without starting Streamlit."""

import ast
import asyncio
import os
import random
import re
import shutil
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote, urljoin, urlparse


class Widget:
    def __init__(self):
        self.logs = []
        self.state = None
        self.updates = []
        self.status_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def container(self):
        return self

    def status(self, *args, **kwargs):
        self.status_calls.append((args, kwargs))
        return self

    def empty(self):
        return self

    def write(self, message):
        self.logs.append(message)

    def progress(self, *args, **kwargs):
        return self

    def update(self, **kwargs):
        self.updates.append(kwargs)
        self.state = kwargs.get('state')


class Response:
    def __init__(self, status=200, text='links', data=b'%PDF-1.7\nexample', headers=None, fail=False):
        self.status_code = status
        self.text = text
        self.data = data
        self.headers = headers or {'Content-Length': str(len(data))}
        self.url = 'https://libgen.li/ads.php?md5=123'
        self.fail = fail
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')

    async def aclose(self):
        self.closed = True

    async def aiter_content(self, chunk_size):
        yield self.data
        if self.fail:
            raise TransportError('connection dropped')



class TransportError(Exception):
    pass


class Session:
    def __init__(self, mirror, binary):
        self.mirror = mirror
        self.binary = binary
        self.calls = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.binary if kwargs.get('stream') else self.mirror
        if isinstance(response, list):
            response = response.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class Anchor(dict):
    def get_text(self, *args, **kwargs):
        return self['label']


def soup(html, parser):
    anchors = [] if html == 'no links' else [
        Anchor(href='/get.php?key=123', label='GET'),
        Anchor(href='https://gateway.example/file.pdf', label='Pinata IPFS'),
    ]
    return SimpleNamespace(find_all=lambda *args, **kwargs: anchors)


async def no_sleep(seconds):
    pass




class PipelineStatusTests(unittest.IsolatedAsyncioTestCase):
    async def run_pipeline(self, search_response, docs, query='HVAC', api_key='test-search-key', row_groups=None, bm25_docs=None, markdown_result='Prepared content'):
        root = Path(__file__).resolve().parents[1]
        tree = ast.parse((root / 'app.py').read_text(encoding='utf-8'))
        functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                     and node.name in ('run_scraping_pipeline', '_safe_pdf_filename', '_document_mirror_links')]
        widget = Widget()
        downloads = []
        self.search_sessions = []
        self.local_sessions = []
        self.parsed_html = []
        self.bm25_calls = []

        def session_factory(**kwargs):
            self.assertEqual(kwargs, {'timeout': 45})
            session = Session(search_response, None)
            self.search_sessions.append(session)
            return session

        def local_factory(**kwargs):
            self.assertEqual(kwargs, {
                'impersonate': 'chrome120',
                'doh_url': 'https://cloudflare-dns.com/dns-query',
                'verify': False,
            })
            session = Session(search_response, None)
            self.local_sessions.append(session)
            return session

        async def extract(html):
            self.parsed_html.append(html)
            if isinstance(markdown_result, Exception):
                raise markdown_result
            return markdown_result

        def bm25(html, search_url):
            self.bm25_calls.append((html, search_url))
            if isinstance(bm25_docs, Exception):
                raise bm25_docs
            return bm25_docs or []

        def download(url, filename):
            downloads.append((url, filename))
            return len(downloads) == 1

        async def invoke(state, on_status):
            if isinstance(docs, Exception):
                raise docs
            return {'extracted_documents': docs}

        namespace = dict(
            os=os, urllib=urllib, re=re, quote=quote, urljoin=urljoin, urlparse=urlparse, st=widget,
            random=SimpleNamespace(shuffle=lambda values: None, uniform=random.uniform),
            asyncio=SimpleNamespace(sleep=no_sleep, to_thread=asyncio.to_thread),
            httpx=SimpleNamespace(AsyncClient=session_factory),
            c_requests=SimpleNamespace(AsyncSession=local_factory),
            invoke_scraper_graph=invoke,
            extract_markdown=extract, download_pdf=download,
            _extract_search_mirror_rows=lambda html, url: row_groups or [],
            _evaluate_with_bm25=bm25,
        )
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'app.py', 'exec'), namespace)
        with patch.dict(os.environ, {'ZENROWS_API_KEY': api_key}):
            summary = await namespace['run_scraping_pipeline'](query, 1)
        return summary, downloads, widget

    async def test_approvals_and_failed_downloads_counted_separately(self):
        summary, downloads, widget = await self.run_pipeline(Response(), [
            {'title': 'Chillers', 'link': '/first'},
            {'title': 'HVAC', 'link': '/second'},
            {'title': 'Excluded', 'link': '/third', 'approved': False},
        ])
        self.assertEqual(summary, dict(pages_processed=1, documents_approved=2, documents_downloaded=1))
        self.assertEqual(len(downloads), 2)
        self.assertFalse(self.bm25_calls)
        endpoint = urllib.parse.urlparse(self.search_sessions[0].calls[0][0])
        self.assertEqual(endpoint.netloc, 'api.zenrows.com')
        params = urllib.parse.parse_qs(endpoint.query)
        self.assertEqual(params['apikey'], ['test-search-key'])
        self.assertEqual(params['premium_proxy'], ['true'])
        target = urllib.parse.urlparse(params['url'][0])
        self.assertEqual(target.scheme, 'https')
        self.assertEqual(target.netloc, 'libgen.bz')
        self.assertEqual(target.path, '/index.php')
        self.assertEqual(urllib.parse.parse_qs(target.query), {
            'req': ['HVAC'], 'columns[]': ['t', 'a', 's', 'y', 'p', 'i'],
            'objects[]': ['f', 'e', 's', 'a', 'p', 'w'],
            'topics[]': ['l', 'c', 'f', 'a', 'm', 'r', 's'],
            'res': ['25'], 'filesuns': ['all'], 'page': ['1'],
        })
        self.assertFalse(self.local_sessions)
        self.assertEqual(self.parsed_html, ['links'])
        self.assertTrue(self.search_sessions[0].closed)
        self.assertEqual(downloads[0][0], ['https://libgen.bz/first'])
        self.assertEqual(widget.updates[-1]['label'], '✅ AI Approved 2 documents.')
        self.assertFalse(widget.updates[-1]['expanded'])
        self.assertIn('⏳ Fetching live search results...', widget.logs)
        self.assertIn('🧠 AI Evaluator is semantically analyzing documents...', widget.logs)

    async def test_unavailable_search_reports_status_inside_card(self):
        summary, downloads, widget = await self.run_pipeline(Response(status=500), [])
        self.assertEqual(summary['pages_processed'], 0)
        self.assertFalse(downloads)
        self.assertEqual(widget.state, 'error')
        self.assertEqual(len(self.search_sessions), 1)
        self.assertEqual(len(self.local_sessions), 1)
        self.assertEqual(sum('returned 500' in message for message in widget.logs), 2)
        self.assertFalse(self.parsed_html)
        self.assertNotIn('test-search-key', repr(widget.logs))

    async def test_failed_api_uses_local_doh_with_identical_search_url(self):
        summary, downloads, _ = await self.run_pipeline(
            [Response(status=500), Response(text='search HTML')],
            [{'title': 'HVAC', 'link': '/ads.php?md5=123'}],
        )
        self.assertEqual(summary['pages_processed'], 1)
        self.assertEqual(self.parsed_html, ['search HTML'])
        self.assertEqual(downloads[0][0], ['https://libgen.bz/ads.php?md5=123'])
        api_url = urllib.parse.urlparse(self.search_sessions[0].calls[0][0])
        target = urllib.parse.parse_qs(api_url.query)['url'][0]
        self.assertEqual(self.local_sessions[0].calls, [(target, {'timeout': 45})])
        self.assertTrue(all(session.closed for session in self.search_sessions + self.local_sessions))

    async def test_empty_api_html_tries_local_doh(self):
        summary, _, _ = await self.run_pipeline([Response(text='  '), Response()], [])
        self.assertEqual(summary['pages_processed'], 1)
        self.assertEqual(len(self.search_sessions), 1)
        self.assertEqual(len(self.local_sessions), 1)
        self.assertEqual(self.parsed_html, ['links'])

    async def test_transport_error_does_not_expose_api_key(self):
        summary, _, widget = await self.run_pipeline(
            [TransportError('https://api.zenrows.com/v1/?apikey=test-search-key'), Response()], [],
        )
        self.assertEqual(summary['pages_processed'], 1)
        self.assertTrue(any('TransportError' in message for message in widget.logs))
        self.assertNotIn('test-search-key', repr(widget.logs))

    async def test_search_query_survives_api_url_encoding(self):
        query = 'ASHRAE & HVAC + chiller/standard'
        await self.run_pipeline(Response(), [], query=query)
        endpoint = urllib.parse.urlparse(self.search_sessions[0].calls[0][0])
        target = urllib.parse.parse_qs(endpoint.query)['url'][0]
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlparse(target).query)['req'], [query])

    async def test_missing_search_key_goes_directly_to_local_doh(self):
        summary, _, widget = await self.run_pipeline(Response(), [], api_key='')
        self.assertEqual(summary['pages_processed'], 1)
        self.assertFalse(self.search_sessions)
        self.assertEqual(len(self.local_sessions), 1)
        self.assertEqual(widget.state, 'complete')

    async def test_both_routes_timeout_skips_page(self):
        summary, downloads, widget = await self.run_pipeline(
            [TransportError('API unavailable'), TransportError('local blocked')], [],
        )
        self.assertEqual(summary['pages_processed'], 0)
        self.assertFalse(downloads)
        self.assertFalse(self.parsed_html)
        self.assertEqual(widget.state, 'error')

    async def test_pipeline_passes_only_approved_books_row_mirrors(self):
        primary = 'https://libgen.bz/ads.php?md5=123'
        alternate = 'https://secondary.example/book/123'
        unrelated = 'https://another.example/book/456'
        summary, downloads, _ = await self.run_pipeline(Response(), [
            {'title': 'ASHRAE', 'link': primary},
        ], row_groups=[
            {'links': [primary, alternate], 'mirror_links': [primary, alternate]},
            {'links': [unrelated], 'mirror_links': [unrelated]},
        ])
        self.assertEqual(summary['documents_downloaded'], 1)
        self.assertEqual(downloads[0][0], [primary, alternate])

    async def test_evaluator_failure_empty_or_unusable_results_use_bm25(self):
        for output in (RuntimeError('LLM rejected output'), [], [{'relevance_reason': 'no usable link'}], None):
            with self.subTest(output=output):
                summary, downloads, widget = await self.run_pipeline(Response(text='raw table'), output,
                    bm25_docs=[{'title': 'ASHRAE HVAC', 'link': '/ads.php?md5=123'}])
                self.assertEqual(summary['documents_downloaded'], 1)
                self.assertEqual(summary['pages_processed'], 1)
                self.assertEqual(downloads[0][0], ['https://libgen.bz/ads.php?md5=123'])
                self.assertEqual(len(self.bm25_calls), 1)
                self.assertEqual(self.bm25_calls[0][0], 'raw table')
                self.assertTrue(any('⚠️' in log and 'BM25' in log for log in widget.logs))
                self.assertIn('AI/BM25', widget.updates[-1]['label'])

    async def test_markdown_failure_still_allows_raw_html_bm25(self):
        summary, _, _ = await self.run_pipeline(Response(), [], markdown_result=RuntimeError('parser'),
            bm25_docs=[{'title': 'ASHRAE', 'link': '/book/123'}])
        self.assertEqual(summary['documents_downloaded'], 1)
        self.assertEqual(len(self.bm25_calls), 1)

    async def test_bm25_failure_does_not_crash_pipeline(self):
        summary, downloads, widget = await self.run_pipeline(Response(), RuntimeError('LLM'), bm25_docs=ImportError('BM25 missing'))
        self.assertEqual(summary['pages_processed'], 0)
        self.assertFalse(downloads)
        self.assertTrue(any('BM25 fallback unavailable' in log for log in widget.logs))


if __name__ == '__main__':
    unittest.main()
