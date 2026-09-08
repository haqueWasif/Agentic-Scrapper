"""Strict final candidates and discovery-only navigation routes."""
import unittest
from test_sync_download import SyncDownloadTests, Response, PDF


class StrictGateways(unittest.TestCase):
    setUp = SyncDownloadTests.setUp
    download = SyncDownloadTests.download

    def test_edition_ads_get_503_retries_get(self):
        self.responses = [
            Response(text='<a href="/ads.php?md5=123">Mirror</a><a href="https://gateway.ipfs.io/ipfs/book">GET</a>'),
            Response(text='<a href="/edition.php?id=123">Back</a><a href="/get.php?md5=123">GET</a><a href="https://files.example/fundamentals.pdf">PDF</a>'),
            Response(status=503), Response(),
        ]
        self.assertTrue(self.download('https://libgen.bz/edition.php?id=123'))
        streamed = [url for url, args in self.calls if args.get('stream')]
        self.assertEqual(streamed, ['https://files.example/fundamentals.pdf', 'https://files.example/fundamentals.pdf'])
        self.assertEqual(self.sleeps, [8])
        print('2013 ASHRAE Handbook Fundamentals — fixture telemetry:')
        for message in self.widget.logs:
            if message.startswith(('Final download candidates:', '🛡️ curl_cffi', 'HTTP 503')):
                print(message)

    def test_temporary_server_errors_retry_same_gateway_until_success(self):
        for status in (500, 502, 503, 504):
            self.responses = [Response(status=status), Response()]
            self.calls.clear()
            self.sleeps.clear()
            self.assertIs(self.ns['download_file']('https://libgen.bz/get.php?md5=123', 'test.pdf', 'https://libgen.bz/ads.php'), True)
            self.assertEqual([call[0] for call in self.calls], ['https://libgen.bz/get.php?md5=123'] * 2)
            self.assertEqual(self.sleeps, [8])

    def test_second_temporary_failure_moves_to_the_next_candidate(self):
        url = 'https://libgen.bz/get.php?md5=123'
        self.responses = [Response(status=502), Response(status=502)]
        self.assertEqual(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php', on_status=self.widget.write), 'retry_next')
        self.assertEqual([call[0] for call in self.calls], [url] * 2)
        self.assertEqual(self.sleeps, [8])
        self.assertTrue(any('Gateway unhealthy:' in message for message in self.widget.logs))

    def test_http2_reset_retries_same_get_with_existing_range(self):
        payload = b'%PDF-1.7\n' + b'x' * 4582010
        existing_size = 4581010
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(payload[:existing_size])
        url = 'https://libgen.bz/get.php?md5=refrigeration2014'
        self.responses = [
            OSError('curl error 92: HTTP/2 stream reset by server'),
            Response(status=206, data=payload[existing_size:], headers={'content-length': str(len(payload) - existing_size)}),
        ]
        self.assertIs(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php', on_status=self.widget.write), True)
        self.assertEqual([call[0] for call in self.calls], [url, url])
        self.assertEqual([call[1]['headers']['Range'] for call in self.calls], [
            'bytes=4581010-', 'bytes=4581010-',
        ])
        self.assertEqual(self.calls[0][1]['http_version'], 'v1')
        self.assertTrue(any('HTTP/2 stream reset' in message and 'Action: retry same gateway' in message for message in self.widget.logs))

    def test_502_uses_same_get_and_range_resume(self):
        payload = b'%PDF-1.7\n' + b'x' * 1049000
        existing_size = 1048576
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(payload[:existing_size])
        url = 'https://libgen.bz/get.php?md5=refrigeration2014'
        self.responses = [Response(status=502), Response(
            status=206, data=payload[existing_size:], headers={'content-length': str(len(payload) - existing_size)},
        )]
        self.assertIs(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php', on_status=self.widget.write), True)
        self.assertEqual([call[0] for call in self.calls], [url, url])
        self.assertEqual([call[1]['headers']['Range'] for call in self.calls], [
            'bytes=1048576-', 'bytes=1048576-',
        ])
        self.assertEqual(self.calls[0][1]['http_version'], 'v1')
        self.assertEqual(self.destination.read_bytes(), payload)
        self.assertTrue(any('HTTP 502 after Range request' in message and 'Action: retry same gateway' in message for message in self.widget.logs))

    def test_500_refreshes_cookies_and_keeps_range_for_same_gateway(self):
        payload = b'%PDF-1.7\n' + b'x' * 1049000
        existing_size = 1048576
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(payload[:existing_size])
        url = 'https://libgen.bz/get.php?md5=refrigeration2014'
        self.responses = [Response(status=500, headers={'Content-Type': 'text/html'}), Response(
            status=206, data=payload[existing_size:], headers={'content-length': str(len(payload) - existing_size)},
        )]
        self.assertIs(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php', on_status=self.widget.write), True)
        self.assertEqual([call[1]['headers']['Range'] for call in self.calls], [
            'bytes=1048576-', 'bytes=1048576-',
        ])
        self.assertEqual(self.sleeps, [8])
        self.assertEqual(self.sessions[0].cookies.clear_calls, 1)
        self.assertTrue(any('refresh session cookies + retry same gateway' in message for message in self.widget.logs))

    def test_partial_html_response_cools_down_without_deleting_part(self):
        payload = b'%PDF-1.7\n' + b'x' * 1049000
        existing_size = 1048576
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(payload[:existing_size])
        url = 'https://libgen.bz/get.php?md5=refrigeration2014'
        self.responses = [Response(status=200, headers={'Content-Type': 'text/html'}), Response(
            status=206, data=payload[existing_size:], headers={'content-length': str(len(payload) - existing_size)},
        )]
        self.assertIs(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php'), True)
        self.assertEqual([call[1]['headers']['Range'] for call in self.calls], [
            'bytes=1048576-', 'bytes=1048576-',
        ])
        self.assertEqual(self.sleeps, [8])
        self.assertEqual(self.destination.read_bytes(), payload)

    def test_success_reports_transfer_metrics(self):
        url = 'https://library.lol/main/ashrae.pdf'
        self.responses = [Response()]
        self.assertTrue(self.ns['download_file'](url, 'test.pdf', 'https://library.lol', on_status=self.widget.write))
        metrics = next(message for message in self.widget.logs if message.startswith('PDF: test.pdf'))
        self.assertIn('Time:', metrics)
        self.assertIn('Average speed:', metrics)
        self.assertIn('Retries: 0', metrics)
        self.assertIn('Gateway: library.lol', metrics)

    def test_active_transfer_still_resumes_with_range(self):
        self.responses = [Response(data=PDF[:3000], fail=True), Response(
            status=206, data=PDF[3000:], headers={'content-length': str(len(PDF) - 3000)},
        )]
        self.assertIs(self.ns['download_file']('https://libgen.bz/get.php?md5=123', 'test.pdf', 'https://libgen.bz/ads.php'), True)
        self.assertEqual(self.calls[1][1]['headers']['Range'], 'bytes=3000-')
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_local_ipfs_and_generic_routes_never_stream(self):
        self.responses = [Response(text=''.join(f'<a href="{url}">GET</a>' for url in (
            'http://localhost:8080/ipfs/test.pdf', 'https://gateway.pinata.cloud/book.pdf',
            'http://hidden.onion/get.php?md5=123',
            'https://example.org/download/book', 'https://libgen.bz/file.php?id=123',
        ))), Response(text='No file')]
        self.assertTrue(self.download('https://libgen.bz/edition.php?id=123'))
        streamed = [url for url, args in self.calls if args.get('stream')]
        self.assertEqual(streamed, ['https://gateway.pinata.cloud/book.pdf'])

    def test_2014_refrigeration_206_interruption_resumes_same_gateway(self):
        payload = b'%PDF-1.7\n' + b'x' * 2517581
        interrupted_at = 2516590
        class InterruptedResponse(Response):
            def iter_content(self, chunk_size):
                yield payload[1000:interrupted_at]
                raise ConnectionError('connection reset during active transfer')

        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(payload[:1000])
        self.responses = [InterruptedResponse(status=206, headers={'content-length': str(len(payload)-1000)}),
                          Response(status=206, data=payload[interrupted_at:], headers={'content-length': str(len(payload)-interrupted_at)})]
        url = 'https://libgen.bz/get.php?md5=refrigeration2014'
        self.assertIs(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php', on_status=self.widget.write), True)
        self.assertEqual([call[0] for call in self.calls], [url, url])
        self.assertEqual([call[1]['headers']['Range'] for call in self.calls], ['bytes=1000-', 'bytes=2516590-'])
        self.assertEqual(self.destination.read_bytes(), payload)
        self.assertTrue(any('Existing bytes: 2516590' in message and 'Action: retry same gateway' in message for message in self.widget.logs))
        print('2014 ASHRAE Handbook Refrigeration — controlled interruption test:')
        for message in self.widget.logs:
            if message.startswith(('Transfer state:', 'Download interrupted:', '📡 Gateway response:')):
                print(message)

    def test_one_active_failure_retries_same_gateway_until_success(self):
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(PDF[:3000])
        self.responses = [ConnectionError('reset'), Response(status=206, data=PDF[3000:])]
        url = 'https://libgen.bz/get.php?md5=123'
        self.assertIs(self.ns['download_file'](url, 'test.pdf', 'https://libgen.bz/ads.php'), True)
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(call[0] == url and call[1]['headers']['Range'] == 'bytes=3000-' for call in self.calls))
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_truncated_stream_without_exception_resumes(self):
        self.responses = [Response(data=PDF[:3000], headers={'content-length': str(len(PDF))}),
                          Response(status=206, data=PDF[3000:])]
        self.assertIs(self.ns['download_file']('https://libgen.bz/get.php', 'test.pdf', 'https://libgen.bz/ads.php'), True)
        self.assertEqual(self.calls[1][1]['headers']['Range'], 'bytes=3000-')
        self.assertEqual(self.destination.read_bytes(), PDF)

    def test_rejected_responses_do_not_retry_even_with_partial(self):
        self.part.parent.mkdir(parents=True, exist_ok=True)
        self.part.write_bytes(PDF[:3000])
        for response in (Response(status=403), Response(status=404),
                         Response(headers={'Content-Type': 'application/json'})):
            self.calls.clear()
            self.responses = [response]
            self.assertEqual(self.ns['download_file']('https://libgen.bz/get.php', 'test.pdf', 'https://libgen.bz/ads.php'), 'retry_next')
            self.assertEqual(len(self.calls), 1)
            self.assertEqual(self.part.read_bytes(), PDF[:3000])
