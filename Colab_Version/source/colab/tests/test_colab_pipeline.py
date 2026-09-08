"""Headless integration and hot-path regressions, without external requests."""
import ast
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock

from app.download_progress import DownloadProgress, ProgressPolicy, METRICS
from colab.pipeline_runner import ColabConfig, run_colab_pipeline, shared_pipeline
from colab.storage import DriveSnapshots
from app.local_library import LocalLibrary, retrieve_pdf_evidence

ROOT = Path(__file__).resolve().parents[2]
MIB = 1024**2


class ProgressTests(unittest.TestCase):
    def test_100_mib_in_256_kib_chunks_uses_twelve_progress_writes(self):
        writes, events, now = [], [], [0.0]
        tracker = DownloadProgress(lambda size: writes.append(size), lambda *args: events.append(args), clock=lambda: now[0])
        for step in range(1, 401):
            now[0] += .001
            tracker(step * 256 * 1024, 100 * MIB)
        self.assertEqual(len(writes), 12)
        self.assertEqual(tracker.downloaded, 100 * MIB)
        self.assertEqual(len(events), 1)

    def test_time_gate_is_independent_of_byte_gate(self):
        writes, now = [], [0.0]
        tracker = DownloadProgress(lambda size: writes.append(size), lambda *_: None, clock=lambda: now[0])
        tracker(100, 1000)
        now[0] = 2
        tracker(200, 1000)
        self.assertEqual(writes, [200])

    def test_100_callbacks_under_half_second_keep_latest_bytes(self):
        now, events = [0.0], []
        tracker = DownloadProgress(lambda *_: True, lambda *args: events.append(args), clock=lambda: now[0])
        for size in range(100):
            now[0] = size * .004
            tracker(size, 100)
        self.assertEqual(len(events), 1)
        self.assertEqual(tracker.downloaded, 99)
        now[0] = .5
        tracker(100, 100)
        self.assertEqual(events[-1], (100, 100))

    def test_failed_write_does_not_advance_watermark_and_checkpoint_forces(self):
        persist = Mock(side_effect=[False, True, True])
        tracker = DownloadProgress(persist, lambda *_: None, clock=lambda: 0)
        tracker(8 * MIB, 20 * MIB)
        self.assertEqual(tracker.last_bytes, 0)
        tracker(8 * MIB + 1, 20 * MIB)
        tracker(9 * MIB, 20 * MIB)
        tracker.checkpoint()
        self.assertEqual(persist.call_count, 3)
        self.assertEqual(tracker.last_bytes, 9 * MIB)


class CoreFixture(unittest.TestCase):
    def pass_network(self):
        network = self.core.NetworkManager(ROOT)
        network.settings = replace(network.settings, defer_download_retries=True,
                                   new_download_min_seconds=0, new_download_max_seconds=0)
        network.cooldown = Mock(side_effect=AssertionError('Worker must not wait for retry'))
        return network

    def test_failed_partial_releases_worker_then_resumes_in_next_pass(self):
        network = self.pass_network()
        seen = []
        def interrupted(**kwargs):
            yield b'a' * 4096
            raise ConnectionError('interrupted')
        def get(url, **kwargs):
            seen.append((url, dict(kwargs['headers'])))
            if len(seen) == 1:
                return SimpleNamespace(status_code=200, headers={'content-length': '8192'}, iter_content=interrupted)
            resumed = 'Range' in kwargs['headers']
            return SimpleNamespace(status_code=206 if resumed else 200,
                                   headers={'content-length': '4096' if resumed else '8192'},
                                   iter_content=lambda **_: iter([b'a' * (4096 if resumed else 8192)]))
        network.session = lambda: SimpleNamespace(get=get)
        candidates = [dict(filename=f'{name}.pdf', mirrors=[f'https://example.test/{name}.pdf'],
                           query='ASHRAE', document_id=name) for name in ('A', 'B')]
        async def run():
            coordinator = self.core._StreamlitDownloadCoordinator(network, lambda *_: None)
            try:
                for candidate in candidates:
                    await coordinator.enqueue(candidate)
                failed = await coordinator.drain_until_idle()
                self.assertEqual(len(failed), 1)
                self.assertEqual([url for url, _ in seen], [c['mirrors'][0] for c in candidates])
                self.assertEqual(self.core._download_paths('A.pdf')[2].stat().st_size, 4096)
                backlog = self.core.GlobalRecoveryBacklog()
                backlog.add_failed_attempt(failed[0], max_attempts=5)
                for candidate in backlog.take_round(max_attempts=5):
                    await coordinator.enqueue(candidate, recovery=True)
                self.assertEqual(await coordinator.drain_until_idle(), [])
                self.assertEqual(coordinator.completed, 2)
                row = coordinator.presentation.worker_rows()[0]
                self.assertEqual((row['bytes_downloaded'], row['total_bytes']), (8192, 8192))
            finally:
                coordinator.close()
        network.settings = replace(network.settings, max_workers=1)
        with patch.object(self.core, '_schedule_validation', return_value=True):
            asyncio.run(run())
        self.assertEqual(len(seen), 3)
        self.assertEqual(seen[-1][1]['Range'], 'bytes=4096-')
        self.assertEqual(self.core._download_paths('A.pdf')[1].read_bytes(), b'a' * 8192)
        network.cooldown.assert_not_called()

    def test_mirror_alternatives_wait_until_later_pass(self):
        network = self.pass_network()
        network.session = lambda: SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(
            status_code=200, text='<a href="https://example.test/a.pdf">GET</a><a href="https://example.test/b.pdf">GET</a>'))
        with patch.object(self.core, 'download_file', return_value='retry_next') as download:
            for route_pass in (0, 1):
                result = self.core.parse_mirror_and_download('https://example.test/mirror', 'A.pdf', False,
                    network=network, on_status=lambda *_: None, _attempt_context={'route_pass': route_pass})
                self.assertEqual(result, 'retry_next')
            self.assertEqual([call.args[0] for call in download.call_args_list],
                             ['https://example.test/a.pdf', 'https://example.test/b.pdf'])

    def test_worker_tries_one_top_level_mirror_per_pass(self):
        network = self.pass_network()
        candidate = dict(filename='A.pdf', mirrors=['https://example.test/one', 'https://example.test/two'])
        with patch.object(self.core, 'parse_mirror_and_download', return_value='retry_next') as parse:
            for attempt in (1, 2):
                result = self.core._download_worker(dict(candidate, _document_attempt=attempt), network, 1)
                self.assertFalse(result['success'])
            self.assertEqual([call.args[0] for call in parse.call_args_list], candidate['mirrors'])
        network.cooldown.assert_not_called()

    def test_rate_limit_returns_worker_and_preserves_recovery_deadline(self):
        for route in ('book.pdf', 'mirror'):
            # Exercise both response paths, not the second request's inherited
            # host cooldown from the first iteration.
            network = self.pass_network()
            self.addCleanup(network.close)
            network.session = lambda: SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(
                status_code=429, headers={'Retry-After': '120'}))
            candidate = dict(filename='A.pdf', query='ASHRAE', mirrors=[f'https://example.test/{route}'])
            result = self.core._download_worker(candidate, network, 1)
            self.assertFalse(result['success'])
            self.assertGreater(result['retry_after_until'], self.core.time.time() + 110)
            restored = self.core._recovery_candidates('ASHRAE')
            self.assertEqual(len(restored), 1)
            backlog = self.core.GlobalRecoveryBacklog()
            backlog.add_restored(restored[0])
            self.assertEqual(backlog.not_before, result['retry_after_until'])
            network.cooldown.assert_not_called()

    def test_backpressure_drain_preserves_failures_for_recovery(self):
        network = self.pass_network()
        failure = dict(success=False, candidate={'filename': 'A.pdf'}, attempt=1)
        with patch.object(self.core, 'GlobalDownloadQueue') as factory:
            queue = factory.return_value
            queue.try_enqueue.side_effect = [False, True]
            queue.drain_outcomes.side_effect = [[SimpleNamespace(candidate=failure['candidate'], result=failure)], [], [], []]
            coordinator = self.core._StreamlitDownloadCoordinator(network, lambda *_: None)
            asyncio.run(coordinator.enqueue({'filename': 'B.pdf'}))
            self.assertEqual(coordinator.drain(), [failure])
            coordinator.close()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.core = shared_pipeline()
        for name, value in dict(DATA_DIRECTORY=self.root, DOWNLOAD_DIRECTORY=self.root / 'ASHRAE_Files',
                                DOWNLOAD_STATE_FILE=self.root / 'downloads.json').items():
            context = patch.object(self.core, name, value)
            context.start()
            self.addCleanup(context.stop)

    def network(self, response):
        get = Mock(return_value=response)
        network = SimpleNamespace(settings=SimpleNamespace(max_request_retries_per_attempt=1, max_stall_seconds=60),
                                  session=lambda: SimpleNamespace(get=get), request_options=lambda _: {},
                                  result=Mock(), gateway_result=Mock(), progress_policy=ProgressPolicy())
        return network, get

    def test_resume_uses_57_mib_part_not_49_mib_ledger(self):
        _, destination, part = self.core._download_paths('book.pdf')
        with part.open('wb') as handle:
            handle.truncate(57 * MIB)
        self.core._update_download_state('book.pdf', '', 'PARTIAL', size=49 * MIB)
        response = SimpleNamespace(status_code=206, headers={'content-length': str(MIB)}, iter_content=lambda **_: iter([b'x' * MIB]))
        network, get = self.network(response)
        self.assertIs(self.core.download_file('https://example.test/book', 'book.pdf', '', network=network, on_status=lambda *_: None), True)
        self.assertEqual(get.call_args.kwargs['headers']['Range'], f'bytes={57 * MIB}-')
        self.assertEqual(destination.stat().st_size, 58 * MIB)
        self.assertFalse(part.exists())
        self.assertEqual(self.core._download_state_entry('book.pdf')['status'].upper(), 'COMPLETED')

    def test_failure_forces_partial_and_preserves_bytes(self):
        def chunks(**kwargs):
            yield b'x' * (256 * 1024)
            raise ConnectionError('fixture interrupted stream')
        network, _ = self.network(SimpleNamespace(status_code=200, headers={'content-length': str(MIB)}, iter_content=chunks))
        self.assertEqual(self.core.download_file('https://example.test/book', 'book.pdf', '', network=network, on_status=lambda *_: None), 'retry_next')
        entry = self.core._download_state_entry('book.pdf')
        self.assertEqual(entry['status'].upper(), 'PARTIAL')
        self.assertEqual(entry['size'], 256 * 1024)
        self.assertEqual(Path(entry['part_path']).stat().st_size, 256 * 1024)

    def test_five_workers_keep_terminal_states_with_reduced_persistence(self):
        before = METRICS.snapshot()
        def worker(number):
            name = f'{number}.pdf'
            self.core._update_download_state(name, '', 'DOWNLOADING', size=0)
            tracker = DownloadProgress(lambda size: self.core._update_download_state(name, '', 'DOWNLOADING', size=size),
                                       lambda *_: None, clock=lambda: 0)
            for chunk in range(1, 401):
                tracker(chunk * 256 * 1024, 100 * MIB)
            self.core._update_download_state(name, '', 'COMPLETED', size=100 * MIB)
        with ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(worker, range(5)))
        entries = json.loads(self.core.DOWNLOAD_STATE_FILE.read_text())['downloads']
        self.assertEqual(len(entries), 5)
        self.assertTrue(all(entry['status'] == 'COMPLETED' and entry['size'] == 100 * MIB for entry in entries.values()))
        self.assertEqual(METRICS.snapshot()['ledger_writes'] - before['ledger_writes'], 70)

    def test_drive_never_enters_chunk_handler_and_partials_are_opt_in(self):
        config = ColabConfig(local_work_dir=str(self.root), use_google_drive=True, drive_output_dir=str(self.root.parent / (self.root.name + '-drive')))
        with tempfile.TemporaryDirectory() as persistent:
            config = replace(config, drive_output_dir=persistent)
            sync = DriveSnapshots(config, self.core)
            _, _, part = self.core._download_paths('book.pdf')
            part.write_bytes(b'x' * 4096)
            with patch('colab.storage.copy_prefix') as copy:
                tracker = DownloadProgress(lambda size: self.core._update_download_state('book.pdf', '', 'DOWNLOADING', size=size), lambda *_: None)
                for size in range(100):
                    tracker(size, 100)
                copy.assert_not_called()
                sync.sync(final=True)
                copy.assert_not_called()
            self.assertFalse((Path(persistent) / 'ASHRAE_Files/book.pdf.part').exists())

    def test_drive_roundtrip_rebases_state_and_restores_real_partial(self):
        with tempfile.TemporaryDirectory() as persistent, tempfile.TemporaryDirectory() as new_local:
            config = ColabConfig(local_work_dir=str(self.root), use_google_drive=True, drive_output_dir=persistent, checkpoint_partials_to_drive=True)
            _, _, part = self.core._download_paths('book.pdf')
            part.write_bytes(b'x' * 4096)
            self.core._update_download_state('book.pdf', '', 'PARTIAL', size=2048)
            sync = DriveSnapshots(config, self.core)
            sync.sync(final=True)
            restored = DriveSnapshots(replace(config, local_work_dir=new_local), self.core)
            restored.restore()
            ledger = json.loads((Path(new_local) / 'downloads.json').read_text())
            self.assertEqual(Path(ledger['downloads']['book.pdf']['part_path']).stat().st_size, 4096)
            with patch('colab.storage.copy_prefix') as copy:
                restored.restore()
                copy.assert_not_called()

    def test_import_does_not_import_streamlit(self):
        program = "from colab.pipeline_runner import shared_pipeline; import sys; shared_pipeline(); assert 'streamlit' not in sys.modules"
        result = subprocess.run([sys.executable, '-X', 'utf8', '-c', program], cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_reuse_skips_network_and_revalidates_each_query(self):
        from pypdf import PdfWriter
        path = self.root / 'Legacy Handbook.pdf'
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.write(path)
        preview = dict(text='ASHRAE publisher geothermal ' * 120, metadata={}, page_count=80,
                       pages_sampled=[1, 80], pages_with_text=80, characters=3500,
                       toc_pages=[], pages_scanned=80, extraction_quality='good', error=None)
        queries = []
        async def graph(state, report):
            queries.append(state['source_metadata']['query'])
            return {'validation': dict(status='APPROVED', approved=True, score=90,
                                      ashrae_identity=True, query_relevant=True)}
        network = self.core.NetworkManager(ROOT)
        network.begin_download = Mock(side_effect=AssertionError('local match must not use network'))
        try:
            with patch('app.local_library.retrieve_pdf_evidence', return_value=preview), patch.object(self.core, 'invoke_pdf_validation_graph', graph):
                for query in ['geothermal', 'chillers']:
                    candidate = dict(filename=path.name, local_path=str(path), query=query, mirrors=[], source_url='', run_id='test')
                    outcome = self.core._download_worker(candidate, network, 1)
                    self.assertTrue(outcome['local_reused'])
                    self.core._stop_validation_queue()
            network.begin_download.assert_not_called()
            self.assertEqual(queries, ['geothermal', 'chillers'])
            self.assertTrue(path.exists())
            self.assertEqual(self.core._download_state_entry(path.name)['validation_status'], 'APPROVED')
        finally:
            self.core._stop_validation_queue()
            network.close()

    def test_chunk_cancellation_forces_partial_without_retry(self):
        import threading
        stop = threading.Event()
        def chunks(**kwargs):
            yield b'x' * 4096
            stop.set()
            yield b'x' * 4096
        network, get = self.network(SimpleNamespace(status_code=200, headers={'content-length': '8192'}, iter_content=chunks))
        network.stop_event = stop
        with self.assertRaises(InterruptedError):
            self.core.download_file('https://example.test/file', 'cancel.pdf', '', network=network, on_status=lambda _: None)
        self.assertEqual(get.call_count, 1)
        entry = self.core._download_state_entry('cancel.pdf')
        self.assertEqual(entry['status'].upper(), 'PARTIAL')
        self.assertEqual(entry['size'], 4096)

    def test_legacy_library_does_not_satisfy_an_unvalidated_query_target(self):
        _, path, _ = self.core._download_paths('legacy.pdf')
        path.write_bytes(b'%PDF legacy')
        self.assertEqual(self.core._completed_download_count(), 1)
        self.assertEqual(self.core._completed_download_count(query='ASHRAE Handbook'), 0)
        self.core._update_download_state('legacy.pdf', '', 'COMPLETED', candidate={'query': 'ASHRAE Handbook'})
        self.assertEqual(self.core._completed_download_count(query='ASHRAE Handbook'), 1)
        self.assertEqual(self.core._completed_download_count(query='Chillers'), 0)


class NotebookTests(unittest.TestCase):
    def test_progress_output_formats_unknown_completed_and_idle(self):
        from colab.pipeline_runner import NotebookProgress
        snapshot = SimpleNamespace(discovery_page=3, max_pages=50, stage1_approved=48,
            stage1_rejected=2, downloaded=135, recovery_backlog=216,
            stage2_active=0, stage2_queued=0, stage2_pending=0, stage2_approved=0, stage2_rejected=0,
            worker_rows=[dict(worker=1, state='COMPLETED', bytes_downloaded=7*MIB, total_bytes=7*MIB),
                         dict(worker=2, state='DOWNLOADING', bytes_downloaded=MIB, total_bytes=0),
                         dict(worker=3, state='IDLE', bytes_downloaded=9*MIB, total_bytes=10*MIB)])
        display = Mock()
        display_module = SimpleNamespace(display=display)
        with patch.dict(sys.modules, {'IPython': SimpleNamespace(display=display_module),
                                      'IPython.display': display_module}):
            NotebookProgress()(snapshot)
        output = display.call_args.args[0]['text/plain']
        self.assertNotIn('Worker 1:', output)
        self.assertIn('Worker 2: DOWNLOADING  1.0 MiB / unknown', output)
        self.assertNotIn('Worker 3:', output)
        self.assertNotIn('9.0', output)

    def test_worker_counts_require_positive_integers(self):
        for field in ('download_workers', 'stage2_workers'):
            for value in (0, -1, True, 2.5, '50'):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    ColabConfig(**{field: value})
        config = ColabConfig(download_workers=50, stage2_workers=10)
        self.assertEqual((config.download_workers, config.stage2_workers), (50, 10))

    def test_larger_worker_override_reaches_headless_pipeline(self):
        core = shared_pipeline()
        counts = []
        async def no_search(*args, **kwargs):
            return None
        def snapshot(value):
            self.assertTrue(core._NETWORK_SETTINGS.defer_download_retries)
            counts.append((len(value.worker_rows), core._validation_queue().max_workers))
        with tempfile.TemporaryDirectory() as temporary:
            config = ColabConfig(local_work_dir=temporary, download_workers=50, stage2_workers=10)
            with patch.object(core, '_fetch_zenrows_search', no_search), patch.object(core, '_fetch_direct_search', no_search), patch.object(core, '_read_cached_search', return_value=None):
                asyncio.run(run_colab_pipeline('ASHRAE', 1, 2, config=config, on_snapshot=snapshot))
        self.assertTrue(counts)
        self.assertTrue(all(value == (50, 10) for value in counts))
        self.assertEqual(core.NetworkManager._read_settings(ROOT / 'config/settings.yaml').max_workers, 5)

    def test_valid_notebook_and_executable_configuration(self):
        import nbformat
        notebook = nbformat.read(ROOT / 'colab' / 'Agentic_Scraper_Colab.ipynb', as_version=4)
        nbformat.validate(notebook)
        namespace = {'DISTRIBUTED_MODE': False}
        for cell in notebook.cells:
            if cell.cell_type == 'code':
                compile(cell.source, '<notebook>', 'exec', flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
                if cell.source.startswith('QUERY ='):
                    exec(cell.source, namespace)
        namespace['USE_GOOGLE_DRIVE'] = False
        for cell in notebook.cells:
            if cell.cell_type == 'code' and cell.source.startswith('from app.download_progress'):
                exec(cell.source, namespace)
        self.assertEqual(namespace['CONFIG'].download_workers, 5)
        install = [cell.source for cell in notebook.cells if cell.cell_type == 'code' and "'pip', 'install'" in cell.source]
        self.assertEqual(len(install), 1)
        run = Mock()
        exec(install[0], {'subprocess': SimpleNamespace(run=run), 'sys': sys, 'REPO_DIR': ROOT, 'DISTRIBUTED_MODE': False})
        self.assertIn(str(ROOT / 'requirements.txt'), run.call_args.args[0])
        run.reset_mock()
        exec(install[0], {'subprocess': SimpleNamespace(run=run), 'sys': sys, 'REPO_DIR': ROOT, 'DISTRIBUTED_MODE': True})
        self.assertEqual(run.call_count,2)
        self.assertIn(str(ROOT / 'colab/requirements-distributed.txt'), run.call_args.args[0])

    def test_mocked_headless_pipeline_without_session_state(self):
        core = shared_pipeline()
        with tempfile.TemporaryDirectory() as temporary:
            # Real queue/coordinator/ledger; only external search is replaced.
            async def no_search(*args, **kwargs):
                return None
            config = ColabConfig(local_work_dir=temporary)
            with patch.object(core, '_fetch_zenrows_search', no_search), patch.object(core, '_fetch_direct_search', no_search), patch.object(core, '_read_cached_search', return_value=None):
                result = asyncio.run(run_colab_pipeline('ASHRAE', 1, 2, config=config, on_snapshot=lambda _: None))
            self.assertEqual(result['pages_processed'], 0)
            self.assertTrue((Path(temporary) / 'run_summary.json').exists())

    def test_fresh_primary_failure_enters_global_recovery(self):
        core = shared_pipeline()
        attempts = []
        async def search(*args, **kwargs):
            return '<html>fixture</html>'
        async def graph(state, on_status):
            return {'extracted_documents': [{'id': 1, 'title': 'ASHRAE Test', 'link': 'https://example.test/book', 'score': 90}]}
        async def markdown(*args):
            return 'ASHRAE Test'
        def worker(candidate, network, worker_id):
            attempt = candidate.get('_document_attempt', 1)
            attempts.append(attempt)
            return dict(success=attempt == 2, candidate=candidate, attempt=attempt, permanent=False)
        from contextlib import ExitStack
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            for name, replacement in {
                '_fetch_zenrows_search': search, '_fetch_direct_search': search,
                '_extract_search_documents': lambda *args: [{'title': 'ASHRAE Test', 'link': 'https://example.test/book', 'text': 'ASHRAE Test'}],
                '_extract_search_mirror_rows': lambda *args: [],
                '_document_mirror_links': lambda *args: ['https://example.test/book'],
                'invoke_scraper_graph': graph, 'extract_markdown': markdown,
                '_download_worker': worker, '_restore_pending_validation_jobs': lambda **kwargs: 0,
            }.items():
                stack.enter_context(patch.object(core, name, replacement))
            original = core.NetworkManager._read_settings
            stack.enter_context(patch.object(core.NetworkManager, '_read_settings', side_effect=lambda path: replace(original(path), recovery_round_delay_seconds=0)))
            result = asyncio.run(run_colab_pipeline('ASHRAE Test', 1, 2,
                                 config=ColabConfig(local_work_dir=temporary), on_snapshot=lambda _: None))
        self.assertEqual(attempts, [1, 2])
        self.assertEqual(result['new_pdfs_downloaded'], 1)

    def test_streamlit_frontend_starts_and_reruns(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(ROOT / 'app.py'), default_timeout=60).run()
        self.assertFalse(app.exception)
        self.assertEqual(app.title[0].value, 'ASHRAE Intelligent Scraper')
        app.text_input[0].set_value('ASHRAE Handbook').run()
        self.assertFalse(app.exception)


class LibraryEvidenceTests(unittest.TestCase):
    def test_unchanged_legacy_index_does_not_reparse_and_matches_other_filename(self):
        from pypdf import PdfWriter
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / 'Old_book_name.pdf'
            writer = PdfWriter()
            writer.add_blank_page(width=72, height=72)
            writer.add_metadata({'/Title': 'ASHRAE Handbook 2014'})
            writer.write(path)
            library = LocalLibrary(root)
            library.refresh()
            self.assertEqual(library.match('ASHRAE Handbook 2014'), path)
            with patch('app.local_library.PdfReader', side_effect=AssertionError('unchanged PDF reparsed')):
                LocalLibrary(root).refresh()
            self.assertTrue(path.exists())
            self.assertIsNone(library.match('ASHRAE Handbook 2015'))

    def test_all_pages_scanned_evidence_bounded_and_late_query_retrieved(self):
        pages = [Mock(extract_text=Mock(return_value=('front matter ' * 300))) for _ in range(80)]
        pages[-1].extract_text.return_value = 'ASHRAE copyright publisher. Special geothermal cooling technology. ' * 100
        with tempfile.TemporaryDirectory() as temporary, patch('app.local_library.PdfReader', return_value=SimpleNamespace(pages=pages, metadata={})):
            source = Path(temporary) / 'book.pdf'
            source.touch()
            evidence = retrieve_pdf_evidence(source, 'geothermal cooling')
        self.assertEqual(evidence['pages_scanned'], 80)
        self.assertLessEqual(len(evidence['text']), 12000)
        self.assertIn(80, evidence['pages_sampled'])
        self.assertTrue(all(page.extract_text.call_count == 1 for page in pages))

    def test_identity_and_query_are_independent_required_gates(self):
        from app.agents.orchestrator import _pdf_validation_result
        base = dict(approved=True, score=90, confidence='high', needs_more_text=False,
                    ashrae_identity=True, query_relevant=True)
        self.assertEqual(_pdf_validation_result(base, extraction_quality='good', require_identity=True)['status'], 'APPROVED')
        self.assertEqual(_pdf_validation_result({**base, 'ashrae_identity': False}, extraction_quality='good', require_identity=True)['status'], 'REJECTED')
        self.assertEqual(_pdf_validation_result({**base, 'query_relevant': None}, extraction_quality='good', require_identity=True)['status'], 'PENDING')
