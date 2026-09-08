"""Transfer completion is distinct from structural integrity and semantic results."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import time
import unittest
from unittest.mock import patch, Mock

from app.fair_download_queue import FairDownloadQueue
from app.network_manager import NetworkManager
from app.download_integrity import verified_download
from colab.pipeline_runner import shared_pipeline
from colab.scheduler_benchmark import pdf_bytes

ROOT=Path(__file__).resolve().parents[1]


class IntegrityLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.core=shared_pipeline()
        self.core._stop_validation_queue(cancel_pending=True)
        temporary=tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name)
        self.net=NetworkManager(ROOT)
        self.net.settings=replace(self.net.settings,scheduler_admission=True,defer_download_retries=True,
            new_download_min_seconds=0,new_download_max_seconds=0,validation_workers=2)
        self.addCleanup(self.net.close)
        for name,value in dict(DATA_DIRECTORY=self.root,DOWNLOAD_DIRECTORY=self.root/'ASHRAE_Files',
            DOWNLOAD_STATE_FILE=self.root/'downloads.json',_NETWORK_SETTINGS=self.net.settings).items():
            ctx=patch.object(self.core,name,value); ctx.start(); self.addCleanup(ctx.stop)
        self.addCleanup(lambda:self.core._stop_validation_queue(cancel_pending=True))
        self.job=dict(document_id='A',filename='A.pdf',mirrors=['https://fixture.test/A.pdf'],query='ASHRAE')

    def transferred(self, content=None):
        _,path,_=self.core._download_paths('A.pdf')
        path.write_bytes(content if content is not None else pdf_bytes(4096))
        self.core._update_download_state('A.pdf','','INTEGRITY_PENDING',candidate=self.job,
                                        document_attempt=1,completed=False)
        return path

    def state(self):
        return self.core._download_state_entry('A.pdf')

    def validate(self):
        self.core._run_validation_job(dict(filename='A.pdf'),1)

    def test_transfer_waiting_for_integrity_is_not_verified_or_deduped(self):
        path=self.transferred()
        self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
        self.assertFalse(self.state()['completed'])
        self.assertEqual(self.core._completed_download_count('ASHRAE'),0)
        self.assertFalse(verified_download(self.state(),path))
        with patch.object(self.core,'_pdf_sha256',side_effect=AssertionError('Unverified dedupe')):
            self.assertIsNone(self.core._record_completed_content_hash('A.pdf'))
            with patch.object(self.core,'_NETWORK_SETTINGS',None):
                self.assertEqual(self.core._completed_download_count('ASHRAE'),0)
                self.assertIsNone(self.core._record_completed_content_hash('A.pdf'))

    def test_real_binary_worker_returns_transfer_success_but_pending_integrity(self):
        payload=pdf_bytes(4096)
        response=SimpleNamespace(status_code=200,headers={'content-length':str(len(payload)),
            'content-type':'application/pdf'},iter_content=lambda **kw:iter([payload]),close=lambda:None)
        full=SimpleNamespace(contains=lambda key:False,try_enqueue=lambda *a,**kw:False)
        with patch.object(self.net,'session',return_value=SimpleNamespace(get=lambda *a,**kw:response)), patch.object(
                self.core,'_validation_queue',return_value=full), patch.object(
                self.core,'check_pdf_integrity',side_effect=AssertionError('Network-side PDF parsing')):
            result=self.core._download_worker(self.job,self.net,1)
        self.assertTrue(result['success'])
        _,path,part=self.core._download_paths('A.pdf')
        self.assertFalse(part.exists()); self.assertEqual(path.read_bytes(),payload)
        self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
        self.assertFalse(self.state()['completed'])
        self.assertEqual(self.core._completed_download_count('ASHRAE'),0)

    def test_full_queue_retains_pending_bytes(self):
        path=self.transferred(); payload=path.read_bytes()
        full=SimpleNamespace(contains=lambda key:False,try_enqueue=lambda *a,**kw:False)
        with patch.object(self.core,'_validation_queue',return_value=full), patch.object(
                self.core,'check_pdf_integrity',side_effect=AssertionError('Network-side parsing')):
            self.assertFalse(self.core._schedule_validation('A.pdf'))
        self.assertEqual(path.read_bytes(),payload)
        self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
        self.assertEqual(self.state()['integrity_status'],'PENDING')
        self.assertEqual(self.core._completed_download_count(),0)

    def test_restart_restores_pending_without_network(self):
        self.transferred()
        # Re-read durable state with no in-memory worker state or network work.
        self.assertEqual(self.core._recovery_candidates('ASHRAE'),[])
        with patch.object(self.net,'session',side_effect=AssertionError('Unexpected network')), patch.object(
                self.core,'_validate_pdf_stage2',return_value={'status':'PENDING'}):
            self.assertEqual(self.core._restore_pending_validation_jobs(),1)
            self.core._stop_validation_queue()
        self.assertEqual(self.state()['status'],'COMPLETED')
        self.assertEqual(self.core._completed_download_count('ASHRAE'),1)

    def test_truncated_pdf_becomes_partial_recovery_not_completed(self):
        path=self.transferred(pdf_bytes(4096)[:-100])
        original=path.read_bytes()
        with patch.object(self.core,'_validate_pdf_stage2',side_effect=AssertionError('Semantic validation of corrupt PDF')):
            self.validate()
        state=self.state()
        self.assertEqual(state['integrity_status'],'PDF_TRUNCATED')
        self.assertEqual(state['status'],'PARTIAL')
        self.assertFalse(state['completed']); self.assertFalse(path.exists())
        self.assertEqual(self.core._download_paths('A.pdf')[2].read_bytes(),original)
        self.assertEqual(self.core._completed_download_count('ASHRAE'),0)
        self.assertEqual(len(self.core._recovery_candidates('ASHRAE')),1)

    def test_existing_partial_is_preserved_when_final_pdf_is_truncated(self):
        self.transferred(pdf_bytes(4096)[:-100])
        part=self.core._download_paths('A.pdf')[2]; part.write_bytes(b'authoritative partial')
        self.validate()
        self.assertEqual(part.read_bytes(),b'authoritative partial')
        self.assertEqual(self.state()['status'],'PARTIAL')
        self.assertEqual(len(list((self.root/'integrity_failed').glob('*.invalid'))),1)

    def test_corrupt_pdf_is_retained_as_evidence_for_normal_recovery(self):
        self.transferred(b'not a PDF' * 1024)
        with patch.object(self.core,'_validate_pdf_stage2',side_effect=AssertionError('Invalid semantic input')):
            self.validate()
        self.assertEqual(self.state()['status'],'FAILED_FOR_ROUND')
        self.assertFalse(self.state()['completed'])
        self.assertEqual(self.core._completed_download_count('ASHRAE'),0)
        self.assertEqual(len(list((self.root/'integrity_failed').glob('*.invalid'))),1)

    def test_valid_pdf_promotes_atomically_before_semantic_stage2(self):
        path=self.transferred()
        def semantic(*a,**kw):
            self.assertTrue(verified_download(self.state(),path))
            self.assertEqual(self.core._completed_download_count('ASHRAE'),1)
            return {'status':'PENDING'}
        with patch.object(self.core,'_validate_pdf_stage2',side_effect=semantic) as semantic_call:
            self.validate()
        semantic_call.assert_called_once()
        self.assertEqual(self.state()['status'],'COMPLETED')
        self.assertEqual(self.state()['integrity_status'],'VALID')

    def test_network_slot_is_free_while_integrity_is_blocked(self):
        entered=threading.Event(); release=threading.Event()
        original=self.core.check_pdf_integrity
        def gate(path):
            entered.set(); self.assertTrue(release.wait(5)); return original(path)
        def transfer(*a,**kw):
            self.transferred(); return True
        queue=FairDownloadQueue(lambda job,w:self.core._download_worker(job,self.net,w),network=self.net,max_workers=1)
        try:
            with patch.object(self.core,'parse_mirror_and_download',side_effect=transfer), patch.object(
                    self.core,'check_pdf_integrity',side_effect=gate), patch.object(
                    self.core,'_validate_pdf_stage2',return_value={'status':'PENDING'}):
                queue.try_enqueue(self.job)
                self.assertTrue(entered.wait(3))
                deadline=time.monotonic()+2
                while not queue.idle and time.monotonic()<deadline: time.sleep(.005)
                self.assertTrue(queue.idle); self.assertEqual(queue.active,0)
                self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
                self.assertEqual(self.core._completed_download_count('ASHRAE'),0)
                outcome=queue.drain_outcomes()[0]
                self.assertTrue(outcome.result['success'])  # Transfer counter, not verified counter.
                release.set(); self.core._stop_validation_queue()
        finally:
            release.set(); queue.close(cancel_pending=True)

    def test_semantic_timeout_does_not_undo_valid_download(self):
        self.transferred()
        with patch.object(self.core,'_validate_pdf_stage2',side_effect=TimeoutError('semantic timeout')):
            self.validate()
        self.assertEqual(self.state()['status'],'COMPLETED')
        self.assertEqual(self.state()['integrity_status'],'VALID')
        self.assertEqual(self.state()['validation_status'],'PENDING')
        self.assertEqual(self.core._completed_download_count('ASHRAE'),1)

    def test_semantic_storage_move_and_report_failure_keep_integrity_success(self):
        self.transferred()
        preview=dict(text='ASHRAE ' * 1000,metadata={},page_count=1,pages_sampled=[1],
            pages_with_text=1,characters=7000,toc_pages=[],pages_scanned=1,extraction_quality='good',error=None)
        async def semantic(*a,**kw):
            return {'validation':dict(status='APPROVED',approved=True,score=90)}
        with patch('app.local_library.retrieve_pdf_evidence',return_value=preview), patch.object(
                self.core,'invoke_pdf_validation_graph',side_effect=semantic), patch.object(
                self.core,'write_validation_report',side_effect=TimeoutError('report failed after move')):
            self.validate()
        self.assertTrue((self.root/'approved/A.pdf').exists())
        self.assertTrue(verified_download(self.state()))
        self.assertEqual(self.state()['validation_status'],'PENDING')
        self.assertEqual(self.core._completed_download_count('ASHRAE'),1)
        with patch.object(self.core,'_validate_pdf_stage2',return_value={'status':'PENDING'}):
            self.assertEqual(self.core._restore_pending_validation_jobs(),1)
            self.core._stop_validation_queue()
        self.assertTrue(verified_download(self.state()))

    def test_orphan_and_old_unverified_completed_files_are_reconciled_pending(self):
        path=self.transferred()
        for previous in ('COMPLETED','PARTIAL'):
            with self.core._DOWNLOAD_STATE_LOCK:
                state=self.core._load_download_state()
                state['downloads']['A.pdf'].update(status=previous,completed=True)
                state['downloads']['A.pdf']['content_sha256']='stale-content-hash'
                state['downloads']['A.pdf'].pop('integrity_status',None)
                self.core._save_download_state(state)
            self.assertEqual(self.core._completed_download_count('ASHRAE'),0)
            self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
            self.assertEqual(self.state()['lifecycle']['download']['status'],'INTEGRITY_PENDING')
            self.assertNotIn('content_sha256',self.state())
        orphan=path.with_name('orphan.pdf'); orphan.write_bytes(pdf_bytes(4096))
        self.assertEqual(self.core._completed_download_count(),0)
        self.assertEqual(self.core._download_state_entry('orphan.pdf')['status'],'INTEGRITY_PENDING')

    def test_late_transfer_status_cannot_overwrite_integrity_failure(self):
        self.transferred(pdf_bytes(4096)[:-100]); self.validate()
        self.core._update_download_state('A.pdf','','INTEGRITY_PENDING',completed=True)
        self.assertEqual(self.state()['status'],'PARTIAL')
        self.assertFalse(self.state()['completed'])

    def test_pending_bytes_restore_from_drive_without_redownload(self):
        from colab.pipeline_runner import ColabConfig
        from colab.storage import DriveSnapshots
        self.transferred()
        with tempfile.TemporaryDirectory() as drive, tempfile.TemporaryDirectory() as fresh:
            config=ColabConfig(local_work_dir=str(self.root),use_google_drive=True,drive_output_dir=drive)
            DriveSnapshots(config,self.core).sync(final=True)
            DriveSnapshots(replace(config,local_work_dir=fresh),self.core).restore()
            fresh=Path(fresh)
            with patch.object(self.core,'DATA_DIRECTORY',fresh), patch.object(self.core,'DOWNLOAD_DIRECTORY',fresh/'ASHRAE_Files'), patch.object(
                    self.core,'DOWNLOAD_STATE_FILE',fresh/'downloads.json'), patch.object(
                    self.core,'download_file',side_effect=AssertionError('Unexpected redownload')), patch.object(
                    self.core,'_validate_pdf_stage2',return_value={'status':'PENDING'}):
                self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
                self.assertEqual(self.core._recovery_candidates('ASHRAE'),[])
                self.assertEqual(self.core._restore_pending_validation_jobs(),1)
                self.core._stop_validation_queue()
                self.assertEqual(self.core._completed_download_count('ASHRAE'),1)

    def test_truncated_drive_link_becomes_real_local_partial(self):
        import os
        with tempfile.TemporaryDirectory() as drive:
            source=Path(drive)/'A.pdf'; payload=pdf_bytes(4096)[:-100]; source.write_bytes(payload)
            _,local,part=self.core._download_paths('A.pdf')
            try:
                local.symlink_to(source)
            except OSError:
                self.skipTest('Creating a symbolic link requires Windows privileges')
            self.core._update_download_state('A.pdf','','INTEGRITY_PENDING',candidate=self.job)
            self.validate()
            self.assertFalse(part.is_symlink()); self.assertEqual(part.read_bytes(),payload)
            with part.open('ab') as handle: handle.write(b'next bytes')
            self.assertEqual(source.read_bytes(),payload)

    def test_integrity_exception_and_changed_file_stay_pending(self):
        path=self.transferred()
        with patch.object(self.core,'check_pdf_integrity',side_effect=OSError('read failed')):
            self.validate()
        self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
        def changed(path):
            path.write_bytes(pdf_bytes(8192))
            return {'valid':True}
        with patch.object(self.core,'check_pdf_integrity',side_effect=changed):
            self.validate()
        self.assertEqual(self.state()['status'],'INTEGRITY_PENDING')
        self.assertEqual(self.core._completed_download_count('ASHRAE'),0)


if __name__=='__main__': unittest.main()
