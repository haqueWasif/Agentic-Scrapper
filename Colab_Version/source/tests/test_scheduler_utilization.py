"""Regression checks for the optimized single-Colab scheduler and real HTTP path."""
import asyncio
from collections import Counter
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch, AsyncMock

from app.network_manager import NetworkManager, RequestDeferred, route_key
from app.fair_download_queue import FairDownloadQueue
from app.throughput import Throughput
from colab.pipeline_runner import shared_pipeline, ColabConfig, run_colab_pipeline

ROOT=Path(__file__).resolve().parents[1]


def drained(queue, seconds=3):
    deadline=time.monotonic()+seconds
    while not queue.idle and time.monotonic()<deadline:
        time.sleep(.005)
    if not queue.idle:
        raise AssertionError('Queue did not drain')


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.net=NetworkManager(ROOT)
        self.net.settings=replace(self.net.settings,scheduler_admission=True,defer_download_retries=True,
            new_download_min_seconds=0,new_download_max_seconds=0,recovery_round_delay_seconds=.01,
            fast_handoff_delay_seconds=.01)
        self.net.telemetry=Throughput(5)
        self.addCleanup(self.net.close)

    def queue(self, worker, workers=5, **kwargs):
        queue=FairDownloadQueue(worker,network=self.net,max_workers=workers,**kwargs)
        self.addCleanup(lambda:queue.close(cancel_pending=True))
        return queue

    def test_three_500_routes_do_not_block_healthy_same_host(self):
        for name in ('A','B','C'):
            self.net.gateway_result('https://host.test/document-'+name,500)
        self.net.before_request('https://host.test/document-D',1)
        self.assertEqual(self.net.host_rate_limits,{})
        self.assertEqual(len(self.net.route_failures),3)
        self.assertEqual(self.net.diagnostics()['error_counts']['HTTP_500'],3)
        for _ in range(2): self.net.gateway_result('https://host.test/document-A',500)
        with self.assertRaises(RequestDeferred): self.net.before_request('https://host.test/document-A',1)
        self.net.context.deferred=False
        self.net.before_request('https://host.test/document-D',1)

    def test_route_key_keeps_document_selectors_ignores_cache_busters(self):
        self.assertEqual(route_key('https://HOST.test/get?md5=abc&ts=1&utm_source=x#top'),route_key('https://host.test/get?ts=2&md5=abc'))
        self.assertNotEqual(route_key('https://host.test/get?md5=abc'),route_key('https://host.test/get?md5=def'))

    def test_429_waits_30_seconds_without_failure_or_attempt(self):
        with patch('app.network_manager.time.time',return_value=1000):
            self.net.rate_limit('https://host.test/A',SimpleNamespace(headers={'Retry-After':'30'}))
            with self.assertRaises(RequestDeferred) as caught: self.net.before_request('https://host.test/D',1)
        self.assertNotIsInstance(caught.exception,TimeoutError)
        result=caught.exception.outcome({'_document_attempt':2})
        self.assertEqual(result['not_before'],1030)
        self.assertEqual(result['retry_class'],'DEFERRED_RATE_LIMIT')
        self.assertEqual(result['attempt'],2)
        self.assertEqual(self.net.route_failures,{})

    def test_future_route_never_occupies_worker(self):
        calls=[]
        route='https://host.test/A'
        self.net.route_waits[route_key(route)]=time.time()+20
        queue=self.queue(lambda job,w:(calls.append(job) or dict(success=True)))
        job=dict(document_id='A',filename='A.pdf',mirrors=[route],_document_attempt=2)
        queue.try_enqueue(job); queue.try_enqueue(dict(job,filename='alias.pdf'))
        time.sleep(.08)
        self.assertEqual(queue.active,0); self.assertEqual(queue.pending,1); self.assertEqual(calls,[])
        self.net.route_waits[route_key(route)]=0
        drained(queue)
        self.assertEqual(calls[0]['_document_attempt'],2)
        self.assertEqual(queue.counters['recoveries'],0)

    def test_admission_race_has_one_deferred_state_and_no_attempt_increment(self):
        calls=[]; saved=[]
        def worker(job,w):
            calls.append((job.get('_document_attempt',1),job.get('consecutive_failures',0)))
            if len(calls)==1: self.net.route_waits[route_key(job['mirrors'][0])]=time.time()+.05
            self.net.before_request(job['mirrors'][0],w)
            return dict(success=True)
        queue=self.queue(worker,on_defer=lambda job:saved.append(dict(job)))
        queue.try_enqueue(dict(document_id='race',filename='race.pdf',mirrors=['https://host.test/race']))
        drained(queue)
        self.assertEqual(calls,[(1,0),(1,0)])
        self.assertEqual(len(saved),1); self.assertEqual(saved[0]['_queue_state'],'DEFERRED_ROUTE')
        self.assertEqual(self.net.route_failures,{})

    def test_handoffs_are_bounded_within_attempt(self):
        calls=[]; saved=[]
        def worker(job,w):
            calls.append((job.get('_document_attempt',1),job.get('fast_handoff_count',0)))
            return dict(success=len(calls)==4,part_size=4096,attempt=job.get('_document_attempt',1))
        queue=self.queue(worker,on_defer=lambda job:saved.append(job['_queue_state']))
        queue.try_enqueue(dict(document_id='A',filename='A.pdf'))
        drained(queue)
        self.assertEqual(calls,[(1,0),(1,1),(1,2),(2,0)])
        self.assertEqual(saved,['HANDOFF_READY','HANDOFF_READY','RECOVERY_READY'])

    def test_two_hundred_recoveries_do_not_starve_twenty_primary(self):
        starts=[]; active=set(); lock=threading.Lock()
        def worker(job,w):
            with lock:
                self.assertNotIn(job['document_id'],active); active.add(job['document_id'])
                starts.append(job['_queue_kind'])
            time.sleep(.003)  # Simulated network work.
            with lock: active.remove(job['document_id'])
            return dict(success=True)
        queue=self.queue(worker,maxsize=300)
        with queue.condition:
            for i in range(200): queue.try_enqueue(dict(document_id=f'R{i}',filename=f'R{i}',_recovery_round=True))
            for i in range(20): queue.try_enqueue(dict(document_id=f'P{i}',filename=f'P{i}'))
            queue.try_enqueue(dict(document_id='P0',filename='alias'))
        drained(queue)
        self.assertEqual(len(starts),220)
        self.assertLessEqual(starts[:5].count('recovery'),2)
        self.assertIn('primary',starts[:5]); self.assertIn('recovery',starts[:10])

    def test_starvation_diagnostic_reports_eligible_work(self):
        queue=self.queue(lambda *args:dict(success=True))
        with patch.object(queue,'_select',return_value=None):
            queue.try_enqueue(dict(document_id='A',filename='A.pdf'))
            deadline=time.monotonic()+1
            while not queue.counters['scheduler_starvation'] and time.monotonic()<deadline: time.sleep(.01)
            self.assertGreater(queue.counters['scheduler_starvation'],0)

    def test_start_admission_and_cooldown_do_not_sleep_in_worker(self):
        with patch('app.network_manager.time.sleep',side_effect=AssertionError('worker slept')):
            self.net.begin_download(1,'A.pdf')
            with self.assertRaises(RuntimeError): self.net.cooldown(1)

    def test_terminal_failure_is_persisted_once_and_cannot_be_rediscovered(self):
        states=[]; calls=[]
        self.net.settings=replace(self.net.settings,max_document_attempts=1)
        queue=self.queue(lambda job,w:(calls.append(job) or dict(success=False)),
                         on_defer=lambda job:states.append(job['_queue_state']))
        job=dict(document_id='terminal',filename='A.pdf')
        queue.try_enqueue(job); drained(queue)
        queue.try_enqueue(dict(job,filename='alias.pdf'))
        self.assertEqual(len(calls),1)
        self.assertEqual(states,['PERMANENTLY_FAILED'])
        self.assertEqual(len(queue.drain_outcomes()),1)

    def test_rolling_errors_stage2_and_drive_metrics(self):
        now=[0.0]; meter=Throughput(1,clock=lambda:now[0])
        now[0]=1; meter.error('HTTP_500')
        with meter.timed(0,'DRIVE_SYNC'):
            snapshot=meter.snapshot({'primary_ready':3},pipeline={'stage2_active':2,'stage2_queued':4})
            self.assertEqual(snapshot['drive_sync_active'],1)
            now[0]=3
        snapshot=meter.snapshot({'primary_ready':1})
        self.assertEqual(snapshot['background_drive_seconds'],2)
        self.assertEqual(snapshot['windows']['30']['error_counts']['HTTP_500'],1)
        self.assertEqual(snapshot['windows']['30']['sampled_average']['stage2_active'],2)
        now[0]=40; snapshot=meter.snapshot()
        self.assertEqual(snapshot['windows']['30']['error_counts']['HTTP_500'],0)
        self.assertEqual(snapshot['windows']['60']['error_counts']['HTTP_500'],1)
        self.assertEqual(snapshot['five_minute_report'][0]['drive_sync_seconds'],2)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.core=shared_pipeline()
        temporary=tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name)
        self.net=NetworkManager(ROOT)
        self.net.settings=replace(self.net.settings,scheduler_admission=True,defer_download_retries=True,
            new_download_min_seconds=0,new_download_max_seconds=0)
        self.addCleanup(self.net.close)
        for name,value in dict(DATA_DIRECTORY=self.root,DOWNLOAD_DIRECTORY=self.root/'ASHRAE_Files',
            DOWNLOAD_STATE_FILE=self.root/'downloads.json',_NETWORK_SETTINGS=self.net.settings).items():
            context=patch.object(self.core,name,value,create=True); context.start(); self.addCleanup(context.stop)

    def test_before_request_race_never_persists_failure(self):
        route='https://host.test/A.pdf'
        self.net.route_waits[route_key(route)]=time.time()+20
        candidate=dict(document_id='A',filename='A.pdf',mirrors=[route])
        original=self.core._update_download_state; states=[]
        def record(*args,**kwargs):
            states.append(args[2].upper()); return original(*args,**kwargs)
        with patch.object(self.core,'_update_download_state',side_effect=record):
            result=self.core._download_worker(candidate,self.net,1)
        self.assertEqual(result['retry_class'],'DEFERRED_ROUTE')
        self.assertFalse(set(states)&{'PARTIAL','FAILED','FAILED_FOR_ROUND','PERMANENTLY_FAILED'})
        self.assertEqual(result['attempt'],1)

    def test_stage2_full_does_not_parse_pdf_in_network_worker(self):
        _,path,_=self.core._download_paths('A.pdf'); path.write_bytes(b'%PDF-fixture')
        self.core._update_download_state('A.pdf','','COMPLETED',completed=True)
        full=SimpleNamespace(contains=lambda key:False,try_enqueue=lambda *args,**kwargs:False)
        with patch.object(self.core,'_validation_queue',return_value=full), patch.object(self.core,'check_pdf_integrity',side_effect=AssertionError('Integrity belongs to Stage 2')):
            self.assertFalse(self.core._schedule_validation('A.pdf'))
        state=self.core._download_state_entry('A.pdf')
        self.assertEqual(state['status'],'INTEGRITY_PENDING'); self.assertEqual(state['validation_status'],'PENDING')

    def test_restored_deadline_and_handoff_budget_survive_restart(self):
        candidate=dict(document_id='A',filename='A.pdf',mirrors=['https://host.test/A.pdf'],
                       query='ASHRAE',fast_handoff_count=2,_queue_kind='handoff')
        self.core._update_download_state('A.pdf','','HANDOFF_READY',candidate=candidate,
                                        document_attempt=2,retry_after_seconds=20)
        restored=self.core._recovery_candidates('ASHRAE')[0]
        self.assertGreater(restored['_not_before'],time.time()+19)
        self.assertEqual(restored['_document_attempt'],2)
        self.assertEqual(restored['fast_handoff_count'],2)
        self.assertEqual(restored['_queue_state'],'HANDOFF_READY')
        calls=[]
        queue=FairDownloadQueue(lambda job,w:(calls.append(job) or dict(success=True)),network=self.net,max_workers=1)
        try:
            queue.try_enqueue(restored); time.sleep(.06)
            self.assertEqual(calls,[]); self.assertEqual(queue.active,0)
        finally: queue.close(cancel_pending=True)

    def test_optimized_pipeline_never_constructs_legacy_recovery_backlog(self):
        attempts=[]
        async def graph(state,on_status): return {'extracted_documents':[dict(state['document_batch'][0],approved=True)]}
        def worker(candidate,network,worker_id):
            attempts.append(candidate.get('_document_attempt',1))
            return dict(success=len(attempts)==2,candidate=candidate,attempt=candidate.get('_document_attempt',1))
        from contextlib import ExitStack
        with ExitStack() as stack:
            for name,value in {'GlobalRecoveryBacklog':Mock(side_effect=AssertionError('Legacy recovery must be inactive')),
                '_fetch_zenrows_search':AsyncMock(return_value='<html>fixture</html>'),
                '_fetch_direct_search':AsyncMock(return_value='<html>fixture</html>'),
                '_extract_search_documents':lambda *a:[dict(title='ASHRAE Test',link='https://host.test/A.pdf',text='ASHRAE')],
                '_extract_search_mirror_rows':lambda *a:[], '_document_mirror_links':lambda *a:['https://host.test/A.pdf'],
                'extract_markdown':AsyncMock(return_value='ASHRAE'), 'invoke_scraper_graph':graph,
                '_download_worker':worker,'_restore_pending_validation_jobs':lambda **kw:0}.items():
                stack.enter_context(patch.object(self.core,name,value))
            original=NetworkManager._read_settings
            stack.enter_context(patch.object(NetworkManager,'_read_settings',side_effect=lambda p:replace(original(p),recovery_round_delay_seconds=.01)))
            result=asyncio.run(run_colab_pipeline('ASHRAE',1,2,config=ColabConfig(local_work_dir=str(self.root),new_download_min_seconds=0,new_download_max_seconds=0),on_snapshot=lambda s:None))
        self.assertEqual(attempts,[1,2]); self.assertEqual(result['new_pdfs_downloaded'],1)

    def test_real_http_streams_resume_and_respect_deadlines(self):
        from colab.scheduler_benchmark import benchmark, MIB
        result=benchmark()
        self.assertEqual(len(result['completed']),10)
        self.assertTrue(any(r['name']=='mirror-E' for r in result['requests']))
        self.assertEqual(result['queue_totals']['recoveries'],3)
        self.assertLessEqual(result['peak_active_writers'],5)
        self.assertLessEqual(result['session_count'],5)
        resumes=[r for r in result['requests'] if r['name']=='resume']
        self.assertEqual(resumes[1]['range'],f'bytes={20*MIB}-')
        self.assertEqual([r['attempt'] for r in result['attempts'] if r['document']=='resume'],[1,1])
        ranges=[r for r in result['requests'] if r['name']=='range']
        self.assertEqual(ranges[0]['range'],f'bytes={256*1024}-')
        limited=[r for r in result['requests'] if r['name']=='limited']
        self.assertGreaterEqual(limited[1]['time']-limited[0]['time'],.95)
        self.assertEqual(result['scheduler_sleep_seconds'],0)
        self.assertAlmostEqual(result['goodput_bytes']/MIB,result['successful_mib'],places=5)
        output=os.environ.get('SCHEDULER_BENCHMARK_OUTPUT')
        if output: Path(output).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__': unittest.main()
