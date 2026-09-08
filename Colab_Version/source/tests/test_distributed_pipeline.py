"""Pure scheduler checks and real PostgreSQL races (TEST_DATABASE_URL)."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, AsyncMock, patch
import uuid

from app.fair_download_queue import FairDownloadQueue
from app.network_manager import NetworkManager, NetworkSettings, RequestDeferred
from app.pipeline_state import stable_document_id
from app.throughput import Throughput
from colab.distributed_coordinator import assigned_pages, DistributedCoordinator
from colab.db import Database
from colab.pipeline_runner import ColabConfig
from colab.shared_storage import SharedFiles

ROOT=Path(__file__).resolve().parents[1]


class SchedulingTests(unittest.TestCase):
    def test_shard_partition(self):
        shards=[assigned_pages(12,3,i) for i in range(3)]
        self.assertEqual(shards,[[1,4,7,10],[2,5,8,11],[3,6,9,12]])
        self.assertEqual(sorted(sum(shards,[])),list(range(1,13)))

    def test_goodput_excludes_existing_and_retried_bytes(self):
        now=[0.0]
        meter=Throughput(1,clock=lambda:now[0])
        meter.baseline('A',100)
        now[0]=1
        meter.received(1,'A',150,50)
        now[0]=2
        meter.received(1,'A',80,80)
        meter.received(1,'A',180,100)
        now[0]=30
        snapshot=meter.snapshot()
        self.assertEqual(snapshot['goodput_bytes'],80)
        self.assertEqual(snapshot['received_bytes'],230)
        self.assertAlmostEqual(snapshot['windows']['30']['goodput_mib_s'],80/1024**2/30)
        self.assertAlmostEqual(sum(snapshot['worker_seconds'][1].values()),30)

    def test_mislabeled_html_is_not_goodput(self):
        meter=Throughput(1)
        meter.received(1,'bad',4096,4096,useful=False)
        self.assertEqual(meter.snapshot()['goodput_bytes'],0)
        self.assertEqual(meter.snapshot()['received_bytes'],4096)
        meter.exclude_uncertain_replay('abandoned')
        meter.received(1,'abandoned',8192,8192)
        self.assertEqual(meter.snapshot()['goodput_bytes'],0)

    def test_heartbeat_is_coarse_and_chunk_guard_is_memory_only(self):
        coordinator=object.__new__(DistributedCoordinator)
        coordinator.config=ColabConfig()
        coordinator.db=Mock()
        coordinator.core=SimpleNamespace(_download_size=lambda name:4096)
        owner=dict(filename='x',claim_token=1,published=False,deadline=120,lost=False)
        coordinator.active={'doc':owner}
        coordinator.local=threading.local()
        coordinator.local.owner=owner
        coordinator.lock=threading.RLock()
        coordinator.instance='test'
        coordinator.qh='qh'
        coordinator.run_id='run'
        coordinator.recovery_jobs=Mock(return_value=[])
        coordinator.stop=Mock()
        now=[0]
        def tick(interval):
            self.assertEqual(interval,10)
            now[0]+=interval
            return now[0]>60
        coordinator.stop.wait.side_effect=tick
        with patch('colab.distributed_coordinator.time.monotonic',side_effect=lambda:now[0]):
            for _ in range(1000):
                coordinator.ensure_owner()
            self.assertEqual(coordinator.db.mock_calls,[])
            coordinator._heartbeat()
        self.assertEqual(coordinator.db.renew.call_count,2)
        self.assertEqual(coordinator.db.progress.call_count,4)

    def test_recovery_fairness_and_no_duplicate_owner(self):
        net=NetworkManager(ROOT)
        net.settings=replace(net.settings,max_workers=5,new_download_min_seconds=0,new_download_max_seconds=0)
        net.telemetry=Throughput(5)
        started=[]
        active=set()
        lock=threading.Lock()
        def worker(job,worker_id):
            with lock:
                self.assertNotIn(job['document_id'],active)
                active.add(job['document_id'])
                started.append(job['_queue_kind'])
            time.sleep(.005)
            with lock:
                active.remove(job['document_id'])
            return dict(success=True,candidate=job)
        queue=FairDownloadQueue(worker,network=net,max_workers=5,maxsize=100)
        with queue.condition:
            for i in range(50):
                queue.try_enqueue(dict(document_id=f'R{i}',filename=f'R{i}',_recovery_round=True))
                queue.try_enqueue(dict(document_id=f'P{i}',filename=f'P{i}'))
            queue.try_enqueue(dict(document_id='P0',filename='duplicate-name'))
        deadline=time.monotonic()+5
        while not queue.idle and time.monotonic()<deadline:
            time.sleep(.01)
        queue.close()
        self.assertTrue(queue.idle)
        self.assertEqual(len(started),100)
        self.assertIn('primary',started[:5])
        self.assertIn('recovery',started[:10])
        self.assertLessEqual(started[:5].count('recovery'),2)

    def test_future_retry_does_not_occupy_worker(self):
        net=NetworkManager(ROOT)
        net.settings=replace(net.settings,new_download_min_seconds=0,new_download_max_seconds=0)
        seen=[]
        queue=FairDownloadQueue(lambda job,w: (seen.append(job['filename']) or dict(success=True)),network=net,max_workers=1)
        queue.try_enqueue(dict(filename='future',document_id='future',_recovery_round=True,_not_before=time.time()+60))
        queue.try_enqueue(dict(filename='ready',document_id='ready'))
        deadline=time.monotonic()+1
        while not seen and time.monotonic()<deadline:
            time.sleep(.01)
        self.assertEqual(seen,['ready'])
        self.assertEqual(queue.active,0)
        queue.close(cancel_pending=True)

    def test_bad_route_and_429_deadlines(self):
        net=NetworkManager(ROOT)
        for _ in range(3):
            net.gateway_result('https://host.test/file',503)
        with self.assertRaises(RequestDeferred):
            net.before_request('https://host.test/file')
        net.rate_limit('https://limited.test/file',SimpleNamespace(headers={'Retry-After':'120'}))
        with self.assertRaises(RequestDeferred):
            net.before_request('https://limited.test/other-file')

    def test_identity_ignores_filename_mirror_order_and_url_fragment(self):
        a=dict(source_url='https://example.test/book?b=2&a=1',title='ASHRAE  Handbook',filename='a.pdf')
        b=dict(a,source_url='https://example.test/book?a=1&b=2#download',title='ashrae handbook',filename='b.pdf',mirrors=['https://other.test'])
        self.assertEqual(stable_document_id(a),stable_document_id(b))


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'),'Set TEST_DATABASE_URL to run actual PostgreSQL concurrency tests')
class PostgreSQLTests(unittest.TestCase):
    def setUp(self):
        self.db=Database(os.environ['TEST_DATABASE_URL'])
        self.addCleanup(self.db.close)
        self.db.initialize()
        self.db.initialize()  # idempotent migration
        self.run=uuid.uuid4().hex
        self.manifest=dict(query='ASHRAE '+self.run,max_pages=12,target_documents=1,shard_count=3,storage='fixture')
        self.qh=self.db.register_run(self.run,self.manifest,'A'+self.run,0)
        self.a,self.b='A'+self.run,'B'+self.run
        self.db.register_run(self.run,self.manifest,self.b,1)
        self.candidate=dict(document_id='doc-'+self.run,filename='A.pdf',title='ASHRAE Handbook',source_url='https://example.test/book')
        self.db.upsert(self.candidate,self.qh)

    def coordinator(self, folder, shard=0):
        folder=Path(folder)
        core=SimpleNamespace(_download_paths=lambda name:(name,folder/name,folder/(name+'.part')),
            _download_size=lambda name:0,_update_download_state=Mock(),_schedule_validation=Mock(),
            invoke_scraper_graph=AsyncMock(return_value={'extracted_documents':[]}),
            invoke_pdf_validation_graph=AsyncMock(return_value={'validation':dict(status='APPROVED',approved=True,score=95,ashrae_identity=True,query_relevant=True)}))
        config=ColabConfig(distributed_mode=True,distributed_run_id=self.run,shard_id=shard,
            shared_storage_dir=str(folder/'shared'),shared_storage_id='fixture',local_work_dir=str(folder),
            new_download_min_seconds=0,new_download_max_seconds=0)
        coordinator=DistributedCoordinator(config,core,self.manifest['query'],12,1,db=Database(os.environ['TEST_DATABASE_URL']))
        coordinator.network=SimpleNamespace(context=threading.local(),telemetry=None)
        self.addCleanup(coordinator.close)
        return coordinator

    def test_coordinator_searches_only_assigned_pages(self):
        with tempfile.TemporaryDirectory() as folder:
            coordinator=self.coordinator(folder,1)
            visited=[]
            for page in coordinator.pages():
                visited.append(page)
                coordinator.page_result(2)
            self.assertEqual(visited,[2,5,8,11])
            self.assertEqual(self.db.counters(self.run,self.qh)['pages_completed'],4)

    def test_full_notebook_adapter_three_shards_reuses_shared_pdf(self):
        from contextlib import ExitStack
        from urllib.parse import parse_qs, urlparse
        from colab.pipeline_runner import shared_pipeline, run_colab_pipeline
        core=shared_pipeline()
        pages,transfers=[],[]
        source='https://example.test/book?md5='+self.run
        async def search(url,*args,**kwargs):
            pages.append(int(parse_qs(urlparse(url).query)['page'][0]))
            return '<html>fixture</html>'
        async def graph(state,on_status):
            return {'extracted_documents':[dict(state['document_batch'][0],approved=True,score=95)]}
        def worker(candidate,network,worker_id):
            transfers.append(candidate['document_id'])
            path=core._download_paths(candidate['filename'])[1]
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(b'%PDF-fixture\n'+b'x'*4096)
            network.distributed.publish(candidate,path)
            return dict(success=True,candidate=candidate,attempt=candidate['_document_attempt'])
        with self.db.pool.connection() as conn:
            conn.execute("UPDATE documents SET download_status='PERMANENTLY_FAILED' WHERE document_id=%s",(self.candidate['document_id'],))
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ,{'DATABASE_URL':os.environ['TEST_DATABASE_URL'],'ZENROWS_API_KEY':''}))
            for name,replacement in {
                '_fetch_direct_search':search,'_fetch_zenrows_search':search,
                '_extract_search_documents':lambda *args:[dict(title='ASHRAE Fixture',link=source,text='ASHRAE Fixture')],
                '_extract_search_mirror_rows':lambda *args:[],
                '_document_mirror_links':lambda *args:[source],
                'invoke_scraper_graph':graph,'extract_markdown':AsyncMock(return_value='ASHRAE Fixture'),
                '_download_worker':worker,'_schedule_validation':Mock(return_value=True),
                '_restore_pending_validation_jobs':lambda **kwargs:0,
            }.items():
                stack.enter_context(patch.object(core,name,replacement))
            stack.enter_context(patch.object(core.random,'uniform',return_value=0))
            for shard in range(3):
                config=ColabConfig(distributed_mode=True,distributed_run_id=self.run,shard_id=shard,
                    shared_storage_dir=str(Path(temporary)/'shared'),shared_storage_id='fixture',
                    local_work_dir=str(Path(temporary)/str(shard)),new_download_min_seconds=0,new_download_max_seconds=0)
                result=asyncio.run(run_colab_pipeline(self.manifest['query'],12,1,config=config,on_snapshot=lambda _:None))
                self.assertEqual(result['global']['completed'],1)
            self.assertEqual(pages,[1,4,7,10,2,5,8,11,3,6,9,12])
            self.assertEqual(len(transfers),1)
            self.assertEqual(self.db.counters(self.run,self.qh)['pages_completed'],12)

    def test_upload_failure_cannot_publish_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            coordinator=self.coordinator(folder)
            coordinator.files.upload_completed_pdf=Mock(side_effect=OSError('fixture storage offline'))
            def operation(candidate):
                path=Path(folder)/candidate['filename']
                path.write_bytes(b'%PDF-fixture')
                coordinator.publish(candidate,path)
            with self.assertRaises(OSError):
                coordinator.execute(self.candidate,1,operation)
            row=self.db.document(self.candidate['document_id'])
            self.assertNotEqual(row['download_status'],'COMPLETED')
            self.assertIsNone(row['claimed_by'])
            self.assertGreater(row['not_before'].timestamp(),time.time())

    def test_query_stage1_and_stage2_cached_without_llm(self):
        with tempfile.TemporaryDirectory() as folder:
            coordinator=self.coordinator(folder)
            doc=dict(id=1,title='ASHRAE Handbook',link='https://example.test/book?md5='+self.run,mirrors=[])
            candidate=dict(doc,document_id=stable_document_id(dict(doc,source_url=doc['link'])),filename='A.pdf')
            self.db.upsert(candidate,self.qh,version=coordinator.version)
            result=asyncio.run(coordinator.stage1({'document_batch':[doc]},Mock()))
            self.assertEqual(len(result['extracted_documents']),1)
            coordinator.stage1_original.assert_not_called()
            owner=self.db.claim(candidate['document_id'],self.a,self.run,self.qh,120)
            self.db.finish(candidate['document_id'],self.a,owner['claim_token'],key='object',size=100,sha='sha')
            coordinator.file_ids['A.pdf']=candidate['document_id']
            first=asyncio.run(coordinator.stage2({'filename':'A.pdf'},Mock()))
            second=asyncio.run(coordinator.stage2({'filename':'A.pdf'},Mock()))
            self.assertEqual(first,second)
            self.assertEqual(coordinator.stage2_original.await_count,1)
            # A provider failure must release semantic ownership immediately.
            with self.db.pool.connection() as conn:
                conn.execute("UPDATE query_document_results SET final_query_status='PENDING' WHERE query_hash=%s AND document_id=%s",(self.qh,candidate['document_id']))
            coordinator.stage2_original.side_effect=RuntimeError('fixture LLM unavailable')
            pending=asyncio.run(coordinator.stage2({'filename':'A.pdf'},Mock()))
            self.assertEqual(pending['validation']['status'],'PENDING')
            self.assertEqual(coordinator.validations,{})
            self.assertIsNotNone(self.db.claim_validation(self.qh,candidate['document_id'],self.b,120))

    def test_atomic_upsert_and_simultaneous_claim(self):
        gate=threading.Barrier(2)
        def claim(instance):
            self.db.upsert(dict(self.candidate,filename=instance+'.pdf'),self.qh)
            gate.wait()
            return self.db.claim(self.candidate['document_id'],instance,self.run,self.qh,120)
        with ThreadPoolExecutor(2) as pool:
            results=list(pool.map(claim,[self.a,self.b]))
        self.assertEqual(sum(row is not None for row in results),1)
        self.assertEqual(self.db.counters(self.run,self.qh)['discovered'],1)

    def test_lease_expiry_and_stale_owner_fencing(self):
        doc=self.candidate['document_id']
        first=self.db.claim(doc,self.a,self.run,self.qh,120)
        self.assertIsNone(self.db.claim(doc,self.b,self.run,self.qh,120))
        self.assertTrue(self.db.renew(doc,self.a,first['claim_token'],120,4096))
        with self.db.pool.connection() as conn:
            conn.execute("UPDATE documents SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE document_id=%s",(doc,))
        second=self.db.claim(doc,self.b,self.run,self.qh,120)
        self.assertGreater(second['claim_token'],first['claim_token'])
        self.assertFalse(self.db.finish(doc,self.a,first['claim_token'],key='stale.pdf',size=1,sha='x'))
        self.assertTrue(self.db.finish(doc,self.b,second['claim_token'],key='valid.pdf',size=1,sha='y'))
        self.assertIsNone(self.db.claim(doc,self.a,self.run,self.qh,120))

    def test_global_target_and_query_isolation(self):
        doc=self.candidate['document_id']
        owner=self.db.claim(doc,self.a,self.run,self.qh,120)
        self.db.finish(doc,self.a,owner['claim_token'],key='valid.pdf',size=1,sha='y')
        validation=self.db.claim_validation(self.qh,doc,self.a,120)
        self.db.save_validation(self.qh,doc,'y',self.a,validation['validation_token'],dict(status='APPROVED',ashrae_identity=True,query_relevant=True))
        other=dict(self.candidate,document_id='other-'+self.run)
        self.db.upsert(other,self.qh)
        self.assertIsNone(self.db.claim(other['document_id'],self.b,self.run,self.qh,120))
        self.assertIsNone(self.db.validation('different-query',doc,'y'))
        self.assertEqual(self.db.document(doc)['ashrae_identity'],True)

    def test_page_ownership_and_manifest_mismatch(self):
        self.assertIsNone(self.db.claim_page(self.qh,1,1,self.b,120))
        claim=self.db.claim_page(self.qh,1,0,self.a,120)
        self.assertIsNotNone(claim)
        self.assertIsNone(self.db.claim_page(self.qh,1,0,self.b,120))
        with self.assertRaises(ValueError):
            self.db.register_run(self.run,dict(self.manifest,shard_count=4),'bad'+self.run,2)

    def test_global_source_limit(self):
        host=self.run+'.test'
        self.db.defer_host(host,time.time()+120)
        self.assertGreater(self.db.host_not_before(host),time.time()+110)
        self.assertFalse(self.db.reserve_start(host,2))

    def test_database_failure_admits_no_network(self):
        coordinator=object.__new__(DistributedCoordinator)
        coordinator.coordination_ok=False
        worker=Mock()
        result=coordinator.execute(self.candidate,1,worker)
        self.assertTrue(result['unclaimed'])
        worker.assert_not_called()

    def test_three_runtimes_different_filenames_one_global_transfer(self):
        operations=[]
        lock=threading.Lock()
        async def unused(*args,**kwargs):
            return {'validation':{'status':'APPROVED','approved':True,'score':95,
                'ashrae_identity':True,'query_relevant':True}}
        with tempfile.TemporaryDirectory() as temporary:
            shared=Path(temporary)/'shared'
            coordinators=[]
            for shard in range(3):
                local=Path(temporary)/str(shard)
                local.mkdir()
                def paths(name,local=local):
                    return name,local/name,local/(name+'.part')
                core=SimpleNamespace(_download_paths=paths,_download_size=lambda name:0,
                    _update_download_state=Mock(),_schedule_validation=Mock(),
                    invoke_scraper_graph=unused,invoke_pdf_validation_graph=unused)
                config=ColabConfig(distributed_mode=True,distributed_run_id=self.run,shard_id=shard,
                    shared_storage_dir=str(shared),shared_storage_id='fixture',local_work_dir=str(local),
                    new_download_min_seconds=0,new_download_max_seconds=0)
                coordinator=DistributedCoordinator(config,core,self.manifest['query'],12,1,db=Database(os.environ['TEST_DATABASE_URL']))
                coordinator.network=SimpleNamespace(context=threading.local(),telemetry=None)
                coordinators.append(coordinator)
                self.addCleanup(coordinator.close)
            gate=threading.Barrier(3)
            def transfer(index):
                coordinator=coordinators[index]
                job=dict(self.candidate,filename=f'runtime-{index}.pdf',mirrors=['https://example.test/book'])
                def operation(candidate):
                    with lock:
                        operations.append(index)
                    path=coordinator.core._download_paths(candidate['filename'])[1]
                    path.write_bytes(b'%PDF-fixture\n'+b'x'*4096)
                    coordinator.publish(candidate,path)
                    return dict(success=True,candidate=candidate,attempt=candidate['_document_attempt'])
                gate.wait()
                return coordinator.execute(job,index+1,operation)
            with ThreadPoolExecutor(3) as pool:
                list(pool.map(transfer,range(3)))
            self.assertEqual(len(operations),1)
            row=self.db.document(self.candidate['document_id'])
            self.assertEqual(row['download_status'],'COMPLETED')
            self.assertEqual(len(list(shared.rglob('*.pdf'))),1)
            for index,coordinator in enumerate(coordinators):
                no_source=Mock(side_effect=AssertionError('Completed document must skip source download'))
                result=coordinator.execute(dict(self.candidate,filename=f'other-{index}.pdf'),index+1,no_source)
                self.assertTrue(result['success'])
                no_source.assert_not_called()

    def test_abandoned_page_takeover_only_after_expiry(self):
        claim=self.db.claim_page(self.qh,1,0,self.a,120)
        self.assertIsNone(self.db.claim_page(self.qh,1,1,self.b,120,True))
        with self.db.pool.connection() as conn:
            conn.execute("UPDATE search_pages SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE query_hash=%s AND page_number=1",(self.qh,))
        self.assertIsNone(self.db.claim_page(self.qh,1,1,self.b,120,False))
        self.assertIsNotNone(self.db.claim_page(self.qh,1,1,self.b,120,True))


if __name__=='__main__':
    unittest.main()
