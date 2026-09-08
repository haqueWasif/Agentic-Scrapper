"""Optional shard orchestration and fenced download ownership around the shared pipeline."""
import asyncio
import hashlib
import inspect
import os
from pathlib import Path
import threading
import time
import uuid
from urllib.parse import urlparse

from app.pipeline_state import stable_document_id
from colab.db import Database, CoordinationUnavailable
from colab.shared_storage import SharedFiles, digest


def assigned_pages(max_pages, shard_count, shard_id):
    if shard_count < 1 or not 0 <= shard_id < shard_count:
        raise ValueError('SHARD_ID must be between 0 and SHARD_COUNT - 1')
    return [page for page in range(1,max_pages+1) if (page-1)%shard_count == shard_id]


class DistributedCoordinator:
    def __init__(self, config, core, query, max_pages, target, *, db=None):
        self.config, self.core = config, core
        self.query = query
        self.db = db or Database(os.environ.get('DATABASE_URL',''), pool_size=config.database_pool_size)
        self.files = SharedFiles(config.shared_storage_dir)
        self.instance = f'colab-{config.shard_id}-{uuid.uuid4().hex[:12]}'
        self.run_id, self.max_pages, self.target = config.distributed_run_id, max_pages, target
        try:
            self.db.initialize()
            self.qh = self.db.register_run(self.run_id, dict(query=query,max_pages=max_pages,
                target_documents=target,shard_count=config.shard_count,storage=config.shared_storage_id),self.instance,config.shard_id)
        except Exception:
            self.db.close()
            raise
        self.active, self.file_ids = {}, {}
        self.validations = {}
        self.lock = threading.RLock()
        self.local = threading.local()
        self.network = None
        self.stop = threading.Event()
        self.global_stats = {}
        self.coordination_ok = True
        self.current_page = None
        self.recovery_candidates = []
        self.stage1_original = core.invoke_scraper_graph
        self.stage2_original = core.invoke_pdf_validation_graph
        from app.agents import orchestrator
        self.version = hashlib.sha256(inspect.getsource(orchestrator.evaluate_documents).encode()).hexdigest()
        self.thread = threading.Thread(target=self._heartbeat, name='postgres-heartbeat', daemon=True)
        self.thread.start()

    def pages(self):
        for page in range(1,self.max_pages+1):
            assigned = (page-1)%self.config.shard_count == self.config.shard_id
            if not assigned and not self.config.takeover_abandoned_pages:
                continue
            claim = self.db.claim_page(self.qh,page,self.config.shard_id,self.instance,self.config.lease_seconds,
                                       self.config.takeover_abandoned_pages)
            if not claim:
                continue
            self.current_page = (page,claim['claim_token'],None)
            try:
                yield page
            finally:
                _,token,found = self.current_page
                self.db.page_done(self.qh,page,self.instance,token,found or 0,
                                  '' if found is not None else 'Search/evaluation did not complete')
                self.current_page = None

    def page_result(self, found):
        if self.current_page:
            self.current_page = (*self.current_page[:2],found)

    async def admit_search(self, url):
        host=urlparse(url).netloc.lower()
        while not await asyncio.to_thread(self.db.reserve_start,host,self.config.new_download_max_seconds):
            await asyncio.sleep(.5)

    def target_reached(self):
        self.global_stats = self.db.counters(self.run_id,self.qh)
        return self.global_stats['accepted'] >= self.target

    def recovery_jobs(self):
        jobs=[]
        for row in self.db.recovery(self.qh):
            job=dict(row['source_metadata'],document_id=row['document_id'],filename=row['filename'],
                     _document_attempt=row['document_attempt']+1,_recovery_round=True, query=self.query,
                     fast_handoff_count=row['fast_handoff_count'],consecutive_failures=row['consecutive_failures'])
            # Paths in /content belong only to the old runtime. Cross-runtime
            # recovery never trusts a recorded local_path or downloaded_bytes.
            job.pop('local_path',None)
            jobs.append(job)
        return jobs

    def ensure_owner(self):
        owner = getattr(self.local,'owner',None)
        if owner is not None and (owner.get('lost') or time.monotonic() >= owner['deadline']):
            raise InterruptedError('Distributed lease lost; transfer stopped to protect ownership')

    def execute(self, candidate, worker, operation):
        # Database failure must never become a local-only download fallback.
        try:
            if not self.coordination_ok:
                raise CoordinationUnavailable('Coordination is unavailable')
            document = candidate.get('document_id') or stable_document_id(candidate)
            candidate = dict(candidate, document_id=document, query=self.query)
            self.file_ids[candidate['filename']] = document
            self.db.upsert(candidate,self.qh,version=self.version)
            row = self.db.document(document)
            if row['download_status'] == 'PERMANENTLY_FAILED':
                return dict(success=False,permanent=True,candidate=candidate,attempt=5,reason='Shared attempt limit reached')
            if row['download_status'] == 'COMPLETED':
                if self.files.persistent_file_exists(row['storage_location'],size=row['final_size'],sha=row['sha256']):
                    path = self.core._download_paths(candidate['filename'])[1]
                    self.files.download_existing_pdf(row['storage_location'],path)
                    self.core._update_download_state(candidate['filename'],'','COMPLETED',size=row['final_size'],candidate=candidate,completed=True)
                    self.core._schedule_validation(candidate['filename'],stage1_score=candidate.get('stage1_score',100))
                    return dict(success=True,local_reused=True,candidate=candidate,attempt=1)
                self.db.invalidate_missing(document,row['storage_location'])
            host = urlparse((candidate.get('mirrors') or [candidate.get('source_url','')])[0]).netloc.lower()
            if not self.db.reserve_start(host,self.config.new_download_max_seconds):
                return dict(success=False,unclaimed=True,candidate=candidate,attempt=candidate.get('_document_attempt',1),retry_after_until=time.time()+1)
            owner = self.db.claim(document,self.instance,self.run_id,self.qh,self.config.lease_seconds)
            if not owner:
                if self.target_reached():
                    return dict(success=False,permanent=True,skipped=True,candidate=candidate,attempt=1,reason='Global target reached')
                return dict(success=False,unclaimed=True,candidate=candidate,attempt=candidate.get('_document_attempt',1),retry_after_until=time.time()+5)
        except Exception:
            return dict(success=False,unclaimed=True,candidate=candidate,attempt=candidate.get('_document_attempt',1),
                        retry_after_until=time.time()+20,reason='Shared coordination unavailable; no network request admitted')
        candidate['_document_attempt'] = owner['document_attempt']
        owner.update(deadline=time.monotonic()+self.config.lease_seconds-5,lost=False,filename=candidate['filename'],published=False,worker=worker)
        if self.network and self.network.telemetry:
            self.network.telemetry.baseline(candidate['filename'],owner['downloaded_bytes'])
            if row.get('claimed_by'):
                self.network.telemetry.exclude_uncertain_replay(candidate['filename'])
        with self.lock:
            self.active[document]=owner
        self.local.owner=owner
        result = {}
        try:
            result=operation(candidate)
            return result
        finally:
            try:
                if not owner['published']:
                    size=self.core._download_size(candidate['filename'])
                    refund=bool(self.network and (getattr(self.network.context,'deferred',False) or getattr(self.network.context,'rate_limited',False)))
                    handoff = size > 0 and owner['fast_handoff_count'] < 2
                    delay = 1 if handoff else min(300, 5 * 2 ** min(owner['consecutive_failures'],6))
                    until = max(time.time()+delay, getattr(self.network.context,'not_before',0) if self.network else 0)
                    self.db.finish(document,self.instance,owner['claim_token'],size=size,
                        partial=handoff,permanent=owner['document_attempt']>=5 and not refund,refund=refund,
                        error=result.get('reason','Transfer did not finish'),route=getattr(self.network.context,'last_route','') if self.network else '',
                        until=until,rate_limited=bool(self.network and getattr(self.network.context,'rate_limited',False)))
            except Exception:
                owner['lost']=True  # Lease expiry recovers; never mark success on DB failure.
            with self.lock:
                self.active.pop(document,None)
            self.local.owner=None

    def publish(self,candidate,path):
        self.ensure_owner()
        owner=self.local.owner
        sha=digest(path)
        # Generation-specific objects prevent a stale owner overwriting a new
        # owner's PDF. Only the DB's fenced completion pointer is authoritative.
        document=owner['document_id']
        folder=hashlib.sha256(document.encode()).hexdigest()
        key=f'objects/{folder}/{owner["claim_token"]}-{sha}.pdf'
        if self.network and self.network.telemetry:
            with self.network.telemetry.timed(owner['worker'],'DRIVE_SYNC'):
                self.files.upload_completed_pdf(path,key)
        else:
            self.files.upload_completed_pdf(path,key)
        if not self.files.persistent_file_exists(key,size=Path(path).stat().st_size,sha=sha):
            raise IOError('Persistent upload verification failed')
        self.ensure_owner()
        if not self.db.finish(document,self.instance,owner['claim_token'],key=key,size=Path(path).stat().st_size,sha=sha):
            raise InterruptedError('Stale completion rejected by PostgreSQL fencing')
        owner['published']=True

    def _heartbeat(self):
        last_renew = 0.0
        while not self.stop.wait(self.config.db_progress_interval_seconds):
            try:
                with self.lock:
                    active=list(self.active.items())
                for document,owner in active:
                    if owner['published']:
                        continue
                    if time.monotonic()-last_renew < self.config.heartbeat_seconds:
                        if not self.db.progress(document,self.instance,owner['claim_token'],self.core._download_size(owner['filename'])):
                            owner['lost']=True
                        continue
                    if not self.db.renew(document,self.instance,owner['claim_token'],self.config.lease_seconds,
                                          self.core._download_size(owner['filename'])):
                        owner['lost']=True
                    else:
                        owner['deadline']=time.monotonic()+self.config.lease_seconds-5
                if time.monotonic()-last_renew >= self.config.heartbeat_seconds:
                    last_renew=time.monotonic()
                stats=getattr(self.core,'_THROUGHPUT_SNAPSHOT',{})
                rate=stats.get('windows',{}).get('60',{}).get('goodput_mib_s',0)
                with self.lock:
                    validations=[(*key,token) for key,token in getattr(self,'validations',{}).items()]
                self.db.heartbeat(self.instance,{'goodput_mib_s':rate},self.qh,self.config.lease_seconds,validations)
                self.global_stats=self.db.counters(self.run_id,self.qh)
                self.recovery_candidates=self.recovery_jobs()
                self.coordination_ok=True
            except Exception:
                self.coordination_ok=False
                with self.lock:
                    for owner in self.active.values():
                        owner['lost']=True

    async def stage1(self,state,on_status):
        documents=state.get('document_batch',[])
        fresh,approved=[],[]
        for doc in documents:
            candidate=dict(doc,source_url=doc.get('link',''),filename=f"candidate-{doc['id']}.pdf",query=self.query)
            candidate['document_id']=stable_document_id(candidate)
            cached=await asyncio.to_thread(self.db.stage1_cached,self.qh,candidate['document_id'],self.version)
            if cached=='APPROVED':
                approved.append(dict(doc,approved=True))
            elif cached!='REJECTED':
                fresh.append(doc)
        if fresh:
            result=await self.stage1_original(dict(state,document_batch=fresh),on_status)
            selected={doc['id'] for doc in result.get('extracted_documents',[])}
            for doc in fresh:
                candidate=dict(doc,source_url=doc.get('link',''),filename=f"candidate-{doc['id']}.pdf",query=self.query)
                candidate['document_id']=stable_document_id(candidate)
                await asyncio.to_thread(self.db.upsert,candidate,self.qh,'APPROVED' if doc['id'] in selected else 'REJECTED',self.version)
            approved.extend(result.get('extracted_documents',[]))
        return {'extracted_documents':approved}

    async def stage2(self,state,on_status):
        filename=state.get('filename','')
        document=self.file_ids.get(filename)
        if not document:
            entry=self.core._download_state_entry(filename) or {}
            document=(entry.get('candidate') or {}).get('document_id')
        if not document:
            return {'validation':{'status':'PENDING','approved':False,'score':0,'reason':'Missing shared document identity'}}
        row=await asyncio.to_thread(self.db.document,document)
        if not row or row['download_status']!='COMPLETED':
            return {'validation':{'status':'PENDING','approved':False,'score':0,'reason':'Awaiting persistent shared download'}}
        cached=await asyncio.to_thread(self.db.validation,self.qh,document,row['sha256'])
        if cached:
            return {'validation':cached}
        claim=await asyncio.to_thread(self.db.claim_validation,self.qh,document,self.instance,self.config.lease_seconds)
        if not claim:
            return {'validation':{'status':'PENDING','approved':False,'score':0,'reason':'Another runtime owns semantic validation'}}
        with self.lock:
            self.validations[(self.qh,document)]=claim['validation_token']
        try:
            # A prior validator may have saved between the first cache read and
            # our atomic lease claim. Recheck before spending another LLM call.
            cached=await asyncio.to_thread(self.db.validation,self.qh,document,row['sha256'])
            if cached:
                return {'validation':cached}
            if row['identity_sha']==row['sha256'] and row['ashrae_identity'] is not None:
                state=dict(state,source_metadata={**(state.get('source_metadata') or {}),
                    'global_ashrae_identity':row['ashrae_identity'], 'global_identity_sha':row['sha256']})
            result=await self.stage2_original(state,on_status)
            validation=result['validation']
            if row['identity_sha']==row['sha256'] and row['ashrae_identity'] is not None:
                validation['ashrae_identity']=row['ashrae_identity']
                if row['ashrae_identity'] is not True and validation.get('status')=='APPROVED':
                    validation.update(status='PENDING',approved=False,reason='Shared ASHRAE identity is not approved')
            saved=await asyncio.to_thread(self.db.save_validation,self.qh,document,row['sha256'],self.instance,claim['validation_token'],validation)
            if not saved:
                return {'validation':{'status':'PENDING','approved':False,'score':0,'reason':'Semantic validation lease lost'}}
            return result
        except Exception:
            return {'validation':{'status':'PENDING','approved':False,'score':0,'reason':'Shared semantic validation unavailable'}}
        finally:
            with self.lock:
                self.validations.pop((self.qh,document),None)
            try:
                await asyncio.to_thread(self.db.release_validation,self.qh,document,self.instance,claim['validation_token'])
            except Exception:
                pass  # No heartbeat will renew this finished job; its lease expires.

    def close(self):
        self.stop.set()
        self.thread.join(timeout=15)
        self.db.close()
