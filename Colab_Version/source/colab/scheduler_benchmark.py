"""Loopback-only real curl_cffi/FairDownloadQueue benchmark; no source/LLM calls.

Run this SAME script with --source pointing to a saved optimized-before source
tree, then the current source tree. Only the fixture's 90+ second route cooldown
is capped to six seconds in both runs; normal recovery/pacing policy is unchanged.
"""
import argparse
from collections import Counter
from contextlib import ExitStack
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
from unittest.mock import patch, Mock

MIB=1024**2


def pdf_bytes(size):
    stream=b'%'+b'x'*max(1,size-600)+b'\n'
    objects=[b'<< /Type /Catalog /Pages 2 0 R >>',
        b'<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
        b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 72 72] /Contents 4 0 R >>',
        b'<< /Length '+str(len(stream)).encode()+b' >>\nstream\n'+stream+b'endstream']
    data=bytearray(b'%PDF-1.4\n'); offsets=[]
    for index,body in enumerate(objects,1):
        offsets.append(len(data)); data.extend(f'{index} 0 obj\n'.encode()+body+b'\nendobj\n')
    xref=len(data)
    data.extend(b'xref\n0 5\n0000000000 65535 f \n')
    for offset in offsets:
        data.extend(f'{offset:010d} 00000 n \n'.encode())
    data.extend(f'trailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n'.encode())
    return bytes(data)


class LocalFixture:
    names=('document-A','document-B','document-C','healthy-D','healthy-E','resume','range','deferred','limited','slow')
    def __init__(self):
        self.payloads={name:pdf_bytes(22*MIB if name=='resume' else MIB) for name in self.names}
        self.requests=[]; self.lock=threading.Lock(); self.calls=Counter()
        fixture=self
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*args):
                pass
            def do_GET(self):
                if self.path == '/mirror/healthy-E':
                    payload=b'<html><a href="/healthy-E.pdf">GET PDF</a></html>'
                    with fixture.lock:
                        fixture.requests.append(dict(name='mirror-E',range=None,time=time.perf_counter()))
                    self.send_response(200)
                    self.send_header('Content-Type','text/html')
                    self.send_header('Content-Length',str(len(payload))); self.end_headers()
                    self.wfile.write(payload); return
                name=self.path.split('?',1)[0].rsplit('/',1)[-1].removesuffix('.pdf')
                if name not in fixture.payloads:
                    self.send_error(404); return
                with fixture.lock:
                    fixture.calls[name]+=1; attempt=fixture.calls[name]
                    fixture.requests.append(dict(name=name,range=self.headers.get('Range'),time=time.perf_counter()))
                status=500 if name in fixture.names[:3] and attempt==1 else 429 if name=='limited' and attempt==1 else 200
                if status!=200:
                    self.send_response(status)
                    if status==429: self.send_header('Retry-After','1')
                    self.send_header('Content-Length','0'); self.end_headers(); return
                payload=fixture.payloads[name]
                offset=int(self.headers.get('Range','bytes=0-').split('=')[1].split('-')[0])
                self.send_response(206 if offset else 200)
                self.send_header('Content-Type','application/pdf')
                self.send_header('Content-Length',str(len(payload)-offset))
                if offset: self.send_header('Content-Range',f'bytes {offset}-{len(payload)-1}/{len(payload)}')
                self.end_headers()
                end=20*MIB if name=='resume' and attempt==1 else len(payload)
                try:
                    for start in range(offset,end,64*1024):
                        self.wfile.write(payload[start:min(start+64*1024,end)])
                        self.wfile.flush()
                        if name=='slow': time.sleep(.012)
                    if name=='resume' and attempt==1:
                        self.close_connection=True
                        self.connection.shutdown(socket.SHUT_RDWR)
                except (OSError,ConnectionError):
                    pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=True
        self.base=f'http://fixture.test:{self.server.server_port}'
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)

    def __enter__(self):
        self.thread.start(); return self

    def __exit__(self,*args):
        self.server.shutdown(); self.server.server_close(); self.thread.join()


def benchmark():
    from app.network_manager import NetworkManager
    from app.fair_download_queue import FairDownloadQueue
    from app.throughput import Throughput
    from curl_cffi import CurlOpt
    from curl_cffi import requests as c_requests
    from colab.pipeline_runner import shared_pipeline
    core=shared_pipeline()
    with tempfile.TemporaryDirectory() as temporary, LocalFixture() as server, ExitStack() as stack:
        root=Path(temporary)
        # Test-only DNS mapping: every request stays on our loopback server.
        # Exercise the real mirror parser without changing its access policy.
        session_type=c_requests.Session
        stack.enter_context(patch.object(c_requests,'Session',side_effect=lambda:session_type(
            curl_options={CurlOpt.RESOLVE:[f'fixture.test:{server.server.server_port}:127.0.0.1'],
                          CurlOpt.PROXY:''})))
        net=NetworkManager(Path(core.PROJECT_ROOT))
        net.settings=replace(net.settings,max_workers=5,validation_workers=2,scheduler_admission=True,
            defer_download_retries=True,new_download_min_seconds=0,new_download_max_seconds=0)
        net.telemetry=Throughput(5)
        for name,value in dict(DATA_DIRECTORY=root,DOWNLOAD_DIRECTORY=root/'ASHRAE_Files',
            DOWNLOAD_STATE_FILE=root/'downloads.json',_NETWORK_SETTINGS=net.settings).items():
            stack.enter_context(patch.object(core,name,value,create=True))
        stack.enter_context(patch.object(core,'_schedule_validation',Mock(return_value=True)))
        errors=Counter(); failed_three=threading.Event()
        original=net.gateway_result
        def gateway(url,status_code=None,**kwargs):
            original(url,status_code,**kwargs)
            if status_code and status_code>=500:
                with server.lock:
                    errors[str(status_code)]+=1
                    if errors['500']>=3: failed_three.set()
            # Compress only long route quarantine in this local fixture, equally
            # for before and after. Production route timing is not modified.
            with net._lock:
                for key,until in list(net.route_waits.items()):
                    net.route_waits[key]=min(until,time.time()+6)
            if getattr(net.context,'not_before',0)>time.time()+6:
                net.context.not_before=time.time()+6
        net.gateway_result=gateway
        active=set(); peak=0; attempts=[]; completed={}; lock=threading.Lock(); scheduling=Counter()
        started=time.perf_counter()
        def worker(job,worker_id):
            nonlocal peak
            with lock:
                assert job['document_id'] not in active, 'Duplicate writer'
                active.add(job['document_id']); peak=max(peak,len(active))
                attempts.append(dict(document=job['document_id'],attempt=job.get('_document_attempt',1),handoff=job.get('fast_handoff_count',0)))
            try:
                result=core._download_worker(job,net,worker_id)
                if result['success']: completed[job['document_id']]=time.perf_counter()-started
                return result
            finally:
                with lock: active.remove(job['document_id'])
        def next_state(job):
            scheduling[job['_queue_kind']]+=1
            if getattr(net.context,'deferred',False): scheduling['route_defer']+=1
            if getattr(net.context,'rate_limited',False): scheduling['rate_limit_defer']+=1
            core._update_download_state(job['filename'],job.get('source_url',''),
                job.get('_queue_state','RECOVERING'),size=core._download_size(job['filename']),
                candidate=job,document_attempt=job['_document_attempt'],
                retry_after_seconds=max(0,job['_not_before']-time.time()))
        queue=FairDownloadQueue(worker,network=net,max_workers=5,maxsize=50,on_defer=next_state)
        jobs={name:dict(document_id=name,filename=name+'.pdf',mirrors=[f'{server.base}/{name}.pdf'],query='ASHRAE') for name in server.names}
        jobs['healthy-E']['mirrors']=[f'{server.base}/mirror/healthy-E']
        _,_,part=core._download_paths('range.pdf')
        part.write_bytes(server.payloads['range'][:256*1024])
        core._update_download_state('range.pdf','', 'PARTIAL',size=128*1024,candidate=jobs['range'])
        jobs['deferred']['_not_before']=time.time()+.4
        max_queues=Counter(); snapshots=[]
        try:
            for name in server.names[:3]: queue.try_enqueue(jobs[name])
            assert failed_three.wait(10), 'Initial controlled failures not received'
            for name in server.names[3:]: queue.try_enqueue(jobs[name])
            queue.try_enqueue(dict(jobs['resume'],filename='alias.pdf'))
            deadline=time.monotonic()+60
            while not queue.idle and time.monotonic()<deadline:
                stats=queue.queue_stats()
                for key in ('primary','handoff','recovery'): max_queues[key]=max(max_queues[key],stats[key])
                snapshots.append(net.telemetry.snapshot(stats))
                time.sleep(.02)  # Test driver, never a network worker.
            assert queue.idle, 'Fixture queue did not drain'
            assert len(completed)==10, f'Incomplete: {set(server.names)-set(completed)}'
            for name in server.names:
                actual=core._download_paths(name+'.pdf')[1].read_bytes()
                assert hashlib.sha256(actual).digest()==hashlib.sha256(server.payloads[name]).digest()
            snapshot=net.telemetry.snapshot(queue.queue_stats())
            wall=time.perf_counter()-started
            seconds=snapshot['elapsed_seconds']*5
            new_bytes=sum(map(len,server.payloads.values()))-256*1024
            return dict(wall_seconds=wall,successful_mib=new_bytes/MIB,aggregate_mib_s=new_bytes/MIB/wall,
                useful_worker_ratio=sum(sum(row.get(s,0) for s in ('RECEIVING_BYTES','RESOLVING','CONNECTING'))
                    for row in snapshot['worker_seconds'].values())/max(.001,sum(sum(v for s,v in row.items() if s!='IDLE')
                    for row in snapshot['worker_seconds'].values())),peak_active_writers=peak,
                worker_percent={state:100*sum(row[state] for row in snapshot['worker_seconds'].values())/seconds
                                for state in snapshot['worker_counts']},queue_max=dict(max_queues),
                queue_totals=queue.queue_stats().get('totals',{}),scheduling=dict(scheduling),completed=completed,attempts=attempts,
                requests=[dict(item,time=item['time']-started) for item in server.requests],
                session_count=len(net._sessions),errors=dict(errors),goodput_bytes=snapshot['goodput_bytes'],
                scheduler_sleep_seconds=sum(row.get('SCHEDULER_SLEEP',0) for row in snapshot['worker_seconds'].values()))
        finally:
            queue.close(cancel_pending=True); net.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    sys.path.insert(0,str(args.source.resolve()))
    result=benchmark()
    text=json.dumps(result,indent=2)
    if args.output: args.output.write_text(text+'\n',encoding='utf-8')
    print(text)
