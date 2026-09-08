"""Synthetic admission/occupancy comparison; never contacts PDF sources."""
from dataclasses import replace
import json
from pathlib import Path
import time

from app.fair_download_queue import FairDownloadQueue
from app.pipeline_scheduler import GlobalDownloadQueue
from app.network_manager import NetworkManager
from app.throughput import Throughput


def benchmark():
    rows=[]
    for fair in (False,True):
        for failed_jobs in (2,10,18):
            network=NetworkManager(Path(__file__).resolve().parents[1])
            network.settings=replace(network.settings,new_download_min_seconds=.02,
                new_download_max_seconds=.02,max_document_attempts=1,scheduler_admission=fair)
            meter=Throughput(5)
            network.telemetry=meter
            healthy_completion=[]
            start=time.monotonic()
            def work(job,worker):
                if not fair:
                    with meter.timed(worker,'ADMISSION_WAIT'):
                        network.begin_download(worker,job['filename'])
                meter.state(worker,'RESOLVING')
                time.sleep(.02)  # Simulated network operation, not scheduler policy.
                if job.get('_recovery_round'):
                    meter.state(worker,'STALL')
                    time.sleep(.25)
                    meter.state(worker,'IDLE')
                    return dict(success=False,permanent=True,candidate=job,attempt=1)
                meter.received(worker,job['document_id'],1024*1024,1024*1024)
                time.sleep(.10)
                healthy_completion.append(time.monotonic()-start)
                if not fair:
                    meter.completed(time.monotonic()-start)
                    meter.state(worker,'IDLE')
                return dict(success=True,candidate=job,attempt=1)
            queue=(FairDownloadQueue(work,network=network,max_workers=5,maxsize=100) if fair
                   else GlobalDownloadQueue(work,max_workers=5,maxsize=100))
            for i in range(failed_jobs):
                queue.try_enqueue(dict(document_id=f'failed-{i}',filename=f'failed-{i}',_recovery_round=True))
            for i in range(20):
                queue.try_enqueue(dict(document_id=f'healthy-{i}',filename=f'healthy-{i}'))
            while not queue.idle:
                time.sleep(.001)
            queue.close()
            snapshot=meter.snapshot()
            seconds=snapshot['elapsed_seconds']*5
            rows.append(dict(mode='fair_admission' if fair else 'previous_worker_pacing',
                failed_jobs=failed_jobs,healthy_jobs=20,
                first_healthy_completion_seconds=min(healthy_completion),
                last_healthy_completion_seconds=max(healthy_completion),
                goodput_mib_s=snapshot['run_goodput_mib_s'],
                worker_percent={state:100*sum(worker[state] for worker in snapshot['worker_seconds'].values())/seconds
                                for state in snapshot['worker_counts']}))
    return {'kind':'synthetic; short simulated I/O, not a remote Colab speed measurement','runs':rows}


if __name__=='__main__':
    print(json.dumps(benchmark(),indent=2))
