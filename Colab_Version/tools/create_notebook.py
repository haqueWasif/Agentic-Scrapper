import json
from pathlib import Path
from textwrap import dedent

cells = []
def md(text):
    cells.append({'cell_type': 'markdown', 'metadata': {}, 'source': dedent(text).strip().splitlines(True)})
def code(text):
    cells.append({'cell_type': 'code', 'execution_count': None, 'metadata': {}, 'outputs': [], 'source': dedent(text).strip().splitlines(True)})

md('''
# ASHRAE Agentic Scraper — Google Colab

Notebook-native entry point for the repository's shared Python pipeline. No GPU/TPU, Streamlit server or tunnel is needed.

**Pipeline:** Search → Stage 1 query filter → existing-local-document check → download/recovery → full-PDF local retrieval → CrewAI Stage 2 → ASHRAE identity + query relevance → accepted library.

Run sections in order. Credentials stay in environment variables; notebook outputs never display them. Local runtime data disappears when Colab resets unless you enable Drive persistence. Keep the notebook and its source modules from the same revision.
''')
md('## 1. Environment check')
code('''
import os, platform, shutil, sys
from pathlib import Path
print('Python:', sys.version.split()[0])
print('Platform:', platform.platform())
print('CPUs:', os.cpu_count())
try:
    mem = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    print('Available RAM: %.2f GiB' % (int(mem['MemAvailable'].split()[0]) / 1024**2))
except (OSError, KeyError):
    print('Available RAM: unavailable on this platform')
disk = shutil.disk_usage('/content' if Path('/content').exists() else Path.cwd())
print('Available local disk: %.2f GiB' % (disk.free / 1024**3))
print('CPU runtime is sufficient; GPU/TPU is not required.')
''')
md('''
## 2. Clone / update / open repository

Default: clone on first run, then `git pull --ff-only` on clean later runs. Dirty worktrees are preserved. Set `REPO_REF` to the branch containing these changes once published.

**Before these changes are published:** upload `Agentic_Scraper_Colab_source.zip` using Colab's Files sidebar and set `SOURCE_ARCHIVE` to `/content/Agentic_Scraper_Colab_source.zip`. The bundle opens in a new repository directory. Existing directories are never overwritten by bundle extraction. Use another `REPO_DIR` if needed.
''')
code('''
import subprocess, zipfile
REPO_URL = 'https://github.com/haqueWasif/Agentic-Scrapper.git'
REPO_REF = 'main'
REPO_DIR = Path('/content/Agentic-Scrapper')
SOURCE_ARCHIVE = ''  # Optional: /content/Agentic_Scraper_Colab_source.zip
DISTRIBUTED_MODE = False  # Set True in ALL cooperating runtimes before installing.

if not REPO_DIR.exists():
    if SOURCE_ARCHIVE:
        REPO_DIR.mkdir(parents=True)
        with zipfile.ZipFile(SOURCE_ARCHIVE) as archive:
            for member in archive.infolist():
                target = (REPO_DIR / member.filename).resolve()
                if REPO_DIR.resolve() not in target.parents:
                    raise ValueError('Unsafe source archive path')
            archive.extractall(REPO_DIR)
        print('Opened uploaded source bundle.')
    else:
        subprocess.run(['git', 'clone', '--branch', REPO_REF, REPO_URL, str(REPO_DIR)], check=True)
elif (REPO_DIR / '.git').exists():
    dirty = subprocess.check_output(['git', '-C', str(REPO_DIR), 'status', '--porcelain'], text=True).strip()
    branch = subprocess.check_output(['git', '-C', str(REPO_DIR), 'branch', '--show-current'], text=True).strip()
    if dirty or branch != REPO_REF:
        print('Using existing checkout; local changes or a different branch prevent automatic update.')
    else:
        subprocess.run(['git', '-C', str(REPO_DIR), 'pull', '--ff-only'], check=True)
else:
    print('Using existing uploaded source directory.')
if (REPO_DIR / 'Colab_Version' / 'source' / 'colab' / 'pipeline_runner.py').is_file():
    REPO_DIR = REPO_DIR / 'Colab_Version' / 'source'
if not (REPO_DIR / 'colab' / 'pipeline_runner.py').is_file():
    raise RuntimeError('This checkout lacks the Colab adapter. Use the source bundle or a branch containing the Colab changes.')
os.chdir(REPO_DIR)
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))
print('Repository:', REPO_DIR)
''')
md('''
## 3. Install the existing requirements

This uses the existing `requirements.txt`. Distributed mode additionally installs the small PostgreSQL client from `colab/requirements-distributed.txt`. Single-runtime mode needs no database. If Colab requests a restart after installation, restart once and rerun setup.
''')
code('''
subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '-r', str(REPO_DIR / 'requirements.txt')], check=True)
if DISTRIBUTED_MODE:
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '-r', str(REPO_DIR / 'colab/requirements-distributed.txt')], check=True)
print('Requirements installed.')
''')
md('## 4. Secrets and optional LangSmith tracing')
code('''
from getpass import getpass
LANGSMITH_TRACING = False
LANGSMITH_PROJECT = 'ASHRAE-Colab'
USE_ZENROWS = False  # Optional existing provider; direct fallback is preserved.

def configure_secret(name, required=False):
    value = os.environ.get(name, '')
    if not value:
        try:
            from google.colab import userdata
            value = userdata.get(name) or ''
        except Exception:
            value = ''
    if not value and required:
        value = getpass(name + ': ')
    if value:
        os.environ[name] = value
    elif required:
        raise ValueError(name + ' is required')

configure_secret('OPENROUTER_API_KEY', required=True)
if DISTRIBUTED_MODE:
    configure_secret('DATABASE_URL', required=True)
configure_secret('ZENROWS_API_KEY', required=USE_ZENROWS)
configure_secret('LANGSMITH_API_KEY', required=LANGSMITH_TRACING)
os.environ['LANGSMITH_TRACING'] = str(LANGSMITH_TRACING).lower()
os.environ['LANGSMITH_PROJECT'] = LANGSMITH_PROJECT
print('Secrets configured. LangSmith tracing:', LANGSMITH_TRACING)
''')
md('''
## 5. Notebook configuration

The existing project settings provide queue/retry defaults. This adapter injects the settings below without changing `config/settings.yaml`. Defaults are five download workers and two validation workers. Positive-integer overrides remain supported, but keep the defaults for this scheduler benchmark. Counts never increase automatically. Start pacing remains **2–6 seconds**. Changing Colab hardware cannot fix a remote HTTP 500/503 or a source cooldown.

**Fair scheduling:** failed or interrupted transfers release their worker. Primary, partial handoff and recovery jobs share capacity with bounded priority. With five workers, at most two handoff/recovery jobs start while eligible primary work remains. Delays and Retry-After stay in the scheduler; one request/mirror attempt per document turn is preserved.

**Single-Colab scheduler benchmark:** leave `DISTRIBUTED_MODE = False`. Start with **2 pages, 20 documents, 5 download workers and 2 Stage 2 workers**. Watch **New completions this runtime**, useful-worker ratio, route deferrals, 429 waits and goodput. Existing library reuse is reported separately. Do not start a large job before measuring this small run. Existing distributed scaffolding is outside this test.
''')
code('''
QUERY = 'ASHRAE Handbook'
MAX_PAGES = 2
TARGET_DOCUMENTS = 20
RUN_ID = 'ashrae-hvac-001'
SHARD_COUNT = 3
SHARD_ID = 0  # Colab A: 0; B: 1; C: 2
SHARED_STORAGE_DIR = '/content/drive/MyDrive/ASHRAE_Shared'
SHARED_STORAGE_ID = 'ashrae-shared-pdfs-v1'
LEASE_SECONDS = 120
HEARTBEAT_SECONDS = 25
DB_PROGRESS_INTERVAL_SECONDS = 10
DATABASE_POOL_SIZE = 4
TAKEOVER_ABANDONED_PAGES = False
DOWNLOAD_WORKERS = 5
STAGE2_WORKERS = 2
USE_GOOGLE_DRIVE = True
DRIVE_OUTPUT_DIR = '/content/drive/MyDrive/ASHRAE_Scraper'
LOCAL_WORK_DIR = '/content/ashrae_work'
EXISTING_LIBRARY_DIRS = []  # Optional: ['/content/drive/MyDrive/My existing ASHRAE library']
PROGRESS_STATE_INTERVAL_SECONDS = 2.0
PROGRESS_STATE_BYTES_MB = 8
PROGRESS_EVENT_INTERVAL_SECONDS = 0.5
DOWNLOAD_CHUNK_SIZE = 256 * 1024
NEW_DOWNLOAD_MIN_SECONDS = 2.0
NEW_DOWNLOAD_MAX_SECONDS = 6.0
CHECKPOINT_PARTIALS_TO_DRIVE = False
PARTIAL_CHECKPOINT_SECONDS = 600
DEBUG_VERBOSE = False
print(f'Workers: {DOWNLOAD_WORKERS} download / {STAGE2_WORKERS} Stage 2')
print(f'Download start pacing: {NEW_DOWNLOAD_MIN_SECONDS:g}–{NEW_DOWNLOAD_MAX_SECONDS:g} seconds')
if DISTRIBUTED_MODE:
    print('Distributed run:', RUN_ID, '| shard', SHARD_ID, 'of', SHARD_COUNT)
    print('Assigned pages:', list(range(SHARD_ID + 1, MAX_PAGES + 1, SHARD_COUNT)))
''')
md('''
## 6. Fast active storage and optional Google Drive

Network → local `/content/ashrae_work/ASHRAE_Files/*.part` → completed PDF → coarse persistence step.

Drive receives completed PDFs and state snapshots every minute and at completion/interruption. Partial checkpoints are disabled by default; enabling them copies a bounded prefix at most every ten minutes, plus the final checkpoint. A large partial can be expensive to copy. No Drive operation runs in a network chunk callback.

In distributed mode, each completed PDF is saved and verified in `SHARED_STORAGE_DIR` BEFORE the database marks it completed. Runtime ledger snapshots use separate instance folders. Cross-runtime recovery cannot assume another runtime's local `.part` exists; without an explicit checkpoint it restarts from byte zero. SQLite on a Drive mount is not a reliable distributed lock; PostgreSQL provides ownership.
''')
code('''
if USE_GOOGLE_DRIVE or (DISTRIBUTED_MODE and SHARED_STORAGE_DIR.startswith('/content/drive/')):
    from google.colab import drive
    drive.mount('/content/drive')
Path(LOCAL_WORK_DIR).mkdir(parents=True, exist_ok=True)
print('Active local storage:', LOCAL_WORK_DIR)
print('Persistent storage:', DRIVE_OUTPUT_DIR if USE_GOOGLE_DRIVE else 'disabled')
''')
md('''
## 7. Initialize shared modules and persistent resume

The execution call restores missing state, the cached library index, validation reports, and completed PDF references from Drive. Existing local files win, preserving newer progress when you rerun a cell. Completed PDFs are linked for on-demand reuse rather than recopied at every startup. New or changed library files receive metadata indexing; unchanged files reuse cached metadata. Full page extraction happens only during Stage 2.

If partial checkpoints are enabled, real `.part` snapshots are copied onto local disk. **Their actual filesystem size is the HTTP Range offset**, even when `downloads.json` contains older progress. With partial checkpoints disabled, interrupted bytes survive only within the current Colab runtime; completed data and state still persist.
''')
code('''
from app.download_progress import ProgressPolicy
from colab.pipeline_runner import ColabConfig, NotebookProgress, run_colab_pipeline
CONFIG = ColabConfig(
    local_work_dir=LOCAL_WORK_DIR, use_google_drive=USE_GOOGLE_DRIVE,
    drive_output_dir=DRIVE_OUTPUT_DIR, existing_library_dirs=tuple(EXISTING_LIBRARY_DIRS),
    download_workers=DOWNLOAD_WORKERS, stage2_workers=STAGE2_WORKERS,
    new_download_min_seconds=NEW_DOWNLOAD_MIN_SECONDS, new_download_max_seconds=NEW_DOWNLOAD_MAX_SECONDS,
    progress_policy=ProgressPolicy(PROGRESS_STATE_INTERVAL_SECONDS, PROGRESS_STATE_BYTES_MB * 1024**2,
                                   PROGRESS_EVENT_INTERVAL_SECONDS, DOWNLOAD_CHUNK_SIZE),
    checkpoint_partials_to_drive=CHECKPOINT_PARTIALS_TO_DRIVE,
    partial_checkpoint_seconds=PARTIAL_CHECKPOINT_SECONDS, debug_verbose=DEBUG_VERBOSE,
    distributed_mode=DISTRIBUTED_MODE, distributed_run_id=RUN_ID,
    shard_count=SHARD_COUNT, shard_id=SHARD_ID,
    shared_storage_dir=SHARED_STORAGE_DIR, shared_storage_id=SHARED_STORAGE_ID,
    lease_seconds=LEASE_SECONDS, heartbeat_seconds=HEARTBEAT_SECONDS,
    db_progress_interval_seconds=DB_PROGRESS_INTERVAL_SECONDS, database_pool_size=DATABASE_POOL_SIZE,
    takeover_abandoned_pages=TAKEOVER_ABANDONED_PAGES,
)
print('Configuration ready; restore happens before search in the next cell.')
''')
md('''
## 8. Run the shared pipeline

Finalized byte transfers are `INTEGRITY_PENDING` until the existing validation pool checks PDF structure. Network workers release without waiting for that check. Valid PDFs become `COMPLETED`; truncated PDFs retain recovery bytes. A full validation queue preserves pending PDFs for later validation, including after restart. Semantic Stage 2 status remains separate. The results cell distinguishes network transfers from integrity-verified completed PDFs.

One updating output area, no dashboard server. Use the cell Stop button to interrupt; allow bounded in-flight requests to finish and the final checkpoint to complete before starting another run. Technical details go to `LOCAL_WORK_DIR/logs/scraper.log`. No per-page LLM calls or whole-PDF uploads: CrewAI evaluates bounded publisher/query evidence retrieved from all extractable pages.
''')
code('''
result = await run_colab_pipeline(
    query=QUERY, max_pages=MAX_PAGES, target_documents=TARGET_DOCUMENTS,
    config=CONFIG, on_snapshot=NotebookProgress(),
)
''')
md('## 9. Results and output directories')
code('''
labels = {
    'pages_processed': 'Pages processed', 'candidates_discovered': 'Candidates discovered',
    'documents_approved': 'Stage 1 approved', 'stage1_rejected': 'Stage 1 rejected',
    'local_pdfs_reused': 'Local PDFs reused', 'new_pdfs_downloaded': 'Network transfers completed this run',
    'verified_pdfs_completed': 'Verified completed PDFs (integrity passed)',
    'download_failures': 'Download failures', 'recovery_remaining': 'Recovery remaining',
    'ashrae_approved': 'ASHRAE identity approved', 'query_relevant': 'Query relevant',
    'stage2_approved': 'Accepted this query', 'pending_validation': 'Pending validation', 'rejected': 'Rejected',
}
for key, label in labels.items():
    print(f'{label}: {result.get(key, 0)}')
for folder in ('ASHRAE_Files', 'approved', 'rejected', 'validation_reports', 'logs'):
    print(folder + ':', Path(LOCAL_WORK_DIR) / folder)
print('Existing library references:', EXISTING_LIBRARY_DIRS)
print('Persistent output:', result['persistent_directory'])
if DISTRIBUTED_MODE:
    print('Global results:', result.get('global', {}))
print('Throughput history:', Path(LOCAL_WORK_DIR) / 'throughput.json')
''')
md('''
## 10. Optional export or individual PDF download

Export reports and selected metadata only. PDFs are never bulk-zipped automatically. For a very large library, use Drive instead of browser downloads. Reused legacy PDFs retain their original filenames and locations; reports record their paths.
''')
code('''
EXPORT_REPORTS = False
SELECTED_PDF = ''  # Full path of one PDF to download, or leave empty.
if EXPORT_REPORTS:
    export_path = Path(LOCAL_WORK_DIR) / 'validation_metadata.zip'
    with zipfile.ZipFile(export_path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for path in (Path(LOCAL_WORK_DIR) / 'validation_reports').glob('*.json'):
            archive.write(path, path.relative_to(LOCAL_WORK_DIR))
        for name in ('downloads.json', 'local_library_index.json', 'run_summary.json'):
            path = Path(LOCAL_WORK_DIR) / name
            if path.exists():
                archive.write(path, name)
    from google.colab import files
    files.download(str(export_path))
if SELECTED_PDF:
    selected = Path(SELECTED_PDF)
    if selected.suffix.lower() != '.pdf' or not selected.is_file():
        raise ValueError('Select an existing PDF')
    from google.colab import files
    files.download(str(selected))
''')
md('''
## 11. Optional performance benchmark

`RUN_SCHEDULER_BENCHMARK` runs ten fixture PDFs through the real FairDownloadQueue, curl_cffi sessions, HTML mirror resolution, ledger and Range resume. Its HTTP server is local: it makes no source or LLM requests. It includes 500s, an interrupted 20 MiB partial, a 429 deadline, and a slow response. For a remote check, use section 5's 5 download / 2 Stage 2 workers, 2 pages and 20-document target. Compare **New PDFs downloaded** separately from **Local PDFs reused** and inspect `throughput.json`.

Run while the pipeline is idle. The synthetic test exercises the real binary writer, ledger, and progress policy with 100 MiB at 256/512/1024 KiB chunks. It measures **local overhead only**, not remote bandwidth. Keep 256 KiB by default: synthetic disk speed alone does not justify changing network chunk size or worker counts.

The real run totals below separately report bytes received (excluding pre-existing resume bytes), time inside binary streaming including local callback overhead, durable writes/write time, progress events, retries/cooldowns, start pacing, and mirror HTTP resolution time. For five concurrent workers, transfer seconds are the sum of worker times, not wall-clock throughput. Compare the same authorized workload on Windows and Colab. High ledger time suggests persistence overhead; high retry/pacing time suggests waiting; low binary throughput can reflect remote delivery or bandwidth and cannot by itself distinguish them. No claim that Colab is faster is made.
''')
code('''
RUN_LOCAL_BENCHMARK = False
RUN_SCHEDULER_BENCHMARK = False
if RUN_SCHEDULER_BENCHMARK:
    from colab.scheduler_benchmark import benchmark
    print(benchmark())
if RUN_LOCAL_BENCHMARK:
    from colab.download_benchmark import benchmark_local_io
    for measurement in benchmark_local_io(directory=LOCAL_WORK_DIR):
        print(measurement)
if 'result' in globals():
    print('Real pipeline totals:', result['benchmark_totals'])
''')
nb = {'cells': cells, 'metadata': {'colab': {'name': 'Agentic_Scraper_Colab.ipynb'},
      'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'},
      'language_info': {'name': 'python', 'version': '3.12'}}, 'nbformat': 4, 'nbformat_minor': 5}
for i, cell in enumerate(cells):
    cell['id'] = f'ashrae-{i:02d}'
(Path(__file__).resolve().parents[1] / 'source/colab/Agentic_Scraper_Colab.ipynb').write_text(json.dumps(nb, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(len(cells), 'cells written')
