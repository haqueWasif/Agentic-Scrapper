# Single-Colab scheduler fix report

Historical scheduler benchmark report. The later [integrity lifecycle correction](INTEGRITY_LIFECYCLE_REPORT.md)
supersedes the completion semantics in sections 13 and 15: a finalized transfer is
now INTEGRITY_PENDING, and COMPLETED requires structural integrity success.

Current optimized architecture, measured on the local Windows environment. Both upload artifacts remain in `Colab_Version/`. No remote source job or LLM call was run for this patch.

## 1. Exact cause found

Ordinary errors from distinct document URLs accumulated under one hostname in `NetworkManager.gateway_result()`. Three errors could create a 90-second host-wide deadline (growing to 300 seconds), making unrelated healthy PDFs ineligible. The saved optimized version also treated an admission race as a generic timeout: the downloader could persist FAILED/PARTIAL, and FairDownloadQueue increased consecutive failures and applied recovery delay even though no request occurred. Its existing context flag already prevented a direct document-attempt increment in the Fair queue, but the failure state could consume an attempt on restart. Short partial handoffs separately consumed document attempts. Synchronous PDF integrity work before Stage 2 admission and ledger persistence under the dispatcher lock were additional worker/dispatch costs found in code. These are demonstrated local causes; they do not establish every cause of the user's remote slowdown.

## 2. Host-wide cooldown contribution

Yes. The same-host fixture reproduces it. Healthy document D completed at 6.623s before versus 0.691s after. Only the fixture caps the old long route quarantine to six seconds, so this is a conservative, short reproduction of the production 90-second threshold. A/B/C initially return 500; D remains healthy throughout.

## 3. Exact old route-failure key

`urlparse(gateway_url).netloc.lower()` keyed both `route_failures` and ordinary-error `route_waits`, for example `libgen.bz` (or `fixture.test:<port>` in the test). Distinct paths and document query values collapsed into this key. The success path reset the host failure count without removing its existing wait.

## 4. Exact new key

`route_key(url)` produces the normalized scheme, lowercase netloc, path and sorted query pairs; fragments are omitted. Document selectors such as `md5` and `id` remain distinct. Only explicit tracking/cache keys (`utm_*`, `_`, `cache_bust`, `cachebuster`, `timestamp`, `ts`) are omitted. For example, `https://HOST.test/get?md5=abc&ts=1#top` becomes `https://host.test/get?md5=abc`. Ordinary failures and waits use this key. Success clears only that route's failure count and wait. Diagnostics expose counts by key, active route deferrals and earliest eligibility.

## 5. 429 versus 5xx

500/502/503/504, resets and timeouts update the individual route. HTTP 429 writes a separate `host_rate_limits[netloc.lower()]` deadline. Numeric and HTTP-date Retry-After remain supported; missing/invalid deadlines retain the existing conservative 60-second fallback. Requests to other paths on that host wait too. A 429 preserves the attempt/handoff/failure budget; it does not trigger ordinary 5xx recovery penalties or switch routes to evade the deadline.

## 6. Exact timeout origin

The removed statement was in `app/network_manager.py`, `NetworkManager.before_request()`:

```python
raise TimeoutError('Route is not eligible until its scheduler deadline')
```

It represented scheduler eligibility, not a real transport timeout.

## 7. Current representation

`RequestDeferred` is typed scheduler control flow. The worker returns a structured `DEFERRED_ROUTE` or `DEFERRED_RATE_LIMIT` outcome containing reason, not_before, route, actual partial size and unchanged attempt. FairDownloadQueue keeps one waiting job. Preflight checks precede transfer-failure writes; a deadline created after admission follows the same deferral path. No failure/handoff counters advance. Persisted deadlines and handoff budgets are restored on restart.

## 8. Worker sleep audit

Optimized mode retains `scheduler_admission=True` and `defer_download_retries=True`. Start pacing, route waits, Retry-After, fast handoff and recovery delays live in the dispatcher. `begin_download()` bypasses its legacy sleep; `cooldown()` rejects accidental optimized-mode calls. `download_file()`'s existing 8–15-second retry sleep is unreachable with the optimized one-request turn. Scheduler sleep measured **0 seconds**.

Remaining synchronous sleeps are bounded filesystem contention handling: the ledger process-lock retry sleeps 0.05s within a five-second acquisition limit; atomic replacement retries sleep 0.05/0.1/0.2/0.4s. They are I/O recovery, not scheduler timing, and were preserved. Search retry/page pacing, event-loop queue polling and telemetry polling run outside network workers. Drive's periodic wait is on its own background thread. Real socket/connect/TLS/read waits remain legitimate network occupancy. The benchmark driver/fixture sleeps simulate scheduling observation and a slow server only.

## 9. FairDownloadQueue policy

The cycle stays PRIMARY, PRIMARY, HANDOFF, PRIMARY, RECOVERY. With five workers, at most two nonprimary jobs are admitted while eligible primary work remains; spare capacity serves recovery when primary work is unavailable. Only up to five jobs are submitted to the executor at a time. Pacing and selected-route eligibility are checked before admission. Persisting a next state no longer holds the dispatcher lock. Eligible work left with spare capacity for 0.25s emits `SCHEDULER_STARVATION` with queue counts. Current/rolling metrics include ready and deferred counts, 30/60/300-second bytes, goodput, completions, errors and worker occupancy, plus sampled Stage 2/Drive activity and five-minute history.

## 10. Legacy recovery interaction

`GlobalRecoveryBacklog` is not instantiated in optimized mode. Restored jobs and new failures flow through FairDownloadQueue. The old round loop is guarded off. Legacy non-Fair mode retains its backlog and round behavior. A headless pipeline regression makes the legacy constructor raise if called and passes through failure then recovery successfully.

## 11. Duplicate recovery prevention

`_next_state()` returns exactly one next scheduling state: HANDOFF_READY, RECOVERY_READY, DEFERRED_ROUTE, DEFERRED_RATE_LIMIT or PERMANENTLY_FAILED. A single queue inserts it once after persistence; no second recovery controller receives it. Deferrals retain the original queue kind and budgets. Stable-ID known/completed/terminal sets reject duplicate discovery. Persistence problems increment `persistence_errors` and emit a warning while retaining the in-memory owner.

## 12. Active ownership

Deduplication uses the candidate's stable document ID, not just filename. Known IDs cover queued, deferred, handoff, recovery and active work. Admission also excludes IDs still in `running`. Worker cleanup and the IDLE event occur before the slot becomes reusable, preventing an old release event from overwriting the next assignment. Ownership cleanup is in `finally`. The local fixture submits a duplicate with a different filename and asserts one active writer per ID, at most five active writers total.

## 13. Partial handoff

The original curl_cffi binary writer and `.pdf.part` remain. A partial transient failure schedules HANDOFF_READY, releases the worker, and later resumes the same local file. There are at most **two fast handoffs within one document attempt**. Handoffs respect route/429 deadlines and the existing one-second minimum; after exhaustion, normal recovery advances the document attempt with bounded exponential delay. The resume offset comes from `part_path.stat().st_size`, never the older ledger byte count. No segmented downloading, byte copying between workers or new download library was added.

## 14. Notebook defaults

`DOWNLOAD_WORKERS=5`, `STAGE2_WORKERS=2`, `MAX_PAGES=2`, `TARGET_DOCUMENTS=20`, `DISTRIBUTED_MODE=False`. Worker counts remain positive-integer configurable. Start pacing remains 2–6 seconds, chunk-size argument remains 256 KiB, progress persistence remains 2 seconds / 8 MiB and UI events 0.5 seconds. The installed curl_cffi version warns that its underlying transfer buffers ignore `iter_content(chunk_size=...)`; this patch does not tune or replace those buffers.

## 15. Stage 2 backpressure

In optimized mode `_schedule_validation()` performs bounded queue admission without parsing the PDF. A full Stage 2 queue leaves the download COMPLETED and validation PENDING. The existing integrity gate still runs in `_validate_pdf_stage2()` inside the separate Stage 2 pool, before semantic evaluation. Network workers do not wait for PDF parsing, semantic validation or Drive copying. Completed-file persistence remains on the Drive background/final-sync path; active chunks stay on local `/content/ashrae_work/ASHRAE_Files/`.

## 16. Worker utilization before/after

| Metric | Current before fix | After fix |
|---|---:|---:|
| Wall seconds | 17.424 | 5.679 |
| Successful new MiB | 30.748 | 30.748 |
| Successful MiB/s | 1.765 | 5.415 |
| Receiving % | 2.457 | 7.800 |
| Resolving % | 0.162 | 0.000 |
| Connecting % | 0.969 | 9.404 |
| Scheduler sleep % | 0.000 | 0.000 |
| Idle % | 96.269 | 80.928 |
| Useful / occupied worker time % | 96.154 | 90.208 |
| Peak recovery queue | 3.000 | 3.000 |
| Recovery transitions | 5.000 | 3.000 |
| Handoff transitions | 1.000 | 1.000 |
| Worker route deferrals | 1.000 | 1.000 |
| Worker 429 deferrals | 1.000 | 1.000 |

Worker state percentages divide cumulative state seconds by **five workers × elapsed wall time**. Useful-worker ratio instead divides receiving + resolving + connecting time by occupied worker time. The fixture's short workload leaves idle capacity while legitimate retry deadlines drain, even after the fix; 100% capacity use is not expected. Before had no named SCHEDULER_SLEEP field, but its exposed retry/admission occupancy and scheduler sleeps in this optimized fixture were zero. The improvement is principally admitting healthy work instead of idling behind an unrelated host quarantine. State classification is operational telemetry, not a CPU profiler.

## 17. Same-workload throughput

Both runs completed all ten PDFs, verified by SHA-256, with **30.748314 MiB** of newly downloaded useful bytes. Before: **17.424s / 1.765 MiB/s**. After: **5.679s / 5.415 MiB/s**, **3.07×** the throughput in this controlled run.

Both use the exact same `scheduler_benchmark.py`: a local HTTP server, real FairDownloadQueue, real mirror parser, persistent curl_cffi sessions, binary stream/Range handling, progress policy and ledger. The reserved `fixture.test` hostname maps explicitly to the loopback server through test-only curl options; production URL access checks are unchanged. Stage 2 admission is mocked in the throughput fixture and separately tested for backpressure/integrity. Source/API calls and Drive are excluded from the local throughput measurement. New-start pacing is zero in **both fixture runs**, route quarantine is capped to six seconds in **both**, and the existing five-second initial recovery delay is unchanged. Production settings remain 2–6-second pacing and normal route cooldowns. This is a scheduler comparison, not a promise of remote Colab bandwidth.

Raw measurements: `Colab_Version/benchmark_results/scheduler-before.json` and `scheduler-after.json`. The before source is the saved **current optimized** snapshot at `runtime/optimized_before_scheduler_fix`, not the older baseline downloader. Reproduce sequentially from the repository root with:

```shell
python Colab_Version/source/colab/scheduler_benchmark.py --source runtime/optimized_before_scheduler_fix --output runtime/scheduler-before.json
python Colab_Version/source/colab/scheduler_benchmark.py --source Colab_Version/source --output runtime/scheduler-after.json
```

Use an installed project environment. In a restricted Windows environment, point `CREWAI_STORAGE_DIR` at a writable directory and set `OPENBLAS_NUM_THREADS=1`.

## 18. Route-failure tests

Passed: A/B/C each failing on one host leave D eligible; three failures of A defer only A. Cache-buster normalization retains distinct document selectors. A route with a future 20-second deadline has zero active workers and one waiting job; the test advances eligibility without consuming an attempt. An admission-race test observes two admissions at attempt 1 with zero consecutive failures and exactly one DEFERRED state. Forced dispatcher starvation emits the diagnostic. Ordinary same-host failures no longer impose host-wide waits.

## 19. 429 tests

Passed: `Retry-After: 30` creates an exact shared host deadline under a controlled clock, blocks another path and preserves attempt 2 with no route-failure count. The real HTTP fixture uses `Retry-After: 1`; its two requests to the limited PDF are **1.178s** apart. Both binary and mirror 429 paths are covered by focused Colab tests. Explicit host throttling remains enforced.

## 20. Range-resume tests

Passed: the interrupted 22 MiB fixture retains exactly 20 MiB and resumes with `Range: bytes=20971520-`, returning 206 and matching the original SHA-256. Both worker turns stay on attempt 1. Another partial has 256 KiB on disk versus 128 KiB in its ledger and starts with `Range: bytes=262144-`. The focused Colab suite also retains its 57 MiB actual / 49 MiB ledger case. Handoff budget coverage observes `(attempt,handoffs)` values `(1,0),(1,1),(1,2),(2,0)`.

## 21. Recovery fairness tests

Passed: 200 restored recovery jobs plus 20 primary jobs with five workers. Primary begins among the first five admissions; no more than two of those admissions are recovery while primary remains eligible. Recovery also starts promptly. Duplicate stable-ID discovery cannot add another writer. All 220 unique jobs finish. Permanent failure creates one terminal record/outcome and cannot be queued again.

## 22. Full test result

New scheduler regressions: **16 passed**. Focused Colab tests: **28 passed**. Full `source/tests` discovery: **211 tests run, 23 error records, 12 skipped**; all new scheduler tests pass within that run. The full suite is **not green**. Its error names exactly match the pre-patch 195-test run: seven AST-isolated scheduler tests omit `NetworkManager`; fifteen pipeline status error records (including subtests) omit extracted helper globals such as `_completed_download_count`; one old CrewAI test assumes a `manager_llm` entry that is absent. Twelve PostgreSQL tests are skipped because this patch intentionally does not start a database or test multi-Colab. These pre-existing failures are not reported as new scheduler failures or as passes.

Logs: `runtime/scheduler-full-tests.log`, `runtime/scheduler-colab-tests.log`, `runtime/scheduler-tests.log`. The bundle build compile-checks all shipped Python and notebook cells and tests ZIP integrity. Run from `Colab_Version/source`:

```shell
python -X utf8 -m unittest discover -s tests -p test_scheduler_utilization.py -v
python -X utf8 -m unittest discover -s colab/tests -p test_colab_pipeline.py -v
python -X utf8 -m unittest discover -s tests -v
```

## 23. Exact 2-page / 20-document Colab check

Stop the old run and allow its checkpoint to finish. Upload **`Colab_Version/Agentic_Scraper_Colab.ipynb`** using Colab's **File → Upload notebook**. Upload the matching **`Agentic_Scraper_Colab_source.zip`** through the Files sidebar. Restart the Python session to clear cached modules. In section 2 set `SOURCE_ARCHIVE='/content/Agentic_Scraper_Colab_source.zip'`, use `REPO_DIR=Path('/content/Agentic-Scrapper-scheduler-fix')` (a fresh extraction directory), and keep `DISTRIBUTED_MODE=False`. Do not clone an older GitHub checkout for this test; these local changes have not been pushed. The ZIP contains all Python files, including runner, storage and benchmarks; do not flatten it.

Run dependency/secrets cells in order. Supply your OpenRouter key via Colab Secrets with notebook access enabled. For a clean NEW-download measurement use a fresh Colab runtime so `/content/ashrae_work` is empty and a fresh Drive output folder, leaving the old library intact. Configure:

```python
QUERY = 'ASHRAE Handbook'
MAX_PAGES = 2
TARGET_DOCUMENTS = 20
DOWNLOAD_WORKERS = 5
STAGE2_WORKERS = 2
DISTRIBUTED_MODE = False
LOCAL_WORK_DIR = '/content/ashrae_work'
USE_GOOGLE_DRIVE = True
DRIVE_OUTPUT_DIR = '/content/drive/MyDrive/ASHRAE_Scheduler_Check_01'
EXISTING_LIBRARY_DIRS = []
NEW_DOWNLOAD_MIN_SECONDS = 2.0
NEW_DOWNLOAD_MAX_SECONDS = 6.0
DOWNLOAD_CHUNK_SIZE = 256 * 1024
CHECKPOINT_PARTIALS_TO_DRIVE = False
```

Keep the remaining notebook defaults. Run the Drive mount, configuration and pipeline cells once, then the results cell. Record **New PDFs downloaded / New completions this runtime**, **Local PDFs reused**, elapsed time and the 30/60/300-second goodput/worker/deferred/error metrics. The 20-document value is a target, not a guarantee of 20 successful source downloads from two pages. Inspect `/content/ashrae_work/throughput.json`, `run_summary.json` and `logs/scraper.log`; compare route deferrals with host 429 waits and Stage 2/Drive activity. A short run cannot provide a full five-minute trend. Keep `RUN_SCHEDULER_BENCHMARK=False` unless deliberately running the optional local fixture while the pipeline is idle. No large job runs automatically.

## 24. Distributed mode

Disabled for this patch and every benchmark/test of the scraper pipeline described above. Existing distributed files/scaffolding remain; no PostgreSQL coordination or multi-Colab functionality was added. The root Streamlit version remains separate.

## 25. Semantic/RAG preservation

No prompts, model selection, Stage 1 decisions, full-PDF evidence retrieval, ASHRAE validation rules or local-library matching were changed. SHA-256 comparison against the optimized-before snapshot confirms unchanged `app/agents/orchestrator.py`, `app/local_library.py`, `app/download_progress.py`, `colab/storage.py`, `app/pipeline_scheduler.py` and `config/settings.yaml`. AST comparison confirms unchanged `_validate_pdf_stage2()`, `_run_validation_job()`, `_bulk_keyword_match()` and the mirror parser's local/source-address access filter. Only when the existing integrity gate runs changed in optimized mode: it is handled by the Stage 2 pool. Curl sessions, streaming, Range, partial files and throttled persistence remain in place.
