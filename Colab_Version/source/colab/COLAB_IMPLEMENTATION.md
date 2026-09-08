# Colab implementation report

1. **Notebook path:** `colab/Agentic_Scraper_Colab.ipynb` in the dedicated Colab folder.

2. **Notebook sections:** 23 cells: title/pipeline description, environment check, clone/update/uploaded source, existing requirements, secrets/tracing, configuration, local/Drive storage, module initialization/resume, execution, results, export, and optional benchmark. All code cells have empty outputs and execution counts.

3. **Reuse:** `colab/pipeline_runner.py` imports the single implementation in `app.py` by its explicit path, using Python's normal module loader to avoid the existing `app` package/name collision. It does not copy, AST-extract, or duplicate scraper functions. Existing graph, network, scheduler, validation, event and lifecycle modules remain in use.

4. **Streamlit in Colab:** not imported or required for execution. `requirements.txt` still installs Streamlit for the local frontend. No server, tunnels, or Streamlit SessionState are needed by the notebook.

5. **Core changes:** `app.py` is import-safe with a guarded local `main()` and a headless mode plus snapshot callback. The shared run owns queue cleanup and cancellation. A fresh-run primary-queue draining bug was corrected so failures reach global recovery. This checkout did **not** contain the full-PDF RAG or legacy-index features described in the request; focused support was added in `app/local_library.py` and connected to the existing pipeline and CrewAI graph. These are shared by both frontends.

6. **Previous persistence:** the download worker called the durable ledger writer on every progress callback, normally once per 256 KiB chunk (about 400 callbacks for 100 MiB), as well as lifecycle writes.

7. **New persistence:** in-memory byte progress; write after at least 2 seconds **OR** another 8 MiB. Constants live in `app/download_progress.py`, and the notebook injects a `ProgressPolicy`. QUEUED, DOWNLOADING start, PARTIAL, FAILED_FOR_ROUND, RECOVERING, COMPLETED, PERMANENTLY_FAILED and Stage 2 states still persist immediately. Atomic temporary files, fsync, replacement, and process/thread locks are retained. Failed persistence does not advance the successful-write watermark. Shutdown waits for active attempts to record their final resumable state before syncing.

8. **Resume safety:** the HTTP Range offset comes from the actual `.part` file size. The ledger can lag without losing received bytes. A test starts with a 57 MiB partial and a 49 MiB ledger entry and verifies `Range: bytes=59768832-` and immediate completion persistence.

9. **Events:** progress telemetry is limited independently to one event per 0.5 seconds per worker. Notebook rendering is capped at once per second. A half-second telemetry task keeps worker updates visible while search or graph calls are awaiting results. Lifecycle events are not throttled.

10. **Chunk size and benchmark:** default remains 256 KiB. The optional synthetic benchmark runs the real downloader/atomic ledger with 256, 512 and 1024 KiB chunks, without remote requests. On this Windows environment, a 100 MiB sample took approximately 0.237, 0.261 and 0.273 seconds respectively; each used 15 total ledger writes, including lifecycle transitions. These short synthetic measurements are not remote throughput measurements and do not establish that Colab is faster. The benchmark records actual received bytes, streaming duration, binary MiB/s, ledger writes/time, event counts, retry/cooldown time, pacing, and mirror HTTP resolution. Real run totals are also returned; summed worker streaming seconds differ from run wall time.

11. **Storage:** active transfers and the ledger stay on `/content/ashrae_work`. A small separate persistence thread checks completed outputs/state every minute; workers do not copy to Drive. Final sync occurs after workers finish. The default persistent destination is `/content/drive/MyDrive/ASHRAE_Scraper`.

12. **Across-session resume:** snapshots contain state, reports, cached library index, and completed PDFs. Restore rebases paths into the new local work directory, preserves existing local files, and links completed PDFs for on-demand access when supported instead of recopying them. Real partial snapshots are restored only when partial checkpointing is enabled. Partial checkpoints default off; enabling them permits a ten-minute interval (minimum five minutes) and a final checkpoint. With the default off, partial bytes survive only within the current Colab runtime. Drive is optional and was tested with temporary filesystem fixtures, not a live Google account.

13. **Legacy PDFs:** configurable existing-library directories support original filenames and cached metadata titles. Matching uses exact normalized titles, preserving edition/year distinctions and refusing ambiguous matches. Cached unchanged files are not reparsed. A match skips network download and still receives query-specific validation. Reused library files keep their original path. Existing unrelated library files cannot satisfy a new query's collection target.

14. **Full-PDF Stage 2:** every extractable page is parsed locally only when validating a document. Page-aware chunks feed separate bounded rankings for publisher identity and query relevance, retaining at most 12,000 evidence characters. The existing LangGraph/CrewAI/OpenRouter path evaluates this evidence. Approval requires both ASHRAE issuing/publisher identity and query relevance; missing evidence stays pending. There is no entire-PDF upload or per-page LLM call. Query and file-signature checks prevent reuse of an outdated validation, and SHA-256 duplicate validation inheritance is restricted to the same query.

15. **Download workers:** default five; explicit positive-integer overrides are now supported in Colab.

16. **Stage 2 workers:** default two; explicit positive-integer overrides are now supported in Colab.

17. **Network scope:** no new proxy, bypass, evasion, or scraping-service features were added. Existing provider routes and request/document budgets remain. Start pacing remains 2–6 seconds and is visible/configurable in the notebook. Cancellation checks preserve partial files without increasing retry frequency.

18. **Tests added:** 20 tests in `colab/tests/test_colab_pipeline.py` cover byte/time thresholds, failed-write watermarks, forced checkpoints, event throttling, actual-size resume, failure/completion/cancellation persistence, five-worker contention, Drive isolation and restore, Streamlit-free import, valid notebook/configuration/install-cell invocation, mocked headless execution, fresh-run recovery, Streamlit startup/rerun, cached legacy matching, query-specific reuse, full-page bounded evidence, independent validation gates, and query-aware collection targets. The existing Stage 2 handoff fixture received its new ledger-reader dependency.

19. **Full test result before folder separation:** `python -X utf8 -m unittest discover -s tests -q` executed 191 tests, with 23 pre-existing error records. An untouched archive of commit `18b22b2` executed 171 tests with the same 23 error records. No new failing test IDs were introduced; all 20 newly added tests pass. The existing failures are seven isolated scheduler fixtures missing `NetworkManager`, outdated pipeline-status fixtures missing core dependencies (15 records including subtests), and one Crew fixture expecting absent manager/planning fields. The full suite is therefore **not green**. This work does not rewrite those unrelated historical test expectations. Python compilation, notebook schema/cell compilation and whitespace checks also pass. Dependencies were installed from the existing requirements into an isolated Python 3.12 environment.

20. **Local Streamlit compatibility:** `streamlit run app.py` retains the local entry point. Streamlit AppTest passed initial rendering and a query-widget rerun with no app exceptions. Authenticated live scraping and a real Colab/Drive session were not executed here.

21. **Exact Colab instructions:**
    - Open https://colab.research.google.com/ and choose **File → Upload notebook**. Select `Agentic_Scraper_Colab.ipynb`.
    - Select a CPU runtime. Upload `Agentic_Scraper_Colab_source.zip` with the Files sidebar.
    - In the repository cell, set `SOURCE_ARCHIVE = '/content/Agentic_Scraper_Colab_source.zip'`. Keep the default fresh `/content/Agentic-Scrapper` directory, or choose a new directory if one already exists.
    - Run cells in order. Install the existing requirements, provide OpenRouter via Colab Secrets/getpass, and optionally configure ZenRows and LangSmith.
    - Set the query, limits and Drive option. Mount Drive if enabled; optionally provide existing library directories. Run the initialization cell, then the single `await run_colab_pipeline(...)` execution cell.
    - Inspect the results and use the optional metadata ZIP or selected-PDF download cell. Do not zip the entire PDF library automatically.
    - Once these source changes are published to GitHub, leave `SOURCE_ARCHIVE` empty and set `REPO_REF` to that branch. The notebook can then clone/pull normally. If source code changes during an existing runtime, restart the runtime before importing the updated modules.

The source bundle contains project code, settings, requirements, tests and this notebook/report. It excludes `.env`, credentials, proxy lists, downloaded documents, runtime caches and Git internals. No commits or remote publication were performed.

## Separate folder layout

All Colab-specific notebook, adapter, storage, benchmark, test, documentation, and bundle files now live under `colab/`. Shared pipeline code remains in `app.py` and `app/`, and the local Streamlit command remains `streamlit run app.py`. Run Colab tests separately with `python -X utf8 -m unittest discover -s colab/tests -q`; local-app tests remain under `tests/`.

## Manual worker overrides

At the user's request, the Colab adapter now permits explicit positive-integer worker counts, including 50 download workers and 10 Stage 2 workers. Default counts remain 5/2. Local Streamlit settings, start pacing, retry budgets, and queue capacities are unchanged. Two additional tests cover rejected invalid values and 50/10 pool initialization/cleanup with mocked search. These tests establish configurability, not remote throughput or a safe maximum on Colab.

## Follow-up: download passes

Colab now enables `defer_download_retries` in the shared network settings. This
supersedes the original request-retry behavior for Colab: each PDF receives one
transfer attempt per pass, and failed transfers release their worker immediately
after the request ends. Immediate request retries and within-pass mirror fallback
are deferred. Mirrors rotate on later document attempts, and alternative links
rotate on later visits to a mirror. Existing first-pass/recovery-pass barriers,
document attempt limits, local partial persistence, Range resume, and start pacing
remain. Streamlit does not enable this policy.

Failures drained during queue admission are retained for recovery. Retry-After
deadlines from rate-limited transfers or Colab mirror resolution are retained in
the ledger and restored into the recovery backlog after restart. Waiting occurs
between recovery passes, outside download workers. Request timeouts still apply;
this change does not interrupt an in-flight request merely because it is slow.

Five additional offline tests cover the real worker/queue/transfer sequence
(partial A, complete B, resume A), one alternative route or mirror per pass,
rate-limit deadline restoration, and failures collected under queue backpressure.
All 27 Colab checks pass, including the existing Streamlit render/rerun check.
No remote throughput improvement has been benchmarked.

## Follow-up: accurate worker sizes

Worker assignment, document changes, and attempt changes now reset presentation
sizes and stall timestamps. Download completion captures final bytes before
scheduling Stage 2, which can move the PDF; library reuse also supplies its final
size. Both byte fields are included in success events instead of retaining the
last throttled progress sample. Unknown or inconsistent totals are displayed as
`unknown` in Colab, and idle rows omit size information. Presentation changes do
not alter downloaded bytes or the resume ledger.

Validation: 28 Colab tests and 12 shared progress-presentation tests pass. The
local test run limits OpenBLAS to one thread after an unrelated subprocess
memory-allocation failure; no notebook resource setting was changed.
# Current update

The throughput/distributed update is documented in
[DISTRIBUTED_IMPLEMENTATION.md](DISTRIBUTED_IMPLEMENTATION.md) and
[README.md](README.md). Its fair queue supersedes the historical end-of-search
recovery-pass behavior described below. Use the matching current notebook and ZIP.
