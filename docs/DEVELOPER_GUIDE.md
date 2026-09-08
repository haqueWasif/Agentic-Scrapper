# Developer Guide

This guide is for engineers who need to run, debug, test, or extend Agentic Scraper without first reverse-engineering the entire repository.

## 1. Prerequisites

Recommended local environment:

- Python 3.11+ or a compatible Python version supported by the installed dependencies;
- Git;
- network access for runtime providers;
- OpenRouter API key for semantic Stage 1 and Stage 2 evaluation;
- optional ZenRows key;
- optional LangSmith key.

## 2. Clone and environment setup

```bash
git clone https://github.com/haqueWasif/Agentic-Scrapper.git
cd Agentic-Scrapper
python -m venv .venv
```

Activate the virtual environment using the normal command for your shell.

Install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

On a compatible shell, `setup.sh` performs those installation steps:

```bash
bash setup.sh
```

## 3. Environment variables

Copy:

```text
.env.example
```

to:

```text
.env
```

Typical configuration:

```env
ZENROWS_API_KEY=
OPENROUTER_API_KEY=
LANGSMITH_API_KEY=
LANGSMITH_TRACING=false
LANGSMITH_PROJECT=ashrae-intelligent-scraper
LANGSMITH_WORKSPACE_ID=
```

### `OPENROUTER_API_KEY`

Required for the CrewAI semantic evaluators.

The current agent configuration uses:

```text
openrouter/openrouter/free
```

The existing tests intentionally verify that provider failures do not silently switch to another model/provider.

### `ZENROWS_API_KEY`

Optional for the main root pipeline because direct `curl_cffi` fallback search paths exist.

The standalone `app/stealth_scraper.py` helper, however, expects ZenRows.

### LangSmith

Tracing is enabled only when:

```env
LANGSMITH_TRACING=true
LANGSMITH_API_KEY=<non-empty>
```

If tracing fails, core work should still continue.

## 4. Run the local Streamlit application

```bash
streamlit run app.py
```

Main controls:

- Search Query
- Max Pages to Scrape
- Target Documents
- Start Scraping
- Scan Existing PDFs
- Re-validate completed PDFs

## 5. Runtime directories

The application can create:

```text
data/
├── ASHRAE_Files/
├── Low_Relevance_Files/
├── approved/
├── rejected/
├── validation_reports/
├── search_cache/
├── downloads.json
├── local_library_index.json
└── throughput.json
```

The repository ignores runtime data and PDFs through `.gitignore`.

## 6. Do not casually delete `.part` files

A `.part` file is not merely temporary garbage.

It is the resumable binary state for an interrupted transfer.

The downloader calculates the next HTTP Range offset from the actual `.part` size.

If you delete it, you intentionally force the next acquisition attempt to start from byte zero.

## 7. Configuration

Primary root settings are stored in:

```text
config/settings.yaml
```

Current default structure:

```yaml
network:
  proxy_enabled: false
  max_workers: 5
  retry_limit: 5
  retry_window_seconds: 1800
  cooldown_enabled: true
  new_download_min_seconds: 2
  new_download_max_seconds: 6
  max_request_retries_per_attempt: 2
  max_stall_seconds: 60
  max_document_attempts: 5
  recovery_round_delay_seconds: 5

pipeline:
  stage1_queue_maxsize: 100
  download_queue_maxsize: 50
  stage2_queue_maxsize: 20

validation:
  max_workers: 2
```

Important: `stage1_queue_maxsize` is present in configuration, but the inspected root runtime does not currently implement a dedicated Stage 1 producer/consumer queue.

## 8. Optional proxies

`config/proxies.txt` contains one proxy URL per line.

Proxy use remains disabled unless:

```yaml
network:
  proxy_enabled: true
```

is configured and usable proxy entries exist.

## 9. Repository map

```text
app.py
```

Main root application and controller.

Contains the current primary orchestration path as well as significant helper logic for:

- search retrieval;
- document parsing;
- durable download state;
- mirror resolution;
- binary transfer;
- recovery integration;
- validation handoff;
- Streamlit presentation.

```text
app/agents/orchestrator.py
```

Semantic agent workflows:

- Stage 1 metadata evaluation;
- Stage 2 PDF-content evaluation;
- JSON normalization/repair;
- LangGraph definitions;
- off-thread graph invocation with UI-safe status bridging.

```text
app/agents/storage.py
```

CrewAI runtime storage policy and stateless task-output handler.

```text
app/network_manager.py
```

Network-wide state:

- sessions;
- proxy selection;
- pacing;
- route cooldowns;
- rate limits;
- gateway health;
- worker events;
- distributed hooks.

```text
app/pipeline_scheduler.py
```

Bounded queues and recovery:

- `GlobalDownloadQueue`;
- `BoundedWorkQueue`;
- `GlobalRecoveryBacklog`.

```text
app/fair_download_queue.py
```

Advanced dispatcher-controlled fair queue used by optimized modes such as current Colab.

```text
app/pipeline_state.py
```

Stable document identity and canonical lifecycle normalization.

```text
app/pipeline_events.py
```

Main-thread presentation state, event bus, aggregate counters, dashboard snapshots.

```text
app/download_progress.py
```

Throttled progress/event/state persistence.

```text
app/pdf_validation.py
```

Structural PDF integrity, validation reports, preview extraction and validation storage directories.

```text
app/local_library.py
```

Existing-PDF index and bounded whole-PDF evidence retrieval.

```text
app/semantic_extractor.py
```

Fetched HTML to evaluator-friendly text.

```text
app/observability.py
```

Optional LangSmith tracing wrapper.

```text
app/throughput.py
```

Goodput, worker occupancy and historical transfer metrics.

## 10. Where to modify behavior

### Change Stage 1 relevance rules

Start here:

```text
app/agents/orchestrator.py
evaluate_documents()
```

Also inspect tests:

```text
tests/test_batch_evaluation.py
tests/test_llm_fallback.py
```

Do not return arbitrary model-generated document objects if source identity can instead be mapped from trusted batch IDs.

### Change Stage 2 semantic rules

Start here:

```text
app/agents/orchestrator.py
evaluate_pdf_content()
_pdf_validation_result()
```

Then inspect:

```text
tests/test_stage2_result_parser.py
tests/test_pdf_validation.py
```

Keep the conservative `PENDING` behavior unless you are deliberately changing acceptance policy.

### Change PDF integrity rules

Start here:

```text
app/pdf_validation.py
check_pdf_integrity()
```

Do not move expensive PyPDF semantic/page extraction into network chunk callbacks.

### Change full-PDF evidence retrieval

Start here:

```text
app/local_library.py
retrieve_pdf_evidence()
```

Current behavior scans extractable pages locally and only retains bounded page-aware evidence.

### Change download worker count / queue capacity

Start here:

```text
config/settings.yaml
app/network_manager.py
app/pipeline_scheduler.py
```

Note that the root `NetworkManager` loader clamps `max_workers` to five.

Current Colab worker overrides have different behavior. See `docs/COLAB.md`.

### Change retry behavior

Understand all three layers before editing:

```text
request-level retry        -> download_file()
document attempt           -> _download_worker()
global recovery scheduling -> GlobalRecoveryBacklog / coordinator
```

Do not collapse them into one loop.

### Change mirror discovery

Start here:

```text
app.py
_extract_search_mirror_rows()
_document_mirror_links()
parse_mirror_and_download()
```

Preserve row provenance.

### Change HTTP Range/resume

Start here:

```text
app.py
download_file()
```

The physical `.part` size should remain authoritative for the Range offset.

### Change durable state

Start here:

```text
app.py
_load_download_state()
_save_download_state()
_update_download_state()
```

and:

```text
app/pipeline_state.py
```

Preserve atomic replacement and lock semantics.

### Change Stage 2 queue behavior

Start here:

```text
app/pipeline_scheduler.py
BoundedWorkQueue
```

and:

```text
app.py
_schedule_validation()
_run_validation_job()
```

### Change Streamlit telemetry

Start here:

```text
app/pipeline_events.py
app.py
render_dashboard()
```

Worker threads should continue publishing plain data only.

### Change LangSmith tracing

Start here:

```text
app/observability.py
```

Tracing should remain failure-isolated.

### Change Colab behavior

Edit:

```text
Colab_Version/source/
```

Then rebuild:

```bash
python Colab_Version/build_bundle.py
```

Do not make a Colab-only fix in the root code and assume the upload bundle automatically contains it.

## 11. Important architectural distinctions

### Source identity vs filename

Filename:

```text
page_001_004_some_title.pdf
```

is presentation/storage metadata.

Canonical document identity is derived independently through `stable_document_id()`.

Do not use filenames as the only cross-run source identifier.

### Source identity vs content identity

`document_id` identifies the source document.

`content_sha256` identifies exact bytes.

Two source records can therefore point to byte-identical content.

This distinction is intentional and powers Stage 2 deduplication.

### Transfer completion vs semantic acceptance

A PDF can finish downloading but still be:

```text
Stage 2 pending
Stage 2 rejected
technically invalid
```

Do not equate "download succeeded" with "final accepted document".

## 12. Debugging a failed search page

Check in this order:

1. Was ZenRows configured?
2. Did direct curl_cffi return usable HTML?
3. Did DoH fallbacks run?
4. Was local search cache available?
5. Did `_extract_search_documents()` produce records?
6. Did mirror-row parsing produce groups?
7. Did `extract_markdown()` return non-empty text?
8. Did Stage 1 raise a provider/configuration error?

Search provider failures should not expose API keys because error strings are sanitized before normal display.

## 13. Debugging a Stage 1 failure

Check:

```text
OPENROUTER_API_KEY
```

Then inspect whether the batch contains:

- unique integer IDs;
- non-empty metadata/text;
- valid source URLs.

The current provider is intentionally fixed to OpenRouter in the tested implementation.

Malformed model JSON should not become approval.

## 14. Debugging a stuck download

Check:

```text
data/downloads.json
<filename>.pdf.part
worker telemetry
route/gateway host
current document_attempt
retry_after_until
last_error
```

Important distinctions:

```text
DOWNLOADING       active/incomplete attempt
FAILED_FOR_ROUND  failed document attempt awaiting recovery
RECOVERING        admitted/restored recovery work
PERMANENTLY_FAILED attempt budget exhausted
```

If `.part` is growing, the system still has useful binary progress even if a UI progress sample appears stale.

## 15. Debugging Range resume

Expected behavior:

```text
actual partial size = N
request header = Range: bytes=N-
```

Do not infer the offset only from `downloads.json` because durable checkpoints are intentionally throttled and can lag behind the actual file.

## 16. Debugging Stage 2

Check:

1. file exists;
2. structural integrity result;
3. validation queue capacity;
4. `validation_status`;
5. validation report JSON;
6. extraction quality;
7. sampled pages/evidence;
8. model raw response;
9. normalized result.

A `PENDING` result may be correct behavior rather than an error.

Examples:

- insufficient text;
- ambiguous publisher identity;
- model parse repair failed;
- queue capacity was full;
- semantic call took longer than the UI timeout marker;
- duplicate is waiting on its canonical result.

## 17. Debugging duplicate PDFs

Look for:

```text
content_sha256
duplicate_of
validation_status
```

`DUPLICATE_PENDING` means the duplicate is intentionally waiting for canonical Stage 2 rather than being revalidated.

## 18. Testing

Main root test command:

```bash
python -X utf8 -m unittest discover -s tests -v
```

Useful focused commands:

```bash
python -X utf8 -m unittest tests.test_batch_evaluation -v
python -X utf8 -m unittest tests.test_global_download_queue -v
python -X utf8 -m unittest tests.test_pipeline_state -v
python -X utf8 -m unittest tests.test_pdf_validation -v
python -X utf8 -m unittest tests.test_stage2_dedup_handoff -v
python -X utf8 -m unittest tests.test_strict_gateways -v
```

The repository's Colab reports note that some historical full-suite fixtures have pre-existing errors even though focused modern suites pass. Treat "all focused tests pass" and "full suite is green" as different claims.

## 19. High-value tests to run after specific changes

### Agent prompt/parser changes

```text
test_batch_evaluation.py
test_llm_fallback.py
test_stage2_result_parser.py
```

### Scheduler changes

```text
test_global_download_queue.py
test_download_round_scheduler.py
test_pipeline_events.py
```

### Download/mirror changes

```text
test_download_routes.py
test_sync_download.py
test_strict_gateways.py
test_gateway_health.py
```

### State/persistence changes

```text
test_download_state_writer.py
test_pipeline_state.py
test_stabilization.py
```

### Stage 2 changes

```text
test_pdf_validation.py
test_stage2_result_parser.py
test_stage2_dedup_handoff.py
```

## 20. Development workflow

Recommended workflow:

```text
1. Identify the subsystem.
2. Read its current focused tests.
3. Make the smallest deterministic change possible.
4. Add/update a focused test.
5. Run that test module.
6. Run related scheduler/state tests.
7. Run broader root tests.
8. Launch Streamlit if UI/root integration changed.
9. If Colab changed, edit Colab_Version/source and rebuild the bundle.
```

## 21. Avoid these anti-patterns

### Do not turn networking into an LLM agent

CrewAI is for semantic classification, not:

- choosing byte offsets;
- writing files;
- deciding whether `Range` is valid;
- persisting JSON state;
- manipulating retry counters.

### Do not create unbounded executors

Use visible bounded queue admission.

### Do not retry forever inside workers

Document recovery belongs to the scheduler.

### Do not merge mirror rows by title

Keep provenance tied to the original search-result row or stable MD5.

### Do not write the ledger every chunk

The project already has a progress checkpoint policy for this reason.

### Do not call `st.*` from worker threads

Publish events and render on the main Streamlit thread.

### Do not assume `COMPLETED` means the same thing in root and current Colab

Current Colab has stronger integrity-before-COMPLETED semantics.

## 22. Current refactoring opportunities

High-value future cleanup, in recommended order:

1. port/normalize the stronger Colab integrity lifecycle into root;
2. extract durable state management from `app.py`;
3. extract mirror/binary download logic from `app.py`;
4. explicitly retire or mark legacy scheduler helpers;
5. implement a real Stage 1 producer/consumer queue if throughput requires it;
6. replace limited manual YAML parsing if configuration grows;
7. normalize historical tests so the full suite has one authoritative green baseline.

These should be incremental refactors, not a rewrite.

## 23. Responsible changes

Preserve existing responsible-access controls:

- bounded concurrency;
- randomized start pacing;
- limited retries;
- `Retry-After` handling;
- route cooldowns;
- no attempts to bypass explicit server rate limits.

Use the software only for documents and sources you are authorized to access.
