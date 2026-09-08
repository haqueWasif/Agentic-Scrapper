# Google Colab Runtime

This document describes the current Colab implementation under `Colab_Version/`.

The Colab version is related to the root Streamlit application, but it is not simply the Streamlit UI running inside a notebook.

## 1. Source-of-truth location

All current Colab work belongs under:

```text
Colab_Version/
```

The self-contained source bundle lives in:

```text
Colab_Version/source/
```

Primary files:

```text
Colab_Version/
├── Agentic_Scraper_Colab.ipynb
├── Agentic_Scraper_Colab_source.zip
├── README.md
├── build_bundle.py
└── source/
    ├── app.py
    ├── app/
    ├── config/
    ├── requirements.txt
    └── colab/
        ├── pipeline_runner.py
        ├── storage.py
        ├── distributed_coordinator.py
        ├── db.py
        ├── distributed_schema.sql
        ├── shared_storage.py
        └── tests/
```

Do not make a Colab-only fix in the repository root and assume the packaged Colab upload will automatically contain it.

## 2. Rebuild after changes

After editing Colab source, run from the repository root:

```bash
python Colab_Version/build_bundle.py
```

This refreshes the matching notebook/source upload artifacts.

## 3. Notebook execution model

The Colab adapter is implemented by:

```text
Colab_Version/source/colab/pipeline_runner.py
```

It loads the Colab copy of `app.py` by explicit file path and runs the pipeline headlessly.

The notebook does not need:

- a Streamlit server;
- tunnels;
- Streamlit SessionState;
- browser UI callbacks.

The normal execution form is conceptually:

```python
await run_colab_pipeline(...)
```

## 4. Default Colab configuration

`ColabConfig` defaults include:

```text
local_work_dir                 /content/ashrae_work
use_google_drive               false
drive_output_dir               /content/drive/MyDrive/ASHRAE_Scraper
download_workers               5
stage2_workers                  2
new_download_min_seconds       2
new_download_max_seconds       6
distributed_mode               false
```

Active network transfers are required to use local runtime storage rather than mounted Google Drive.

## 5. Why active transfers stay on `/content`

The Colab design separates hot transfer storage from persistence:

```text
network workers
    |
    v
/content/ashrae_work
    |
    +----> background snapshot/sync
             |
             v
        Google Drive
```

This avoids placing high-frequency `.part` writes and ledger activity directly on a Drive mount.

## 6. Notebook-safe progress

`NotebookProgress` maintains one updating output region rather than continuously appending output cells.

It displays high-level information such as:

- current search page;
- Stage 1 approvals/rejections;
- transfer completion count;
- recovery backlog;
- goodput windows;
- worker states;
- Stage 2 queue/worker state;
- accepted/rejected library counts.

Update frequency is intentionally bounded.

## 7. Colab scheduler settings

The Colab adapter overrides the shared network settings with:

```text
defer_download_retries = true
scheduler_admission = true
```

This is a significant behavioral difference from the normal root Streamlit path.

## 8. FairDownloadQueue

Current optimized Colab mode uses `FairDownloadQueue`.

It has separate internal queues for:

```text
primary
handoff
recovery
```

with a weighted cycle similar to:

```text
primary
primary
handoff
primary
recovery
```

The scheduler, not the network worker, owns:

- new job admission;
- pacing between job starts;
- route eligibility deadlines;
- fairness between primary and recovery work;
- partial-file handoff priority.

## 9. Fast handoff behavior

When a transfer makes partial progress but fails, optimized Colab mode can give it a short handoff path before moving it into longer recovery.

Conceptually:

```text
partial progress
   -> handoff queue
   -> short delay
   -> resume from .part
```

After limited handoff opportunities, repeated failure moves to normal recovery with longer backoff.

This prevents partially successful downloads from waiting behind a very large recovery backlog while still avoiding unlimited immediate retry loops.

## 10. Deferred retry passes

Because `defer_download_retries=True`, Colab does not spend the same amount of time on immediate within-pass retry behavior as the default root path.

The intent is:

```text
one bounded transfer attempt
   -> release worker
   -> retry on a later scheduler pass when appropriate
```

`.part` files and HTTP Range resume remain intact.

`Retry-After` deadlines are preserved and later scheduler admission respects them.

## 11. DriveSnapshots

Google Drive persistence is implemented in:

```text
Colab_Version/source/colab/storage.py
```

`DriveSnapshots` supports:

- restoring previous JSON state;
- restoring completed PDFs;
- optional partial-file checkpoints;
- rebasing saved paths into a new Colab runtime;
- background periodic sync;
- final sync at shutdown.

## 12. Drive restore behavior

During restore:

- existing local files are preserved;
- unsafe paths escaping the configured directories are rejected;
- JSON paths are rebased from old storage roots to current local roots;
- completed PDFs can be linked when supported instead of fully recopied at every startup;
- partials are restored only when partial checkpointing was enabled.

## 13. Partial Drive checkpoints

Partial-file Drive checkpoints are optional.

They are intentionally coarse and must not occur at high frequency.

The configuration enforces a minimum checkpoint interval of several minutes.

Default behavior keeps partials only in the current local Colab runtime unless checkpointing is explicitly enabled.

## 14. Colab integrity lifecycle

The most important current difference between Colab and root Streamlit is the stronger integrity lifecycle.

### Older/root-style sequence

```text
DOWNLOADING
  -> finalized .pdf
  -> COMPLETED
  -> later structural validation
```

### Current Colab sequence

```text
DOWNLOADING
  -> finalized .pdf bytes
  -> INTEGRITY_PENDING
  -> validation pool performs structural PDF check

valid
  -> COMPLETED
  -> semantic Stage 2

truncated/corrupt
  -> recovery/failure state
```

Therefore, current Colab `COMPLETED` has stronger meaning than root `COMPLETED`.

## 15. Verified-download predicate

Current Colab contains a dedicated integrity predicate.

A verified download requires, conceptually:

```text
status == COMPLETED
integrity_status == VALID
integrity path matches
integrity signature exists
current file size/mtime still match the recorded signature
```

A changed or missing file is therefore not silently treated as a still-verified download.

## 16. Integrity failure behavior

The Colab integrity lifecycle distinguishes different failure types.

Examples:

### Truncated finalized PDF

The system can preserve resumable evidence by turning finalized invalid bytes back into a `.part`-compatible recovery artifact when safe.

### Other corrupt finalized PDF

Evidence can be retained under an integrity-failure location and the document returned to recovery/failure state.

### Read/changed-file race

The record can remain `INTEGRITY_PENDING` rather than making a false terminal decision.

## 17. Semantic Stage 2 after integrity

Successful structural verification does not guarantee semantic approval.

After integrity becomes valid:

```text
COMPLETED
  -> Stage 2 semantic validation
```

Semantic status can still be:

```text
APPROVED
REJECTED
PENDING
```

A semantic timeout or report-writing issue does not undo structural validity.

## 18. Colab result counters

Current integrity-aware Colab mode separates transfer and verified-completion concepts.

Important meanings include:

```text
downloaded_this_run       network transfer completions in this runtime
new_pdfs_downloaded       compatibility name for transfer count
local_pdfs_reused         local bytes reused without implying integrity certificate
verified_pdfs_completed   structurally verified query-scoped PDFs
documents_downloaded      verified completion count in optimized Colab mode
throughput completions     transfer completions for throughput measurement
```

Be careful when comparing root and Colab run summaries because similarly named counters can have different integrity semantics.

## 19. Existing local libraries in Colab

`ColabConfig.existing_library_dirs` allows the notebook to reuse already-present PDF libraries.

Matching uses the same local-library principles:

- normalized filename/title;
- cached stat/metadata information;
- ambiguous matches refused;
- no full-page scan at startup.

A reused local PDF still requires query-specific validation.

## 20. Throughput telemetry

Colab uses `app/throughput.py` to measure:

- received bytes;
- useful goodput bytes;
- completion counts;
- connection duration;
- document duration;
- worker state occupancy;
- 30/60/300-second goodput;
- five-minute trends;
- Drive sync time;
- queue state.

The high-water logic distinguishes repeated/replayed bytes from genuinely new useful bytes where possible.

## 21. Optional distributed mode

Distributed Colab mode is disabled by default.

When enabled, PostgreSQL becomes the coordination authority across independent Colab runtimes.

Core components:

```text
colab/distributed_coordinator.py
colab/db.py
colab/distributed_schema.sql
colab/shared_storage.py
```

## 22. Why PostgreSQL is used

A SQLite database on a mounted Google Drive path is not considered a reliable coordination mechanism across independent Colab machines.

PostgreSQL provides:

- leases;
- atomic claims;
- fencing tokens;
- query/document state sharing;
- page ownership;
- source rate-limit state;
- shared validation results.

## 23. Distributed schema

The current schema includes tables such as:

```text
pipeline_runs
pipeline_instances
documents
query_document_results
search_pages
source_limits
```

## 24. Search-page sharding

Default deterministic page ownership uses:

```text
(page - 1) % shard_count == shard_id
```

This lets multiple Colab instances process different pages of the same run.

Optional takeover logic can claim abandoned pages when enabled.

## 25. Download ownership leases

Before a distributed runtime performs network acquisition, it must successfully claim the document.

The coordinator tracks:

```text
claimed_by
claim_token
lease_expires_at
document_attempt
```

A worker that loses its lease is interrupted to protect shared ownership.

## 26. Fenced publication

Completed shared PDFs are published using generation-specific persistent object names.

The database claim token acts as a fence.

A stale runtime cannot simply overwrite the canonical completion pointer after a newer owner has taken over.

## 27. Shared recovery

Distributed recovery reads incomplete document state from PostgreSQL and reconstructs candidates.

Cross-runtime recovery does not trust stale `/content/...` local paths from the previous machine.

Source metadata and durable shared state are used to rebuild work safely.

## 28. Shared Stage 1 caching

Distributed mode can cache Stage 1 results by:

```text
query hash
document identity
prompt/evaluator version
```

Documents with compatible cached results avoid duplicate LLM work across runtimes.

## 29. Shared Stage 2 caching

Stage 2 validation can also be shared using:

```text
query context
document identity
PDF SHA-256
validation lease
```

Only one runtime should own semantic validation at a time.

## 30. Shared ASHRAE identity

When a PDF SHA already has a trusted publisher-identity decision, later query-specific validation can reuse that global identity and evaluate only query relevance.

This reduces duplicate semantic work while keeping publisher identity and query relevance as separate decisions.

## 31. Distributed failure policy

If shared coordination becomes unavailable, the system should not silently fall back to uncontrolled local-only network acquisition.

The coordinator marks work unclaimed/deferred so another lease-safe attempt can occur later.

This prevents multiple independent runtimes from unknowingly downloading the same document concurrently during a database outage.

## 32. Distributed target tracking

Each runtime can query global counters and stop producing new work when the shared accepted-document target has been reached.

This is different from a purely local target counter.

## 33. Colab secrets

Typical secrets may include:

```text
OPENROUTER_API_KEY
ZENROWS_API_KEY
LANGSMITH_API_KEY
DATABASE_URL       # only for distributed mode
```

Do not write secrets into the repository or bundle.

The Colab runner installs a logging filter that redacts configured secret values from logs.

## 34. Run instructions

Typical current workflow:

1. Open Google Colab.
2. Upload `Colab_Version/Agentic_Scraper_Colab.ipynb`.
3. Upload the matching `Agentic_Scraper_Colab_source.zip`, if using the upload-bundle path.
4. Set `SOURCE_ARCHIVE` to that uploaded ZIP.
5. Choose a fresh extraction directory.
6. Install dependencies.
7. Configure OpenRouter and optional provider/tracing secrets.
8. Configure query/page/target/Drive options.
9. Mount Drive only if persistence is enabled.
10. Initialize the runtime.
11. Run the single `await run_colab_pipeline(...)` cell.
12. Inspect run summary and validation output.

## 35. Focused Colab tests

From:

```text
Colab_Version/source/
```

use the focused commands documented by the project, for example:

```bash
python -X utf8 -m unittest discover -s colab/tests -p test_colab_pipeline.py -q
python -X utf8 -m unittest discover -s tests -p test_scheduler_utilization.py -q
python -X utf8 -m unittest discover -s tests -p test_distributed_pipeline.py -q
```

Database-backed tests require an appropriate disposable test database configuration.

## 36. Integrity-specific regression tests

The integrity lifecycle report documents focused validation commands such as:

```bash
python -X utf8 -m unittest discover -s tests -p test_integrity_lifecycle.py -v
python -X utf8 -m unittest discover -s tests -p test_scheduler_utilization.py -v
python -X utf8 -m unittest discover -s colab/tests -p test_colab_pipeline.py -v
```

Treat the repository's historical full-suite status separately from focused modern regressions; existing reports explicitly note pre-existing old-fixture errors in the full discovery run.

## 37. When changing scheduler behavior

Preserve:

- bounded worker count;
- one dispatcher owning admission;
- route cooldown deadlines;
- Retry-After handling;
- `.part` resume;
- fast handoff limits;
- document attempt limits;
- no retry loops that monopolize workers.

## 38. When changing Drive persistence

Preserve:

- active transfer storage outside Drive;
- no per-chunk Drive copies;
- atomic publication of snapshots;
- path rebasing safety;
- final sync after worker pools shut down.

## 39. When changing distributed mode

Preserve:

- PostgreSQL as coordination authority;
- lease checks before/while work executes;
- fencing-token completion;
- no silent local-only fallback on coordination failure;
- shared rate-limit deadlines;
- generation-specific persistent objects;
- query-specific semantic state separate from global document state.

## 40. Root-vs-Colab warning

Do not copy state-transition assumptions directly between the two runtimes without checking current implementation.

Most importantly:

```text
root COMPLETED
```

and

```text
current Colab COMPLETED
```

are not guaranteed to mean exactly the same structural-integrity state.

Read `docs/ARCHITECTURE.md` before porting lifecycle changes between versions.
