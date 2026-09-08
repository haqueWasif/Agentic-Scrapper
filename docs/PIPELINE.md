# Pipeline Lifecycle

This document follows one document through the current Agentic Scraper pipeline from search discovery to final storage.

## 1. Pipeline at a glance

```text
Search page
  -> retrieve HTML
  -> parse document rows + mirrors
  -> Stage 1 semantic filter
  -> local-library reuse check
  -> bounded download queue
  -> mirror resolution
  -> resumable .part transfer
  -> structural PDF integrity
  -> bounded Stage 2 queue
  -> whole-PDF bounded evidence
  -> Stage 2 semantic validation
  -> approved / rejected / pending
```

Supporting systems operate throughout:

```text
stable document identity
downloads.json lifecycle state
recovery backlog
event telemetry
optional LangSmith
```

## 2. Run initialization

`run_scraping_pipeline()` starts by validating:

- query is non-empty;
- `max_pages >= 1`;
- `target_documents >= 1`.

It then creates a unique run identifier and initializes:

- `NetworkManager`;
- run metrics;
- Streamlit presentation state when not headless;
- local library index;
- Stage 2 recovery;
- interrupted download recovery;
- the global download coordinator.

The system is designed so a process restart does not necessarily repeat discovery, Stage 1, or already-received bytes.

## 3. Restoring pending Stage 2 work

Before new network work begins, `_restore_pending_validation_jobs()` scans durable records for PDFs that:

- have completed transfer state;
- do not already have terminal Stage 2 state;
- still exist on disk;
- are not already in the validation queue.

These jobs are re-admitted to Stage 2 without redownloading the PDF.

If queue capacity is full, the records remain `PENDING` and can be restored again later.

## 4. Restoring interrupted downloads

`_recovery_candidates()` reconstructs recoverable acquisitions from `downloads.json`.

Eligible statuses include states such as:

```text
QUEUED
DOWNLOADING
FAILED_FOR_ROUND
RECOVERING
PARTIAL
FAILED
```

Important rules:

- completed PDFs are not downloaded again;
- real `.part` files are prioritized;
- larger partials are preferred when only a limited number of recovery jobs are needed;
- stale `DOWNLOADING` means the previous process died before finishing the attempt, so the same attempt number is resumed;
- a completed failed attempt consumes one document attempt and schedules the next one;
- attempt counts above the configured maximum become terminal failures.

## 5. Search-page production

The producer iterates over search pages until one of the stopping conditions is reached:

- page limit reached;
- requested target is already satisfied by completed + queued primary work;
- no useful document records remain;
- distributed mode reports the global target has been reached.

For pages after page 1, the root pipeline introduces a delay before another search-page request.

## 6. Search retrieval fallback chain

For every page, the application constructs a Libgen-style search URL from the encoded query.

Retrieval order:

```text
1. ZenRows, when configured
2. direct curl_cffi browser-style request
3. curl_cffi with Cloudflare DNS-over-HTTPS
4. curl_cffi with Google DNS-over-HTTPS
5. local search cache
```

If all providers fail, the page is added to the failed URL list and the pipeline moves on.

Successful HTML is cached locally for fallback use.

## 7. Search-row parsing

The fetched HTML is parsed twice for two different purposes.

### 7.1 Evaluator document records

`_extract_search_documents()` creates records containing:

```json
{
  "title": "...",
  "link": "...",
  "text": "search-result row text"
}
```

Navigation links and irrelevant page controls are excluded.

### 7.2 Row-level mirror groups

`_extract_search_mirror_rows()` groups download/mirror candidates by their original table row.

This preserves document provenance and prevents one row from borrowing mirrors from another row.

Preferred grouping recognizes routes such as:

- `library.lol`;
- Libgen routes;
- other row-scoped mirror links.

## 8. Stable identity before semantic work

`stable_document_id()` derives a durable identity.

Priority:

```text
Libgen MD5
  -> libgen-md5:<md5>

otherwise
  -> source-sha256:<digest>
```

Before sending a document to Stage 1, the root runtime can skip identities whose Stage 1 lifecycle is already terminal for the current query.

This makes Streamlit reruns more idempotent.

## 9. Preparing evaluator input

The application converts the fetched page into link-preserving text using `extract_markdown()`.

It also builds a bounded document-context representation that preserves:

- title;
- source URL;
- search-result text.

The page batch receives stable integer IDs local to that evaluation call.

Example:

```json
[
  {"id": 1, "title": "...", "link": "..."},
  {"id": 2, "title": "...", "link": "..."}
]
```

## 10. Stage 1 semantic evaluation

The root runtime emits `STAGE1_STARTED` and invokes the Stage 1 LangGraph.

CrewAI/OpenRouter is instructed to return only approved integer IDs.

Example valid result:

```json
[1, 4, 7]
```

Python then:

1. validates that every returned value is an integer;
2. rejects unknown IDs;
3. maps approved IDs back to original document metadata;
4. persists non-selected records as `STAGE1_REJECTED`.

Malformed model output is not interpreted as approval.

## 11. Stage 1 rejection lifecycle

For every rejected document, the pipeline builds a candidate with:

```text
document_id
source_url
mirrors
title
query
run_id
source_page
source_item
stage1_status = REJECTED
```

The ledger is updated with a terminal Stage 1 lifecycle.

The binary download stage is never entered.

## 12. Stage 1 approved-candidate preparation

For approved records, the pipeline determines whether acquisition is necessary.

Checks include:

- target remaining capacity;
- whether a completed local destination already exists;
- whether the source/document is already scheduled for recovery;
- whether a local-library match exists;
- whether usable source links/mirrors exist.

A new candidate contains information similar to:

```json
{
  "filename": "page_001_004_....pdf",
  "mirrors": ["..."],
  "source_url": "...",
  "stage1_score": 100,
  "document_id": "libgen-md5:...",
  "title": "...",
  "query": "...",
  "run_id": "...",
  "source_page": 1,
  "source_item": 4
}
```

## 13. Local-library reuse

Before admitting a network download, `LocalLibrary.match()` can return a unique existing PDF.

If a local match exists:

- the candidate records `local_path`;
- network transfer is skipped;
- the local PDF still enters validation;
- query-specific Stage 2 semantics are not skipped just because bytes already exist.

Ambiguous matches do not auto-resolve.

## 14. Download queue admission

New candidates are handed to `_StreamlitDownloadCoordinator.enqueue()`.

The coordinator:

1. marks whether work is primary or recovery;
2. persists `QUEUED`/`RECOVERING` before worker admission;
3. updates presentation state;
4. repeatedly attempts queue admission;
5. drains worker outcomes while waiting for queue capacity.

This is explicit backpressure: discovery can wait for capacity without allocating unlimited download jobs.

## 15. Primary downloads overlap later discovery

Once queued, download workers run independently of the producer.

Conceptually:

```text
Page 1 Stage 1 approved
      |
      +----> download worker starts
      |
producer continues
      |
Page 2 discovery + Stage 1
```

This is the main event-driven improvement over a strictly page-by-page downloader.

## 16. Worker startup

`_download_worker()` performs one document-level attempt.

It validates the attempt range and persists:

```text
DOWNLOADING
```

It then emits a worker-start event and asks `NetworkManager.begin_download()` for admission/network state.

## 17. Network admission and pacing

In the default root mode, `NetworkManager` controls new-transfer pacing.

The default configured interval is randomized between:

```text
2 and 6 seconds
```

When scheduler admission is enabled, admission responsibility shifts to the fair scheduler instead.

## 18. Mirror iteration

A candidate can contain multiple source mirrors.

For each document attempt, the worker tracks:

- visited mirror pages;
- attempted binary routes;
- failed routes.

This state is attempt-local and prevents tight loops across the same route.

## 19. Mirror-page resolution

`parse_mirror_and_download()` recognizes several route forms.

Direct binary forms:

```text
.pdf
get.php
IPFS gateway
```

Navigation/resolution forms can expose:

```text
library.lol /main/
Libgen edition/file/ads pages
GET/DOWNLOAD anchors
generic buttons or scripts
```

Candidate priority generally favors direct/known binary routes over generic fallbacks.

Unsafe local/onion destinations are blocked.

## 20. `.part` creation and resume

The binary downloader writes to:

```text
<filename>.pdf.part
```

Before every request it checks the actual partial-file size.

If `existing_size > 0`:

```http
Range: bytes=<existing_size>-
```

is added.

The actual `.part` size is more authoritative than possibly stale ledger progress.

## 21. Response handling

Successful binary responses are expected to be HTTP `200` or `206` with a compatible content type.

Important cases:

### `429`

The pipeline reads `Retry-After`, updates the route deadline, preserves state, and hands recovery back to the scheduler.

### `500/502/503/504`

The downloader can perform a small same-source request retry budget before handing the document back to global recovery.

### `416`

The Range is not satisfiable. The current partial can be reset/recovered according to active policy.

### HTML response during PDF transfer

The response is treated as a challenge/non-binary failure rather than appended to the PDF.

### Invalid content type

The route is rejected and another route/recovery path is used.

## 22. Progress persistence

Binary chunks are written frequently, but durable state is intentionally throttled.

Default thresholds:

```text
checkpoint after ~2 seconds
OR
checkpoint after ~8 MiB additional data
```

UI/network progress events are independently throttled.

Lifecycle transitions remain immediate.

## 23. Successful transfer

After a binary transfer completes successfully, the worker:

- finalizes the completed local PDF;
- records durable transfer completion;
- captures final byte size;
- schedules PDF validation;
- emits `DOWNLOAD_COMPLETED`;
- releases the download worker.

The Stage 2 result is not awaited by the download worker.

## 24. Temporary document failure

When the document attempt cannot finish, the worker records:

```text
FAILED_FOR_ROUND
```

and returns information including:

```text
candidate
attempt
retry_after_until
downloaded_bytes
reason
```

The partial file remains available for later Range resume.

## 25. Permanent failure

If the current document attempt reaches the configured maximum, the worker records:

```text
PERMANENTLY_FAILED
```

The global scheduler will not create another attempt beyond the limit.

## 26. Collecting primary outcomes

While search pages are still being produced, the coordinator's `drain()` method collects completed worker outcomes.

This updates:

- completed download count;
- local-reuse count;
- failed-result list;
- worker presentation;
- lifecycle activity.

## 27. End of primary production

After the search-page loop ends, the producer stops admitting new primary jobs.

The global primary queue is then drained fully.

Temporary failures from all pages are added to one `GlobalRecoveryBacklog`.

This means recovery is global rather than page-local.

## 28. Global recovery rounds

For each recovery round:

1. drain valid backlog candidates;
2. wait until configured/global `not_before` deadline;
3. enqueue candidates as recovery work;
4. drain the queue;
5. convert failed attempt `n` into attempt `n+1` when allowed;
6. stop when backlog is empty or all documents are terminal.

This keeps attempt arithmetic deterministic.

## 29. Stage 2 queue admission

`_schedule_validation()` runs a deterministic integrity gate before semantic validation is queued.

If the file is structurally invalid, Stage 2 semantic work is not scheduled.

If the validation queue already contains the file, it is not enqueued twice.

If capacity is available:

```text
validation_status = QUEUED
```

If capacity is full:

```text
validation_status = PENDING
```

The PDF remains intact and recoverable.

## 30. Stage 2 worker startup

`_run_validation_job()` marks:

```text
validation_status = RUNNING
```

and emits `STAGE2_STARTED`.

A timer can mark slow semantic work as `PENDING` for visibility without forcibly killing the underlying validation thread.

## 31. Content-hash deduplication before Stage 2

For downloaded files, Stage 2 can compute a SHA-256 content hash.

If a byte-identical canonical document already exists for the query:

### canonical terminal

The duplicate inherits `APPROVED` or `REJECTED` without another semantic validation.

### canonical still validating

The duplicate becomes:

```text
DUPLICATE_PENDING
```

and waits for canonical propagation.

## 32. Structural integrity gate

Before semantic evaluation, `check_pdf_integrity()` verifies:

- file exists;
- file is non-empty;
- file is large enough to plausibly be a PDF;
- header starts with `%PDF-`;
- strict PyPDF parsing succeeds;
- page count is at least one.

Technical failures produce statuses such as:

```text
PDF_INVALID
```

with a specific technical error type.

## 33. Whole-PDF evidence retrieval

For structurally usable PDFs, `retrieve_pdf_evidence()` scans all extractable pages locally.

It uses bounded token-overlap ranking to retain evidence for:

```text
ASHRAE publisher identity
query relevance
```

Only a limited page-cited evidence payload is sent to Stage 2.

This avoids uploading an entire large technical book to the LLM.

## 34. Stage 2 semantic decision

The Stage 2 graph invokes CrewAI/OpenRouter using:

```text
filename
normalized filename
PDF metadata
page count
sampled page numbers
bounded evidence
source/query metadata
```

The model returns fields similar to:

```json
{
  "approved": true,
  "ashrae_identity": true,
  "query_relevant": true,
  "score": 91,
  "confidence": "high",
  "needs_more_text": false,
  "categories": ["HVAC"],
  "reason": "..."
}
```

Python normalizes that into:

```text
APPROVED
REJECTED
PENDING
```

## 35. Conservative Stage 2 normalization

A nominal approval does not automatically become `APPROVED`.

Examples that remain `PENDING` include:

- score below approval threshold;
- poor extraction quality;
- model requests more evidence;
- conflicting publisher/query identity;
- malformed output after repair;
- provider/runtime error.

The pipeline is deliberately biased toward uncertainty rather than false acceptance.

## 36. Validation reports

Every Stage 2 result is persisted as JSON in:

```text
data/validation_reports/
```

Reports include fields such as:

```text
query
file signature
publisher identity
query relevance
status
score
confidence
categories
reason
raw model response
sampled pages
page count
PDF metadata
extraction quality
```

## 37. Final storage

Depending on status and source mode, terminal PDFs can be moved to:

```text
data/approved/
data/rejected/
```

Collision-safe filenames are used.

Existing-library PDFs are treated more conservatively and are not necessarily moved as if they were newly downloaded artifacts.

## 38. Propagating duplicate validation

When a canonical document completes Stage 2, duplicate records waiting in `DUPLICATE_PENDING` can inherit that terminal result.

No duplicate semantic LLM call is required.

## 39. Event lifecycle

Throughout the pipeline, events drive counters and UI state.

Common events:

```text
DOCUMENT_DISCOVERED
STAGE1_STARTED
STAGE1_APPROVED
STAGE1_REJECTED
DOWNLOAD_QUEUED
DOWNLOAD_STARTED
DOWNLOAD_COMPLETED
DOWNLOAD_FAILED_FOR_ROUND
DOWNLOAD_PERMANENTLY_FAILED
RECOVERY_QUEUED
STAGE2_QUEUED
STAGE2_STARTED
STAGE2_APPROVED
STAGE2_REJECTED
STAGE2_PENDING
STAGE2_INVALID
DUPLICATE_DETECTED
```

Worker threads publish data only. Streamlit rendering occurs on the main thread.

## 40. Typical successful lifecycle

```text
DISCOVERED
  -> Stage 1 APPROVED
  -> DOWNLOAD QUEUED
  -> DOWNLOADING
  -> COMPLETED
  -> Stage 2 QUEUED
  -> Stage 2 RUNNING
  -> APPROVED
  -> final_status ACCEPTED
```

## 41. Typical temporary-failure lifecycle

```text
DISCOVERED
  -> Stage 1 APPROVED
  -> DOWNLOADING attempt 1
  -> FAILED_FOR_ROUND
  -> RECOVERING attempt 2
  -> Range resume from .part
  -> COMPLETED
  -> Stage 2
```

## 42. Typical Stage 1 rejection

```text
DISCOVERED
  -> Stage 1 REJECTED
  -> final_status STAGE1_REJECTED
```

No binary acquisition occurs.

## 43. Typical technical PDF failure

```text
Stage 1 APPROVED
  -> transfer completes
  -> PDF integrity fails
  -> PDF_INVALID / technical error
```

No semantic Stage 2 LLM call is needed for clearly invalid bytes.

## 44. Typical semantic uncertainty

```text
structurally valid PDF
  -> Stage 2 evidence insufficient
  -> PENDING
```

The pipeline preserves the PDF and report rather than guessing a final classification.

## 45. Root vs Colab lifecycle note

The current Colab source has a stronger transfer-integrity lifecycle.

Colab:

```text
transfer finalizes
  -> INTEGRITY_PENDING
  -> structural validation
  -> only then COMPLETED
```

Root Streamlit currently does not use exactly the same transition semantics.

Do not assume root `COMPLETED` and current Colab `COMPLETED` have identical integrity meaning when debugging cross-version state.
