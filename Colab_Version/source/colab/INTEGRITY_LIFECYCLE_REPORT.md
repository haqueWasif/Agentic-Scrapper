# Integrity lifecycle correction

This is a lifecycle correction to the current single-Colab version. The notebook
and matching ZIP remain together in `Colab_Version/`. The earlier scheduler
benchmark report is historical; its premature COMPLETED semantics are superseded.

## Before and after

Before:

```text
DOWNLOADING
  -> atomic .part-to-.pdf finalization
  -> COMPLETED (before structural validation)
  -> existing Stage 2 pool: integrity, then semantics
```

A full queue, semantic timeout, restart reconciliation, or mere `.pdf` existence
could leave or create a COMPLETED record without proven integrity. Content dedupe
could also run before the integrity gate.

After:

```text
DOWNLOADING
  -> atomic .part-to-.pdf finalization
  -> INTEGRITY_PENDING / completed=false / integrity_status=PENDING
  -> network worker released, without waiting for integrity
  -> existing validation/Stage 2 pool checks PDF structure
       valid: atomically COMPLETED / completed=true / integrity_status=VALID
              -> existing semantic Stage 2 evaluation
       truncated: PARTIAL with resumable .part evidence; completed=false
       corrupt: FAILED_FOR_ROUND, evidence retained; completed=false
       read error or bytes changed during checking: remain INTEGRITY_PENDING
```

`INTEGRITY_PENDING` represents a finalized transfer awaiting structural checking;
there is no intermediate known-good COMPLETED state. The existing integrity
checker and validation-pool concurrency are preserved. No PDF parsing was added
to network workers. Only bounded state persistence and queue admission follow
transfer finalization; the worker does not wait for a validation result.

## Completion, reuse and counters

The ledger now records structural status separately from `validation_status`
(semantic Stage 2). Verified completion requires a VALID integrity record for the
file's recorded path, size and modification-time signature. A changed or missing
file is not counted as verified. The atomic ledger transaction records integrity
success and promotes COMPLETED together. Late transfer-status writes cannot
overwrite an integrity failure or a completed verification of the same bytes.

Restart reconciliation no longer infers structural validity from a `.pdf`
suffix. Unmarked files and old premature COMPLETED records in the download
directory become INTEGRITY_PENDING and receive validation. Pending files are
restored into the existing validation queue without a network request. Queue-full
files stay intact and pending; the existing capacity-restoration pass admits them
later. Pending records whose finalized bytes were actually lost can use the
existing recovery path and any surviving partial bytes.

Content hashing/dedupe only operates on structurally verified downloads, and
canonical duplicate records must also be verified. Local-library matching itself
is unchanged: finding local bytes avoids fetching the same bytes again, but does
not turn an unchecked file into a verified download. It goes through integrity
before verified completion. The pending/verified distinction also survives the
notebook runner clearing its temporary settings.

Counters have these meanings:

| Field | Meaning |
|---|---|
| `downloaded_this_run` | Network transfers completed during this run, even if integrity later fails. |
| `new_pdfs_downloaded` | Compatibility name for the same per-run transfer count. |
| `local_pdfs_reused` | Local bytes reused; this alone is not an integrity certificate. |
| `verified_pdfs_completed` | Query-scoped, ledger-backed structurally verified files. |
| `documents_downloaded` | Verified completed count in optimized Colab mode. |
| Throughput `completions` | Transfer completions, preserving the existing throughput architecture. |

The live notebook labels transfers explicitly, and the results cell separately
prints verified completed PDFs. FairDownloadQueue's per-run completion set still
deduplicates network work; it is not used as a durable integrity certificate.

## Truncation and semantic outcomes

For PDF_TRUNCATED, the finalized local PDF becomes `.pdf.part` if no partial
already exists. If a partial already exists, it is left untouched and the invalid
finalized file moves into `integrity_failed/*.invalid`. Other corrupt finalized
PDFs also move to that evidence directory and become FAILED_FOR_ROUND. Recovery
keeps the existing source/attempt evidence, limits and scheduling. No same-run
retry is forced into FairDownloadQueue's completed-transfer set: normal subsequent
recovery scanning/run can resume the durable recovery record.

External library files are never moved or edited on rejection. A rejected local
reference is removed from the recoverable candidate so it cannot be mistaken for
a verified reusable download. When Drive restoration supplied a symbolic link to
a truncated PDF, the validation-side transition materializes a real local partial
before any future Range append, preserving the local hot-storage rule.

Semantic timeout, rejection, or a report-write failure does not undo successful
structural verification. Structural success is recorded before semantic work,
and the verified path is updated when semantic storage moves a file into
approved/rejected. Semantic status can remain PENDING while download status stays
COMPLETED. Integrity failures do not proceed to semantic evaluation.

## Scope verification

The saved pre-adjustment bundle is `runtime/integrity-before.zip`. Byte comparison
confirms no changes to FairDownloadQueue, NetworkManager, the legacy scheduler,
throughput code, ProgressPolicy, the PDF integrity checker, local-library matching
and retrieval, semantic prompts/orchestration, Drive synchronization, scheduler
benchmark, or source settings. AST comparison confirms that binary streaming,
request/retry behavior, Range handling and mirror resolution are unchanged; the
binary helper only maps final transfer status to INTEGRITY_PENDING.

Notebook configuration cells are unchanged: 5 download workers, 2 validation
workers, 2 pages, 20-document target, original pacing/chunk settings, and
`DISTRIBUTED_MODE=False`. This report covers that current single-Colab mode;
previous distributed scaffolding remains disabled and was not redesigned or
validated here. The repository-root Streamlit version was not edited.

## Tests

The lifecycle suite has **16 tests: 15 passed, 1 skipped**. The skip is the real
symbolic-link test because creating a Windows symbolic link requires privileges
unavailable in this environment. The Drive snapshot/restore test passes using its
existing copy fallback. All seven requested scenarios pass: delayed integrity,
full queue, restart without downloading, truncation, valid promotion, released
network worker while integrity blocks, and semantic timeout preserving validity.

Additional passing coverage includes the real binary worker's pending state,
corrupt-file evidence, preserving an existing partial, legacy/orphan reconciliation,
late-state races, read errors, changed bytes during verification, verified-only
content dedupe, and report failure after moving a valid PDF.

The **16 scheduler regressions pass**, including the existing ten-PDF real HTTP
fixture and Range resume. The **28 focused Colab tests pass**. Full discovery runs
**227 tests with 23 error records and 13 skips**. The 23 error names match the
pre-adjustment full-suite run: pre-existing AST-fixture missing globals and an old
CrewAI expectation. Twelve skips are the existing PostgreSQL tests; the thirteenth
is the Windows symbolic-link test above. The full suite is not claimed green.

Run from `Colab_Version/source` in the installed project environment:

```shell
python -X utf8 -m unittest discover -s tests -p test_integrity_lifecycle.py -v
python -X utf8 -m unittest discover -s tests -p test_scheduler_utilization.py -v
python -X utf8 -m unittest discover -s colab/tests -p test_colab_pipeline.py -v
python -X utf8 -m unittest discover -s tests -v
```

Logs and scope evidence are in `runtime/integrity-tests.log`,
`runtime/integrity-full-tests.log`, `runtime/integrity-colab-tests.log` and
`runtime/integrity-scope-audit.json`. No external source download, LLM request or
new throughput-tuning benchmark was run. Existing local HTTP regression coverage
was retained.

## Updated upload files

Use the refreshed `Colab_Version/Agentic_Scraper_Colab.ipynb` with
`Colab_Version/Agentic_Scraper_Colab_source.zip`. Upload the matching pair, restart
the Python session and extract into a fresh source directory so cached modules
cannot retain the old completion semantics. Keep your data/Drive paths to restore
saved work. No individual Python uploads or scheduler setting changes are needed.
