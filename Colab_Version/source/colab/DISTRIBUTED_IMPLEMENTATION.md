# Colab throughput and distributed coordination report

Implemented locally on 2026-09-08. The notebook and matching source ZIP are in
`colab/`. Changes have not been committed or pushed. This report supersedes the
older report's end-of-search recovery-pass description; prior local storage,
resume, progress throttling, library matching, full-PDF retrieval and OpenRouter
integration remain in place.

## Findings and measurements

1. **Why throughput fell previously.** The user's historical run did not record
   windowed goodput or worker occupancy, so its exact causal breakdown cannot be
   established retrospectively. The code contained worker-side start pacing,
   recovery withheld until primary discovery/draining finished, and failed
   streaming responses without guaranteed explicit closure. These are concrete
   scheduler/resource issues addressed here, not a claim that they explain every
   slowdown. Source failures, rate limits and storage latency still need a real
   instrumented Colab run to quantify. No live source or OpenRouter load test was
   performed for this change.

2. **Worker percentages before/after.** The synthetic comparison instruments the
   existing worker-paced FIFO queue and the new fair dispatcher with identical
   simulated operations: five workers, 20 healthy jobs, 20 ms start pacing,
   20 ms resolution, 100 ms useful streaming, and 250 ms stalls. These are
   simulated bytes and delays, not internet speeds. At 18 failing recovery jobs:

   | State | Worker-paced FIFO | Fair dispatcher |
   |---|---:|---:|
   | Receiving bytes | 25.94% | 23.84% |
   | Resolving | 8.87% | 8.66% |
   | Stall | 57.00% | 53.21% |
   | Admission wait inside workers | 4.60% | 0.00% |
   | Idle | 3.60% | 14.29% |
   | Retry wait / connecting / ledger / Drive | 0% | 0% |

   Percentages include the final drain of failing jobs. Fairness protects healthy
   work but does not eliminate the failed-work tail. Consequently this experiment
   does **not** establish an overall goodput increase; see item 25.

3. **Recovery accumulation.** In the synthetic workload, increasing failed jobs
   delayed the old queue's first healthy completion from 0.172 to 1.000 seconds.
   The fair queue kept it at 0.125 seconds in all three samples. While eligible
   primary jobs exist, recovery admission is bounded. Existing active requests
   are allowed to return; newly arriving primary work does not preempt sockets.

4. **Route failure accumulation.** The user's status logs show many stalled and
   failing transfers, but do not measure the causal contribution of each host.
   Host-wide consecutive failures now feed temporary quarantine using the existing
   per-route health scores. Controlled tests verify deferral after repeated 503s.
   Failed streaming responses are explicitly closed in `finally` to release their
   resources. This fixes a resource-management gap without claiming a measured
   historical connection leak.

5. **Idle eligible-work gaps.** Restored recovery was previously withheld during
   discovery even when primary work was unavailable. Colab now admits eligible
   recovery during search. The first synthetic test also exposed a 50 ms polling
   gap in the new implementation; dispatch now wakes at the admission deadline.
   Delayed jobs and locally quarantined routes remain queued without occupying a
   network worker. Actual I/O, including database claim latency, remains work.

6. **Fairness policy.** `app/fair_download_queue.py` uses a repeating
   primary/primary/handoff/primary/recovery selection cycle, skipping unavailable
   classes. With primary work ready, at most `ceil(workers * 0.4)` handoff/recovery
   jobs may be active at admission, minimum one. With five workers this is two.
   Empty primary capacity can serve recovery. Each document has one local queue
   identity; primary, handoff and recovery cannot simultaneously own it. First
   two partial handoffs use short deferral; repeated failures use exponential
   recovery delay capped at 300 seconds. Document attempts normally stop at five.

7. **Bad-route cooldown.** Three consecutive server/connection failures at a host
   cause a 90-second local quarantine, rising with subsequent failures up to 300
   seconds. Success resets its consecutive count. Admission denial does not count
   as another HTTP failure. HTTP 429 uses Retry-After seconds or an HTTP date,
   with a 60-second fallback when absent/unusable; distributed runtimes share that
   deadline in PostgreSQL. No worker sleeps to serve those cooldowns.

## Database, ownership and storage

8. **Database.** Optional PostgreSQL, accessed through `psycopg` and a small pool
   (default four connections per runtime). `DATABASE_URL` comes from a secret or
   environment variable. The optional dependency file is
   `colab/requirements-distributed.txt`. Single-Colab mode imports no database
   client and requires no database service. Credentials are not hardcoded or
   intentionally printed; startup connection errors are sanitized.

9. **Schema.** `colab/distributed_schema.sql` creates six small tables:
   `pipeline_runs` (manifest), `pipeline_instances` (heartbeat/metrics),
   `documents` (global identity, ownership and download metadata),
   `query_document_results` (query-specific Stage 1/2 plus validation lease),
   `search_pages` (shard/page lease), and `source_limits` (shared host deadlines).
   No PDF blobs are stored. Initialization runs under an advisory transaction lock
   and is idempotent. Existing query/page assignments cannot silently acquire a
   conflicting shard layout under another run ID.

10. **Atomic claim.** `Database.claim()` locks the run row to check global accepted
    count, then performs one conditional `UPDATE ... RETURNING` on the document.
    Its predicate requires an eligible state, attempts below the limit,
    `not_before <= clock_timestamp()`, and no owner or an expired lease. It sets
    the owner, expiry and incremented fencing token. There is no unprotected
    select-then-claim sequence. Completion, progress and renewal require matching
    document, owner, token and an unexpired lease. Zero returned rows means no
    source request is authorized.

11. **Lease and heartbeat.** Defaults: 120-second lease, 25-second heartbeat
    configuration, 10-second progress tick. The coarse loop renews at roughly
    30-second intervals with these defaults. Per-chunk guards check only in-memory
    ownership/deadline; PostgreSQL writes occur on lifecycle transitions or coarse
    ticks. Upload completion also requires a still-valid fenced lease.

12. **Sharding.** A page belongs to a shard when
    `(page_number - 1) % SHARD_COUNT == SHARD_ID`. Page claims also enforce this in
    PostgreSQL. A page is marked completed after successful evaluation, including
    zero approvals. Failed retrieval/evaluation is retained as unfinished work.

13. **Three-shard example.** For 12 pages: shard 0 gets 1,4,7,10; shard 1 gets
    2,5,8,11; shard 2 gets 3,6,9,12. For 60 pages those sequences end at 58,59,60.
    Tests cover both helper partitioning and the full notebook adapter's actual
    search calls.

14. **Duplicate prevention.** MD5 is preferred; fallback identity hashes normalized
    source/title metadata, with canonical URL query ordering and no filename.
    Atomic upsert yields one global document row. A local queue deduplicates IDs;
    PostgreSQL leases serialize competing runtimes. Distributed local filenames
    include a document-ID suffix to reduce presentation-name collisions.
    Immutable storage generation keys prevent stale owners overwriting the object
    referenced by a newer completion.

    **Guarantee boundary:** PostgreSQL guarantees one valid owner and rejects stale
    commits. A lease cannot prove exactly-once physical HTTP delivery under every
    process suspension/network partition: an old socket may have bytes in flight
    when ownership expires. Workers check ownership before requests/chunks and
    stop when lease loss is observed, but strict zero overlapping network bytes in
    every failure scenario would require cooperation from the remote source.
    The concurrent healthy-runtime tests perform exactly one source transfer.

15. **Completed skip.** Before source work, completed shared metadata is checked
    against the persistent object's size and SHA-256. Valid objects are copied for
    local use, never fetched from the original PDF source again. Missing or invalid
    objects leave completed state before recovery. A local library match can be
    published by the claimant without source transfer. Shared storage uses the
    small exists/upload/download interface in `colab/shared_storage.py`.

16. **Global target.** PostgreSQL counts `APPROVED` query results globally. At the
    target, all runtimes stop admitting new download claims and further discovery.
    Existing downloads and validations may finish and cause overshoot. A completed
    transfer is counted separately from an accepted ASHRAE/query result; slow
    validation can leave more downloaded PDFs than accepted documents.

17. **Colab crash.** Its lease expires and another running runtime can claim the
    document through recovery polling, or a restarted shard can recover it. Old
    fencing tokens cannot publish completion. Resume the failed shard for its
    unfinished search pages. Optional manual foreign-page takeover requires an
    expired existing page lease; default is off. Idle runtimes that have already
    exited must be restarted to perform later recovery.

18. **Database outage.** No new uncoordinated source request starts. Claim failures
    are deferred, not converted into a local-only fallback. Renewal failure marks
    active ownership lost; deadline/chunk/request guards stop the transfer. A
    local file uploaded during an outage is not globally completed until a valid
    fenced database update succeeds. Unreferenced generation objects may remain
    after interrupted publication; automatic destructive cleanup was not added.

19. **Partial recovery.** Same-runtime handoff keeps local `.part` bytes and uses
    their real size as the Range offset. Cross-runtime jobs discard another
    runtime's recorded local path and do not assume its partial exists. Coarse DB
    byte metadata is not a partial file. A fresh runtime restarts from zero unless
    an actual persistent partial was explicitly restored.

20. **Drive writes.** Growing partial uploads default off; optional snapshots
    remain coarse (600 seconds). Completed distributed objects are saved and
    hash/size verified before the completion transaction. Runtime ledgers use
    separate instance snapshot folders. Shared Drive operations never occur per
    network chunk. The same actual shared folder must be visible to every runtime;
    identical paths on independent Drives are insufficient. Drive visibility and
    latency have not been benchmarked across live Colab machines.

## Semantics, tests and usage

21. **Stage 1 reuse.** Results are keyed by query hash, global document ID and a
    hash of the Stage 1 evaluator source (prompt version). A shard checks completed
    cached decisions before calling OpenRouter on fresh documents from its pages.
    Simultaneous first evaluations can still race before either result is saved;
    this implementation does not introduce a separate Stage 1 lease. Download
    ownership remains exclusive regardless of that race.

22. **Stage 2 reuse.** Query decisions are cached per query/document/SHA, with a
    separate validation lease. Global ASHRAE identity is stored with the document
    SHA; a different query receives the known identity and evaluates relevance.
    The integration adds that reuse instruction to the existing strict prompt.
    Rejection for one query does not invalidate global ASHRAE identity. Local-only
    cached reports cannot bypass distributed validation storage. Inconclusive/API
    failures remain pending; OpenRouter is still the backend.

23. **Download defaults.** Five workers per Colab. Explicit positive integer
    overrides remain supported. No 50-, 100- or 1,000-worker remote speed claim is
    made, and the notebook does not automatically increase concurrency.

24. **Stage 2 defaults.** Two workers per Colab. Three default runtimes may thus
    have six concurrent validations; the model/provider's quota is still relevant.
    This implementation adds no credential rotation or alternative model backend.

25. **Synthetic throughput-decay benchmark.** Run
    `python -m colab.throughput_benchmark`. One measured run:

    | Failed recovery jobs | First healthy, old → fair (s) | Last healthy, old → fair (s) | Whole-run simulated goodput, old → fair (MiB/s) |
    |---|---|---|---|
    | 2 | 0.172 → 0.125 | 0.719 → 0.828 | 27.82 → 24.15 |
    | 10 | 0.656 → 0.125 | 1.140 → 0.953 | 17.54 → 16.00 |
    | 18 | 1.000 → 0.125 | 1.579 → 0.922 | 12.67 → 11.74 |

    Fairness improves healthy-job latency under accumulating failure; total
    simulated goodput is lower in this sample because pacing and the failed tail
    still cost time. The benchmark does not demonstrate a faster source or Colab.
    Separate queue tests verify bounded recovery, no duplicate local owner, and
    useful work proceeding past a future-dated retry.

    Live instrumentation records 30/60/300-second windows, worker state seconds,
    queue waits, connection lifetime, document completion time and five-minute
    summaries to `throughput.json`. Existing prefixes/retransmitted offsets are
    excluded through per-document high-water marks; HTML with the wrong content
    type is rejected, and bytes without a PDF signature are excluded from goodput.
    If a dead owner may have uncheckpointed bytes, its takeover is conservatively
    counted as wire traffic rather than guessed new goodput. This can undercount
    useful recovery bytes; `uncertain_replay_documents` reports those cases.
    Global goodput sums recent coarse runtime rates, so it is a delayed operational
    estimate. `RECEIVING_BYTES` measures the active streaming phase, not an exact
    NIC-level occupancy trace; no-byte stalls are recognized at the timeout.

26. **Distributed checks.** 20 focused tests pass, including 12 tests against a
    real local PostgreSQL 17.6 server. They cover simultaneous claims/upserts,
    three runtimes with different filenames and one source transfer, completed
    skip, lease renewal/expiry/fencing, memory-only chunk guards, global target,
    database failure, failed upload, manifest mismatch, source deadlines, semantic
    caches, and page takeover. The full notebook adapter runs three sequential
    simulated shards through all 12 assigned pages with one source transfer;
    separate concurrent coordinator tests exercise the actual ownership race.

27. **Full test results.** The focused Colab suite passes **28 tests**, including
    notebook syntax/configuration, default operation, existing resume behavior,
    storage, RAG and a Streamlit startup/rerun smoke check. Root discovery runs
    **195 tests**, with **23 pre-existing error records** and **12 PostgreSQL tests
    skipped** when `TEST_DATABASE_URL` is absent. The PostgreSQL checks are run
    separately with the real test server and pass. Existing error groups are seven
    AST-extracted scheduler fixtures missing `NetworkManager`, fifteen legacy
    pipeline-status errors/subtest errors missing extracted dependencies, and one
    stale CrewAI `manager_llm` assertion. The full repository suite is therefore
    **not all green**; no new error group was introduced. No live Colab or source
    quota/load test has been run. Notebook/source bundle integrity and headless
    imports are checked during packaging.

28. **Start A/B/C.** Upload the matching notebook and source ZIP to each runtime;
    set `SOURCE_ARCHIVE` and a fresh extraction directory. Set
    `DISTRIBUTED_MODE=True` before dependency installation. Configure the same
    PostgreSQL `DATABASE_URL` secret, `RUN_ID`, query, page count, target, shared
    folder and `SHARD_COUNT=3`. Set A's `SHARD_ID=0`, B's `1`, C's `2`. Run cells in
    order. PostgreSQL tables initialize automatically. See `README.md` for the
    exact upload sequence, database privileges, Drive setup and restart behavior.

29. **Single-Colab compatibility.** `DISTRIBUTED_MODE=False` uses the new local fair
    queue, existing local files and optional Drive snapshots, without PostgreSQL.
    The shared Streamlit frontend retains its existing queue selection/defaults.
    Notebook tests verify headless operation and positive worker overrides.

30. **No new bypass mechanisms.** No IP/proxy rotation, account cycling, CAPTCHA
    bypass, Cloudflare bypass or source-access workaround was added. Existing
    source routes were retained. HTTP 429 waits are respected and shared across
    distributed runtimes. Source start reservation is shared and the runtime's
    existing pacing is preserved; distribution is for work partitioning and
    recovery, not additional provider entitlement.
