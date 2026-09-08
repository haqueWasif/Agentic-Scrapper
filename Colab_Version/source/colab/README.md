# Google Colab scraper

Use `Agentic_Scraper_Colab.ipynb` with its matching
`Agentic_Scraper_Colab_source.zip`. Colab frontend, storage and PostgreSQL modules
stay in this package. Downloading and semantic logic are in `../app.py` and
`../app/` inside the independent `Colab_Version/source` tree. The Streamlit
version remains separately at the repository root.

This patch targets **one Colab**. Keep 5 download workers, 2 Stage 2 workers,
2 pages, a 20-document target, and `DISTRIBUTED_MODE = False`.
[SCHEDULER_FIX_REPORT.md](SCHEDULER_FIX_REPORT.md) contains the measured comparison,
regression results, and exact instructions for a small remote run.

The latest [integrity lifecycle correction](INTEGRITY_LIFECYCLE_REPORT.md) separates
finalized transfers from structurally verified COMPLETED files. Pending integrity
work is restored without another network download, using the same validation pool.

## Upload and run one Colab

1. Upload `Agentic_Scraper_Colab.ipynb` through Colab's **File → Upload notebook**.
2. Upload `Agentic_Scraper_Colab_source.zip` through the notebook's **Files** sidebar.
3. In section 2 set
   `SOURCE_ARCHIVE = '/content/Agentic_Scraper_Colab_source.zip'` and use a fresh
   `REPO_DIR`, for example `Path('/content/Agentic-Scrapper-scheduler-fix')`.
   The bundle includes `pipeline_runner.py`, `storage.py`, the shared app and all
   dependencies/configuration files needed by the notebook. Do not flatten it.
4. Keep `DISTRIBUTED_MODE = False` for ordinary single-runtime operation.
5. Set `OPENROUTER_API_KEY` in Colab Secrets and enable notebook access. An existing
   environment variable or hidden input prompt also works. Run sections in order.

These local changes have not been pushed to GitHub. Cloning an older `main`
checkout will not install them. To update a running notebook, stop and let its
checkpoint finish, upload the new ZIP, restart the Python session, and use a fresh
extraction directory. Keep your work/storage paths to retain saved files.

## Previously added distributed scaffolding (outside this scheduler test)

Use three copies of the **same notebook**, each with the matching ZIP.

1. Supply a PostgreSQL database reachable from all runtimes. Put its full connection
   URL in a **`DATABASE_URL` Colab secret**, with notebook access enabled in each
   runtime. Use the TLS settings supplied by your database service. The database
   user must be allowed to create the scraper's tables and read/write them.
2. Set `DISTRIBUTED_MODE = True` in section 2 **before** running installation and
   secrets cells. The optional PostgreSQL package installs automatically.
3. In section 5 use identical `RUN_ID`, `QUERY`, `MAX_PAGES`, `TARGET_DOCUMENTS`,
   `SHARD_COUNT = 3`, and `SHARED_STORAGE_ID` in all three. `SHARED_STORAGE_DIR`
   must point to the **same actual mounted persistent folder**, accessible with
   read/write permissions from all three runtimes. Paths in different users'
   separate My Drives do not become shared just because their strings match.
4. Set `SHARD_ID = 0` in A, `1` in B, and `2` in C. Leave other settings identical.
5. Run the remaining cells, including the Drive mount and pipeline cell, in each.
   Tables initialize automatically and repeated initialization is safe.

For 12 pages, A searches **1,4,7,10**, B **2,5,8,11**, C **3,6,9,12**.
The database rejects conflicting manifests for the same run. Reusing the same
normalized query with a different page/shard layout in this database is also
rejected because page ownership is query-scoped.

`TARGET_DOCUMENTS = 500` means **500 globally accepted query results**, not 500
per runtime. Once reached, new download claims stop; already admitted work can
finish, so the final accepted count may exceed the target. Completed PDF count
and accepted semantic count are displayed separately.

## Storage and recovery

Active bytes go to local `/content/ashrae_work/ASHRAE_Files/*.part`. Same-runtime
handoff resumes the actual local partial size using HTTP Range. Completed files
are uploaded under immutable generation keys in shared storage, size/hash checked,
then marked globally completed with a fenced PostgreSQL update. Other runtimes
reuse that object without fetching the PDF from its source again.

Runtime snapshots have separate `runtimes/<instance-id>/` folders, preventing
Colabs from overwriting each other's `downloads.json`. A new instance does not
otherwise restore another instance's snapshots. PostgreSQL plus shared completed
objects provide cross-runtime recovery. If explicitly restoring an old partial
checkpoint, copy its snapshot into the intended runtime's local work folder before
starting; never assume a path in another Colab's `/content` is accessible.

Partial checkpointing defaults **off**. Optional checkpoints remain coarse
(600 seconds by default). No growing partial is uploaded per network chunk and
distributed correctness does not depend on partial checkpoints. After a dead
runtime loses its local disk, a new owner restarts from zero unless a partial was
explicitly restored. Lease expiry releases abandoned document work automatically.

Restart a failed shard with the same `SHARD_ID` to recover its unfinished pages.
`TAKEOVER_ABANDONED_PAGES = True` is an explicit alternative: it allows another
shard to take only a foreign page whose existing lease expired. It does not sweep
foreign pages that were never claimed. Default is **False**.

PostgreSQL is the shared state authority. SQLite on a Google Drive mount is not a
reliable distributed locking mechanism. A database outage blocks new unclaimed
downloads; existing owners stop if their lease guard expires or renewal fails.
Source HTTP 429 and Retry-After deadlines are shared across runtimes.

## Throughput and worker settings

Defaults remain **5 download / 2 Stage 2 workers** per runtime. Positive integer
overrides are supported, but no maximum remotely useful count has been measured.
Three Colabs therefore default to 15 potential download workers in total, with
shared source admission and cooldowns. Increasing workers does not establish
additional source capacity or OpenRouter quota.

Primary, partial handoff and recovery queues use bounded fairness. With five
workers, up to two handoff/recovery jobs can be admitted while eligible primary
jobs remain. Retry and start-pacing delays live in the dispatcher. A worker makes
one bounded document attempt, then returns. Recovery can progress during search;
it no longer waits for an end-of-search pass barrier.

The live view shows 30/60/300-second goodput, worker states, waiting queues, and
local/global counters. At most ten active worker rows are shown; completed and
idle rows are summarized. Detailed cumulative times and five-minute intervals
are saved to `LOCAL_WORK_DIR/throughput.json` approximately every ten seconds and
at shutdown. `logs/scraper.log` retains source diagnostics.

Run `python -m colab.throughput_benchmark` while the scraper is idle for the
synthetic scheduler comparison. It does not contact sources or measure Colab
bandwidth. See `DISTRIBUTED_IMPLEMENTATION.md` for the requested 30-point report,
measured synthetic results, and limitations of lease-based coordination.

## Verification

```shell
python -X utf8 -m unittest discover -s colab/tests -p test_colab_pipeline.py -q
python -X utf8 -m unittest discover -s tests -p test_distributed_pipeline.py -v
```

For the second command, set **`TEST_DATABASE_URL` to a disposable PostgreSQL test
database** to include real transaction, claim, shard, and integration tests.
Without it those database tests are skipped. Test rows are intentionally retained
for inspection; use a test database rather than your production database.
