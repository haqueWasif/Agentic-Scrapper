You are the Senior Systems Architect and Lead Python Engineer for this
repository:

https://github.com/haqueWasif/Agentic-Scrapper.git

I want to upgrade the CURRENT WORKING SYSTEM into a significantly more
robust, higher-throughput, fault-tolerant architecture.

This is an incremental architecture upgrade.

DO NOT rewrite the project from scratch.

First audit the current repository and understand the implementation
before changing anything.


======================================================================
CURRENT TECHNOLOGY STACK — MUST BE PRESERVED
======================================================================

Keep:

- Streamlit
- LangGraph
- CrewAI
- OpenRouter
- LiteLLM
- ZenRows
- BeautifulSoup
- curl_cffi
- PyPDF
- ThreadPoolExecutor / Python concurrency where appropriate
- existing NetworkManager
- existing Stage 1 relevance evaluation
- existing Stage 2 PDF relevance validation
- existing .part resume system
- HTTP Range resume
- existing recovery concepts
- LangSmith observability
- current config/settings.yaml architecture


DO NOT replace these with unrelated frameworks.

Do not introduce:

- Celery
- Kafka
- RabbitMQ
- Redis
- Airflow
- Kubernetes
- a distributed microservice architecture

This application is still intended to run as one local Python /
Streamlit application.

The goal is to make the CURRENT application architecture much better,
not to turn it into a distributed platform.


======================================================================
HIGH-LEVEL GOAL
======================================================================

Move from the current partially sequential/page-blocking architecture:

Search page
    |
Parse
    |
Stage 1
    |
Download that page's documents
    |
Recovery
    |
next search page


toward a:

EVENT-DRIVEN HYBRID AGENTIC PIPELINE


Desired architecture:


                         Streamlit UI
                              |
                              v
                     Pipeline Controller
                         / LangGraph
                              |
          +-------------------+------------------+
          |                   |                  |
          v                   v                  v
     Discovery            State Store        Observability
     Producer                |               / LangSmith
          |                   |
          v                   |
       Parser                 |
          |                   |
          v                   |
    Stage 1 Queue             |
          |                   |
          v                   |
 Stage 1 CrewAI/OpenRouter    |
          |                   |
          v                   |
     Approved Queue ----------+
          |
          v
    GLOBAL DOWNLOAD QUEUE
          |
    +-----+-----+-----+-----+-----+
    |     |     |     |     |     |
   W1    W2    W3    W4    W5
    |     |     |     |     |
    +-----------+-----------+
                |
        +-------+-------+
        |               |
      success       temporary failure
        |               |
        v               v
 Stage 2 Queue     Recovery Backlog
        |               |
   +----+----+          |
   |         |          |
  V1        V2          |
   |         |          |
   +----+----+          |
        |               |
        v               |
 APPROVED/REJECTED      |
        |               |
        v               |
      Storage           |
                        |
               after primary work drains
                        |
                        v
                 Recovery Round
                        |
                 same download pool


======================================================================
ARCHITECTURE CLASSIFICATION
======================================================================

The target architecture is:

Hybrid event-driven agentic pipeline.

Specifically:

- sequential dependencies between logical stages
- parallel processing within independent stages
- producer/consumer queues between stages
- bounded concurrency
- durable lifecycle state
- LangGraph orchestration
- CrewAI only where semantic reasoning is needed
- deterministic Python for network/files/state/retries
- LangSmith for observability only


Do NOT create a hierarchical CrewAI manager-agent architecture.

CrewAI should remain specialized for semantic reasoning.

Do not turn downloading, filesystem logic, retries or persistence into
LLM agents.


======================================================================
PHASE 0 — AUDIT BEFORE MODIFYING
======================================================================

Before writing code, inspect the repository and report:

1. Current main execution flow in app.py.

2. Every place where search pages are processed.

3. Every place Stage 1 is invoked.

4. Every place downloads are submitted.

5. Current ThreadPoolExecutor creation.

6. Current global max_workers behavior.

7. Current recovery scheduler.

8. Current persistent downloads.json state model.

9. Current Stage 2 executor/queue behavior.

10. Current LangGraph graphs and nodes.

11. Current LangSmith instrumentation.

12. Current document duplicate handling.

13. Current Streamlit event/telemetry flow.

14. Which parts currently block later pipeline stages.

15. Which parts can safely run concurrently.


Then show the current architecture diagram.

Only after the audit should implementation begin.


======================================================================
1. CREATE ONE CANONICAL DOCUMENT LIFECYCLE
======================================================================

Every document must have ONE stable document lifecycle.

Create or normalize a canonical document state model.

Conceptually:

{
    "document_id": "...",

    "title": "...",

    "source_metadata": {...},

    "discovery": {
        "status": "DISCOVERED"
    },

    "stage1": {
        "status": "APPROVED",
        "score": null,
        "completed": true
    },

    "download": {
        "status": "COMPLETED",
        "document_attempt": 2,
        "bytes_downloaded": 45123456,
        "part_path": "...",
        "last_error": null
    },

    "stage2": {
        "status": "APPROVED",
        "score": 95,
        "confidence": "high",
        "completed": true
    },

    "final_status": "ACCEPTED"
}


Do not necessarily use exactly this JSON schema if the current project
already has compatible state objects.

Prefer extending existing persistent state.


======================================================================
2. DOCUMENT STATE MACHINE
======================================================================

Implement clear lifecycle semantics.

Conceptual states:

DISCOVERED

    |
    v

STAGE1_PENDING

    |
    v

STAGE1_RUNNING

   / \
  /   \
 v     v

STAGE1_REJECTED
STAGE1_APPROVED
       |
       v
DOWNLOAD_QUEUED
       |
       v
DOWNLOADING
   /           \
  /             \
 v               v
COMPLETED    FAILED_FOR_ROUND
  |               |
  |               v
  |          RETRY_WAIT
  |               |
  |               v
  |        DOWNLOAD_QUEUED
  |
  v
STAGE2_PENDING
  |
  v
STAGE2_RUNNING
  |
  +-------------+-------------+
  |             |             |
  v             v             v
APPROVED     REJECTED       PENDING


Every transition must be explicit.

Avoid ambiguous states such as one boolean trying to represent multiple
pipeline stages.


======================================================================
3. IDEMPOTENCY
======================================================================

Every stage must be idempotent.

Running the pipeline twice must not duplicate completed work.


Rules:

If Stage 1 already completed:

do not call Stage 1 again unless explicitly re-evaluating.


If final PDF already exists and is valid:

do not download again.


If .part exists:

resume from existing bytes.


If Stage 2 already completed:

do not run Stage 2 again unless:

Re-validate completed PDFs

is selected.


If document already exists in a queue:

do not enqueue it again.


If document is currently DOWNLOADING:

do not assign it to another worker.


If document is COMPLETED:

do not schedule download again.


======================================================================
4. STABLE DOCUMENT IDENTITY
======================================================================

Do not rely only on filenames.

Use the strongest existing stable identity available.

Possible evidence:

- source document ID
- MD5
- canonical source record
- normalized source URL identity
- deterministic hash of source metadata


Use one stable document_id throughout:

Stage 1
download
recovery
Stage 2
storage
telemetry


Filename remains a presentation/storage property.


======================================================================
5. GLOBAL PRIMARY DOWNLOAD QUEUE
======================================================================

Remove the architecture where each search page owns a complete
download/recovery lifecycle.


Current bad behavior resembles:

Page 1
    |
Stage 1
    |
download page 1
    |
recover page 1
    |
Page 2


Target:

Search/Stage1 results continuously produce approved documents.

Approved documents enter:

GLOBAL PRIMARY DOWNLOAD QUEUE


Example:

Page 1 approved:
A B C

Page 2 approved:
D E F

Page 3 approved:
G H


Global queue:

A B C D E F G H


The global queue owns download scheduling.


======================================================================
6. SEARCH SHOULD NOT WAIT FOR RECOVERY
======================================================================

Restored incomplete documents from a previous application run must NOT
block new discovery.


At startup:

load state

    |
    v

identify incomplete/recoverable jobs

    |
    v

GLOBAL RECOVERY BACKLOG


Then continue normal search and Stage 1.


Do NOT:

await full recovery before search starts.


The application should remain productive even if one old PDF has a
problem.


======================================================================
7. PRODUCER / CONSUMER MODEL
======================================================================

Implement lightweight bounded producer/consumer queues.

Do not introduce external infrastructure.


Conceptually:


Discovery Producer
       |
       v
Stage1 Queue
       |
       v
Stage1 Consumer
       |
       v
Download Queue
       |
       v
Download Workers
       |
       v
Stage2 Queue
       |
       v
Stage2 Workers


Use standard Python concurrency primitives appropriate for the existing
Streamlit architecture.


Possible tools:

queue.Queue
asyncio.Queue
ThreadPoolExecutor

Choose based on the existing code.


Do NOT mix concurrency models unnecessarily.


======================================================================
8. BOUNDED QUEUES / BACKPRESSURE
======================================================================

Prevent unlimited queue growth.


Add configurable queue capacities.

Example:

pipeline:
  stage1_queue_maxsize: 100
  download_queue_maxsize: 50
  stage2_queue_maxsize: 20


Use sensible defaults.


If downstream is saturated:

upstream should pause/yield rather than accumulating unlimited work.


Example:

Download queue full

        |
        v

Stage1 producer pauses

        |
        v

download workers drain queue

        |
        v

Stage1 resumes


This is backpressure.


======================================================================
9. DOWNLOAD WORKER POOL
======================================================================

Keep:

download max_workers = 5


This remains configurable.


Exactly one GLOBAL download concurrency limit exists.


Never accidentally create:

5 primary workers
+
5 recovery workers


Recovery jobs must use the SAME pool.


At any moment:

active download work <= max_workers


======================================================================
10. DOWNLOAD WORKERS MUST ONLY DO DOWNLOAD WORK
======================================================================

Download workers should:

- resolve the existing permitted source candidate using current logic
- resume .part
- perform network transfer
- validate basic file completion
- emit events
- update state
- enqueue Stage 2 after success


Download workers must NOT:

- call Streamlit directly
- wait for Stage 2 LLM validation
- perform CrewAI evaluation
- hold a thread while waiting for unrelated recovery rounds


When PDF completes:

enqueue Stage 2

then immediately take another download job.


======================================================================
11. STAGE 2 MUST HAVE ITS OWN EXECUTION POOL
======================================================================

Use a separate Stage 2 validation queue.


Example:

validation:
  max_workers: 2


Current download workers:

5


Stage 2 workers:

2


These are separate resource pools.


Example:

Download Worker 1
    |
PDF completed
    |
enqueue Stage2
    |
Worker 1 immediately takes another PDF


Meanwhile:

Validation Worker 1
    |
PyPDF extraction
    |
LangGraph
    |
CrewAI
    |
OpenRouter


Do not make download workers wait for Stage 2.


Preserve the existing Stage 2 decision logic.


======================================================================
12. GLOBAL RECOVERY BACKLOG
======================================================================

Temporary download failure becomes:

FAILED_FOR_ROUND


Then:

GLOBAL RECOVERY BACKLOG


Do NOT immediately retry that document while unattempted primary jobs
remain.


Example:

Primary queue:

A B C D E F G H


B fails.

Do:

B -> recovery backlog


Continue:

C D E F G H


Only when the current primary work has drained should recovery rounds
consume the backlog.


======================================================================
13. RECOVERY ROUNDS
======================================================================

Recovery remains round-based.


Example:

Primary:

A -> success
B -> fail
C -> success
D -> fail


Recovery backlog:

B
D


Recovery Round 1:

B -> success
D -> fail


Recovery Round 2:

D -> success


Use same global worker pool.


No infinite retries.


Preserve:

max_document_attempts


======================================================================
14. ATTEMPT COUNT CORRECTNESS
======================================================================

Enforce:

1 <= document_attempt <= max_document_attempts


Never allow:

attempt 6/5


FAILED_FOR_ROUND:

previous attempt genuinely completed and failed.

Next recovery:

attempt + 1


Stale DOWNLOADING caused by application crash:

resume same logical attempt where appropriate.

Do not consume another document attempt simply because the Python
process restarted.


If persisted attempt already equals maximum and that attempt genuinely
failed:

mark:

PERMANENTLY_FAILED


Do not submit it again.


======================================================================
15. HEALTHY TRANSFER VS STALLED TRANSFER
======================================================================

Preserve progress-aware transfer behavior.


Do not abort healthy slow downloads solely because total runtime is
large.


Track:

bytes_downloaded
last_progress_time


If bytes increase:

transfer is alive.


If no meaningful progress for:

max_stall_seconds

then current request/attempt may fail according to current recovery
policy.


Do not rewrite the working curl_cffi transfer layer unnecessarily.


======================================================================
16. STATE STORE
======================================================================

The project currently uses downloads.json.

For this change:

prefer improving the existing durable state mechanism rather than
introducing a new database unless there is a compelling technical
reason.


Preserve current Windows-safe state persistence:

- thread serialization
- process coordination
- unique temp files
- atomic replacement
- bounded PermissionError retries


The state store should eventually represent the entire pipeline
lifecycle, not only raw download progress.


If it is safer to retain separate files such as:

downloads.json
validation reports

that is acceptable.

Do not create a massive persistence rewrite.


======================================================================
17. EVENT BUS / TELEMETRY
======================================================================

Create or normalize one internal event model.


Example events:

DOCUMENT_DISCOVERED
STAGE1_STARTED
STAGE1_APPROVED
STAGE1_REJECTED

DOWNLOAD_QUEUED
DOWNLOAD_STARTED
DOWNLOAD_PROGRESS
DOWNLOAD_FAILED_FOR_ROUND
DOWNLOAD_RECOVERY_QUEUED
DOWNLOAD_COMPLETED
DOWNLOAD_PERMANENTLY_FAILED

STAGE2_QUEUED
STAGE2_STARTED
STAGE2_APPROVED
STAGE2_REJECTED
STAGE2_PENDING


Workers emit events.


Streamlit main thread consumes events.


Never call:

st.*
functions

inside worker threads.


======================================================================
18. STREAMLIT SHOULD BECOME A VIEW / CONTROL PLANE
======================================================================

Do not let Streamlit UI structure dictate pipeline execution.


Streamlit should:

- start/stop the pipeline
- display status
- display counters
- consume events
- allow revalidation
- display configuration


Pipeline workers should execute independently of UI rendering details.


The application should survive ordinary Streamlit rerendering without
duplicating work.


======================================================================
19. BETTER LIVE METRICS
======================================================================

Display metrics such as:

Discovered:
X

Stage 1 pending:
X

Stage 1 approved:
X

Stage 1 rejected:
X

Download queued:
X

Downloading:
X / 5

Completed downloads:
X

Recovery backlog:
X

Permanent download failures:
X

Stage 2 queued:
X

Stage 2 running:
X / 2

Stage 2 approved:
X

Stage 2 rejected:
X

Stage 2 pending/error:
X

Final accepted documents:
X


Do not derive these counters from temporary UI widgets.

Derive them from actual state.


======================================================================
20. DUPLICATE PREVENTION BEFORE DOWNLOAD
======================================================================

Before submitting a document:

check stable document_id.


If already known:

do not enqueue duplicate work.


Normalize title/source information only as supporting evidence.


Do not use normalized title alone as the only dedup mechanism.


======================================================================
21. POST-DOWNLOAD FILE DEDUPLICATION
======================================================================

After successful download:

calculate a file hash such as:

SHA-256


Store it in document state.


If another completed PDF has the same content hash:

mark it as duplicate.


Do not run Stage 2 twice on byte-identical content unless explicitly
requested.


Do not delete files automatically in this change unless the existing
storage behavior already supports safe deduplication.


Instead record:

duplicate_of


Example:

{
    "sha256": "...",
    "duplicate_of": "document_id_xyz"
}


======================================================================
22. STAGE 1 AI ROLE
======================================================================

Do NOT change current Stage 1 relevance semantics.


Stage 1 remains:

metadata semantic filtering.


CrewAI/OpenRouter should answer the existing relevance question.


Do not add more AI agents.


======================================================================
23. STAGE 2 AI ROLE
======================================================================

Do NOT change current Stage 2 relevance semantics.


Stage 2 remains:

actual PDF evidence validation.


Continue using:

- filename
- PDF metadata
- representative pages
- extraction quality
- adaptive preview behavior
- CrewAI
- OpenRouter
- APPROVED / REJECTED / PENDING


Do not move deterministic extraction logic into the LLM.


======================================================================
24. LANGGRAPH ROLE
======================================================================

Currently LangGraph mainly wraps Stage 1 and Stage 2.


Gradually move high-level lifecycle routing into LangGraph where this
makes the code cleaner.


Do NOT attempt a giant one-shot rewrite.


Possible target graph:

START
   |
   v
DISCOVER
   |
   v
STAGE1
  / \
 /   \
reject approve
       |
       v
DOWNLOAD
  /       \
retry     success
 |           |
 +-----------+
             |
             v
          STAGE2
        /   |    \
       /    |     \
 approved rejected pending
       |
       v
      END


However:

actual network transfer execution should remain in deterministic worker
code.


LangGraph should orchestrate state/routing, not perform unnecessary
thread/network abstractions.


Implement only the amount of graph expansion that clearly improves the
current architecture.


======================================================================
25. LANGSMITH
======================================================================

Keep current optional LangSmith implementation.


Trace high-level AI operations such as:

ASHRAE-Stage1-Metadata-Evaluation

ASHRAE-Stage2-PDF-Validation


Optionally add bounded pipeline-level trace metadata such as:

run_id
document counts
stage durations


Do not send:

PDF binary data
full huge PDF text
credentials
sensitive environment values


LangSmith failure must not stop the pipeline.


Streamlit should show:

LangSmith tracing: Enabled

or:

LangSmith tracing: Disabled


======================================================================
26. PIPELINE RUN ID
======================================================================

Create a stable run_id for each explicit scraping run.


Example:

run_20260906_023500_ab12


Use it for:

telemetry
logging
state correlation
LangSmith metadata


Do not use it as document identity.


Document identity must survive multiple runs.


======================================================================
27. GRACEFUL SHUTDOWN
======================================================================

If the user stops the application or Python exits:

- stop accepting new discovery work
- allow bounded in-flight state updates
- preserve .part files
- persist document state
- do not corrupt queues/state


On next startup:

reconstruct work from durable state.


======================================================================
28. RESTART RECONSTRUCTION
======================================================================

On application restart:

scan persistent state.


Reconstruct:

- pending Stage 1 jobs if necessary
- recoverable download jobs
- completed downloads needing Stage 2
- pending Stage 2 documents


Do NOT redo completed work.


Example:

PDF complete
Stage2 incomplete

-> enqueue Stage2 only


Example:

Stage1 approved
download partial

-> recovery backlog


Example:

Stage2 approved

-> do nothing


======================================================================
29. ERROR TAXONOMY
======================================================================

Use explicit error categories.

Example:

DISCOVERY_ERROR
STAGE1_ERROR

NETWORK_TIMEOUT
NETWORK_STALL
HTTP_TRANSIENT
SOURCE_UNAVAILABLE
DOWNLOAD_VALIDATION_ERROR

PDF_EXTRACTION_ERROR
STAGE2_MODEL_ERROR
STAGE2_PARSE_ERROR

STATE_PERSISTENCE_ERROR


Persist:

error_type
error_message
timestamp


Do not overload semantic rejection with technical failure.


Technical failure:

PENDING / RETRY / ERROR


Semantic irrelevance:

REJECTED


======================================================================
30. LOGGING
======================================================================

Reduce noisy low-level output in the normal Streamlit activity panel.


Normal UI:

meaningful lifecycle events.


Debug logs:

TRACE-level HTTP/network internals.


Use Python logging consistently.


Avoid printing giant URLs, prompts or full PDF text into normal UI.


======================================================================
31. CONFIGURATION
======================================================================

Normalize configuration.

Example:

network:
  max_workers: 5
  max_request_retries_per_attempt: 2
  max_stall_seconds: 60
  max_document_attempts: 5
  recovery_round_delay_seconds: 5

validation:
  max_workers: 2

pipeline:
  stage1_queue_maxsize: 100
  download_queue_maxsize: 50
  stage2_queue_maxsize: 20

observability:
  # Continue using environment variables for secrets.
  enabled_from_environment: true


Use current configuration conventions if equivalent settings already
exist.


======================================================================
32. PERFORMANCE GOAL
======================================================================

Keep independent pipeline stages productive simultaneously.


Desired example:

T0:

Search page 1


T1:

Stage1 evaluates page 1
Search page 2 starts


T2:

Approved page-1 docs start downloading
Stage1 evaluates page 2
Search page 3 starts


T3:

Download workers continue
Stage2 validates completed PDF A
Stage1 evaluates later documents
Search continues


This is the desired pipeline parallelism.


The system should not unnecessarily wait for one complete stage batch
before another independent stage can progress.


======================================================================
33. DO NOT OVERSUBSCRIBE THE SYSTEM
======================================================================

Concurrency must remain bounded.


Download:

max 5


Stage2:

max configured validation workers


Stage1:

keep model concurrency conservative according to the current design.


Do not start hundreds of threads.


Do not create one thread per document.


======================================================================
34. TESTING — ARCHITECTURAL TESTS
======================================================================

Add focused deterministic tests.


TEST 1 — global queue

3 search batches produce:

A B C
D E F
G H I


Expected:

one global download scheduling system receives all approved jobs.


--------------------------------------------------

TEST 2 — pipeline overlap

While download A is active:

later discovery/Stage1 work can continue.

Expected:

download does not globally block discovery.


--------------------------------------------------

TEST 3 — separate Stage2 pool

PDF A completes.

Expected:

Stage2 is queued.

Download worker immediately becomes available for another PDF.


--------------------------------------------------

TEST 4 — global concurrency

max_workers = 5

50 download jobs.


Expected:

active network document workers never exceed 5.


--------------------------------------------------

TEST 5 — global recovery

B, F, J fail primary attempt.


Expected:

recovery backlog contains:

B
F
J


Recovery only uses same fixed worker pool.


--------------------------------------------------

TEST 6 — restart partial

document state:

stage1 approved
download DOWNLOADING
part file exists
attempt 3


Process restart.


Expected:

recoverable work reconstructed.

part preserved.

attempt semantics remain valid.


--------------------------------------------------

TEST 7 — Stage2 restart

PDF complete
Stage2 incomplete.


Expected:

no redownload.

Stage2 queued directly.


--------------------------------------------------

TEST 8 — duplicate queue

Same document_id discovered twice.


Expected:

only one download job.


--------------------------------------------------

TEST 9 — identical PDF hash

Two source records produce byte-identical PDFs.


Expected:

same SHA-256 detected.

second document references duplicate canonical content.

Stage2 is not unnecessarily repeated.


--------------------------------------------------

TEST 10 — queue backpressure

Download queue reaches configured maximum.


Expected:

upstream producer yields/pauses.

memory does not grow without bound.


--------------------------------------------------

TEST 11 — state consistency

Simulate frequent parallel transitions.


Expected:

state remains valid.

no duplicate DOWNLOADING assignment.


--------------------------------------------------

TEST 12 — LangSmith disabled

Expected:

pipeline works normally.


--------------------------------------------------

TEST 13 — LangSmith enabled

Expected:

existing Stage1/Stage2 traces continue.


======================================================================
35. NON-REGRESSION TESTS
======================================================================

Existing functionality must continue to work:

- ZenRows search
- BeautifulSoup parsing
- Stage1 CrewAI/OpenRouter
- curl_cffi
- .part resume
- HTTP Range
- existing PDF completion checks
- Stage2 PDF extraction
- Stage2 CrewAI/OpenRouter
- validation report writing
- LangSmith optional tracing
- Streamlit controls


======================================================================
36. IMPLEMENT IN SMALL PHASES
======================================================================

Do NOT make one giant unreviewable patch.


Implement in approximately this order:


PHASE 1

Canonical document state + state-transition helpers.


PHASE 2

Global download queue and remove page-local recovery blocking.


PHASE 3

Separate Stage2 queue/executor cleanly from download workers.


PHASE 4

Global recovery backlog and correct restart semantics.


PHASE 5

Bounded queues/backpressure.


PHASE 6

Stable document IDs + duplicate queue protection.


PHASE 7

Post-download SHA-256 duplicate detection.


PHASE 8

Improve LangGraph lifecycle routing only where beneficial.


PHASE 9

Telemetry/metrics cleanup.


After EACH phase:

run tests before continuing.


======================================================================
37. CODE QUALITY
======================================================================

Do not put even more unrelated logic into giant app.py functions.


When useful, extract focused modules such as:

app/pipeline_state.py
app/pipeline_scheduler.py
app/events.py


BUT:

do not split code merely for aesthetics.


Each new module must have one clear responsibility.


Avoid circular imports.


Keep public interfaces small.


======================================================================
38. IMPORTANT DESIGN PRINCIPLE
======================================================================

Use:

LLMs for semantic decisions.

Use:

deterministic Python for deterministic work.


Specifically:


AI:

Stage 1 relevance
Stage 2 relevance


Python:

search transport
parsing
queues
state
file IO
downloads
retries
resume
hashing
deduplication
scheduling
telemetry


Do not convert deterministic operations into CrewAI agents.


======================================================================
39. TARGET FINAL ARCHITECTURE
======================================================================

The final logical architecture should resemble:


                  ┌─────────────────────┐
                  │     STREAMLIT UI    │
                  │   Control / View    │
                  └──────────┬──────────┘
                             │ events
                             ▼
                  ┌─────────────────────┐
                  │ Pipeline Controller │
                  │ / LangGraph Routing │
                  └──────────┬──────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
      Persistent State   LangSmith        Event Bus
            │
            │
            ▼
       Discovery
            │
            ▼
          Parse
            │
            ▼
    ┌─────────────────┐
    │ Stage 1 Queue   │
    └────────┬────────┘
             │
             ▼
       CrewAI/OpenRouter
             │
             ▼
      Approved Documents
             │
             ▼
    ┌──────────────────────┐
    │ Global Download Queue│
    └──────────┬───────────┘
               │
       ┌───────┼───────────────┐
       ▼       ▼       ▼       ▼
      W1      W2      W3 ...   W5
       │
       ├─────────────── temporary failure
       │                       │
       │                       ▼
       │                Recovery Backlog
       │                       │
       │                 recovery rounds
       │                       │
       │                same worker pool
       │
       ▼
 PDF Completed
       │
       ▼
 ┌──────────────────┐
 │  Stage 2 Queue   │
 └────────┬─────────┘
          │
       ┌──┴──┐
       ▼     ▼
      V1     V2
       │
       ▼
 CrewAI/OpenRouter
       │
   ┌───┼─────┐
   ▼   ▼     ▼
Approved Rejected Pending
   │
   ▼
Final Dataset


======================================================================
40. AFTER IMPLEMENTATION
======================================================================

Provide a detailed engineering report containing:

1. Original architecture.

2. New architecture.

3. Exact blocking problems removed.

4. Files changed.

5. New modules created.

6. Document state model.

7. State-transition diagram.

8. Queue architecture.

9. Download worker lifecycle.

10. Stage2 worker lifecycle.

11. Recovery lifecycle.

12. Restart lifecycle.

13. Deduplication strategy.

14. Backpressure strategy.

15. LangGraph's final responsibility.

16. CrewAI's final responsibility.

17. LangSmith's final responsibility.

18. Streamlit's final responsibility.

19. Concurrency guarantees.

20. Persistence guarantees.

21. Test results.

22. Remaining architectural limitations.

23. Recommended future improvements, ordered by value.

Also print a final architecture diagram in plain text.

Do not make unrelated feature changes.