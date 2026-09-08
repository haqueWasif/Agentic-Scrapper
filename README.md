# Agentic Scraper

Agentic Scraper is a hybrid event-driven document-acquisition pipeline for discovering, evaluating, downloading, validating, and organizing ASHRAE/HVAC technical documents.

It combines deterministic scraping/network logic with LLM-based semantic reasoning:

- **Streamlit** for the local UI
- **BeautifulSoup**, **ZenRows**, and **curl_cffi** for discovery and retrieval
- **LangGraph + CrewAI + OpenRouter** for semantic Stage 1 and Stage 2 decisions
- **ThreadPoolExecutor**-based bounded queues for concurrent downloads and PDF validation
- resumable `.part` files and **HTTP Range** downloads
- durable `downloads.json` lifecycle state and restart recovery
- **PyPDF** structural validation and bounded whole-PDF evidence retrieval
- SHA-256 content deduplication
- optional **LangSmith** observability
- a separate Google Colab runtime with Drive persistence and optional distributed coordination

> The repository contains two related but operationally distinct versions. The root Streamlit implementation and `Colab_Version/` should not be treated as identical copies.

## Runtime versions

| Version | Location | Start here |
|---|---|---|
| **Local Streamlit** | Repository root: `app.py`, `app/`, `config/` | `streamlit run app.py` |
| **Google Colab** | [`Colab_Version/`](Colab_Version/) | [`Colab_Version/README.md`](Colab_Version/README.md) |

All Colab-specific code, notebook files, upload bundles, configuration, and dependencies live under `Colab_Version/`. Make Colab edits inside `Colab_Version/source/`, then rebuild the upload bundle with:

```bash
python Colab_Version/build_bundle.py
```

## Documentation

Start with the document that matches what you are trying to do:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system architecture, modules, queues, state model, concurrency, and root-vs-Colab differences
- [`docs/PIPELINE.md`](docs/PIPELINE.md) — complete document lifecycle from search page to final PDF classification
- [`docs/DEVELOPER_GUIDE.md`](docs/DEVELOPER_GUIDE.md) — local setup, configuration, testing, debugging, and where to make changes
- [`docs/COLAB.md`](docs/COLAB.md) — Colab runtime, Drive persistence, fair scheduling, integrity lifecycle, and optional distributed mode
- [`ARCHITECTURE_UPGRADE.md`](ARCHITECTURE_UPGRADE.md) — historical/implementation architecture-upgrade specification

## High-level pipeline

```text
Search / Discovery
        |
        v
HTML parsing + provenance-preserving mirror extraction
        |
        v
Stage 1 semantic relevance evaluation
LangGraph -> CrewAI -> OpenRouter
        |
        +---------------- rejected ----------------> durable lifecycle state
        |
      approved
        v
Local-library reuse check
        |
        +---------------- existing PDF ------------+
        |                                          |
        v                                          v
Bounded global download queue                 Stage 2 queue
        |
   fixed workers
        |
Mirror resolution
        |
Resumable .part + HTTP Range transfer
        |
        +---------- temporary failure ----------> Recovery backlog
        |
      success
        v
PDF structural integrity gate
        |
        v
Whole-PDF bounded evidence retrieval
        |
        v
Stage 2 semantic validation
LangGraph -> CrewAI -> OpenRouter
        |
        +--------------+--------------+
        |              |              |
     APPROVED       REJECTED        PENDING
```

The important design rule is:

> **LLMs make semantic decisions; deterministic Python owns networking, files, retries, queues, persistence, integrity, and scheduling.**

## Quick start — local Streamlit

### 1. Clone and create an environment

```bash
git clone https://github.com/haqueWasif/Agentic-Scrapper.git
cd Agentic-Scrapper
python -m venv .venv
```

Activate the environment for your platform, then install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

On compatible shells you can also run:

```bash
bash setup.sh
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and configure the values needed by your environment.

Important variables:

```env
ZENROWS_API_KEY=
OPENROUTER_API_KEY=
LANGSMITH_API_KEY=
LANGSMITH_TRACING=false
LANGSMITH_PROJECT=ashrae-intelligent-scraper
LANGSMITH_WORKSPACE_ID=
```

`OPENROUTER_API_KEY` is required for semantic Stage 1/Stage 2 evaluation. ZenRows is optional in the main root pipeline because direct browser-style fallbacks are also implemented.

### 3. Run the app

```bash
streamlit run app.py
```

The local UI lets you configure the query, maximum search pages, target document count, run the acquisition pipeline, and scan existing PDFs directly through Stage 2.

## Default concurrency and network configuration

Root defaults are stored in [`config/settings.yaml`](config/settings.yaml):

```yaml
network:
  max_workers: 5
  new_download_min_seconds: 2
  new_download_max_seconds: 6
  max_request_retries_per_attempt: 2
  max_stall_seconds: 60
  max_document_attempts: 5

pipeline:
  download_queue_maxsize: 50
  stage2_queue_maxsize: 20

validation:
  max_workers: 2
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) before changing concurrency or retry behavior.

## Important runtime data

Runtime files are intentionally excluded from Git. Depending on which features are used, the application can create data such as:

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

`.part` files are meaningful recovery artifacts. Do not delete them while investigating an interrupted download unless you intentionally want to restart that transfer from zero.

## Tests

The repository includes focused offline tests for:

- batch-safe Stage 1 evaluation
- LLM JSON parsing and provider behavior
- global download queue limits and backpressure
- recovery attempt arithmetic
- mirror/gateway routing
- HTTP Range resume
- durable state writing
- pipeline lifecycle state
- PDF integrity
- Stage 2 result parsing and deduplication
- event/UI presentation
- observability

Run the test suite from the repository root with your normal Python test environment, for example:

```bash
python -X utf8 -m unittest discover -s tests -v
```

For Colab-specific focused commands, see [`docs/COLAB.md`](docs/COLAB.md).

## Project structure

```text
Agentic-Scrapper/
├── app.py                         # Root Streamlit entry point and main pipeline controller
├── app/
│   ├── agents/
│   │   ├── orchestrator.py        # Stage 1 and Stage 2 LangGraph/CrewAI workflows
│   │   └── storage.py             # Stateless CrewAI storage policy
│   ├── download_progress.py       # Throttled progress/state persistence
│   ├── fair_download_queue.py     # Fair admission scheduler used by optimized modes
│   ├── local_library.py           # Existing-PDF matching and bounded PDF evidence retrieval
│   ├── network_manager.py         # Network sessions, rate limits, routes, proxy state
│   ├── observability.py           # Optional LangSmith tracing
│   ├── pdf_validation.py          # PDF integrity, reports, validation folders
│   ├── pipeline_events.py         # Event bus, UI presentation, run metrics
│   ├── pipeline_scheduler.py      # Download queue, Stage 2 queue, recovery backlog
│   ├── pipeline_state.py          # Stable IDs and canonical lifecycle view
│   ├── semantic_extractor.py      # HTML -> evaluator-friendly text
│   ├── stealth_scraper.py         # Standalone ZenRows fetch helper
│   └── throughput.py              # Goodput and worker-utilization telemetry
├── config/
│   ├── settings.yaml
│   └── proxies.txt
├── tests/
├── docs/
├── Colab_Version/
├── ARCHITECTURE_UPGRADE.md
├── requirements.txt
└── setup.sh
```

## Current implementation note

The current Colab source has a newer **integrity-before-COMPLETED** lifecycle than the root Streamlit implementation. In current Colab mode, a finalized transfer is kept in `INTEGRITY_PENDING` until structural PDF validation succeeds. The root implementation has not yet been normalized to exactly the same lifecycle semantics.

Read [`docs/COLAB.md`](docs/COLAB.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) before porting state logic between the two versions.

## Responsible use

Use the acquisition pipeline only for material you are authorized to access and in accordance with applicable copyright law, website policies, rate limits, and network-access rules. The current scheduler includes pacing, bounded retries, and explicit `Retry-After` handling; changes should preserve those responsible-access constraints.
