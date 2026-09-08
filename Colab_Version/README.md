# Colab version — start here

Everything needed for the Colab version is contained in this folder. Its source
is independent of the Streamlit files at the repository root.

The latest [integrity lifecycle correction](source/colab/INTEGRITY_LIFECYCLE_REPORT.md)
keeps finalized transfers `INTEGRITY_PENDING` until structural validation succeeds.
It preserves the scheduler settings and separates transfer counts from verified PDFs.

| File or folder | Purpose |
|---|---|
| `Agentic_Scraper_Colab.ipynb` | Upload this notebook to Google Colab. |
| `Agentic_Scraper_Colab_source.zip` | Upload this matching ZIP through Colab's Files sidebar. |
| `source/` | Complete runnable Python source, configuration and dependencies. |
| `source/colab/pipeline_runner.py` | Colab configuration and pipeline runner. |
| `source/colab/storage.py` | Drive snapshots and restore. |
| `source/colab/distributed_coordinator.py` | Multi-Colab coordination. |
| `source/colab/db.py` and `distributed_schema.sql` | PostgreSQL client and schema. |
| `source/colab/download_benchmark.py` and `throughput_benchmark.py` | Benchmarks. |
| `source/colab/scheduler_benchmark.py` | Ten-PDF real local HTTP scheduler benchmark. |
| `source/colab/SCHEDULER_FIX_REPORT.md` | Scheduler changes, measured comparison, tests and the 2-page Colab check. |
| `source/app.py` and `source/app/` | Shared pipeline implementation copied into this version. |
| `source/config/` and `source/requirements.txt` | Settings and dependencies. |
| `build_bundle.py` | Rebuild the upload ZIP and refresh the top-level notebook after edits. |
| `tools/create_notebook.py` | Notebook generator, if its cells need regeneration. |

The Python package inside `source/` is named `colab` so existing imports such as
`from colab.pipeline_runner import ...` continue to work. It is not another copy
of the project. The top-level notebook is refreshed from its source copy whenever
the bundle is built.

## Run in Colab

1. Upload the notebook above with **File → Upload notebook**.
2. Upload the ZIP above through the **Files** sidebar.
3. Set `SOURCE_ARCHIVE = '/content/Agentic_Scraper_Colab_source.zip'` in section 2.
4. Choose a fresh extraction directory, for example
   `REPO_DIR = Path('/content/Agentic-Scrapper-organized')`.
5. Configure secrets and run the remaining cells in order.

The ZIP includes the complete `source/` layout expected by the notebook; there is
no need to upload individual Python files. Runtime data, downloaded PDFs, API keys
and virtual environments are excluded.

For this scheduler check, keep `DISTRIBUTED_MODE = False`, `DOWNLOAD_WORKERS = 5`,
`STAGE2_WORKERS = 2`, `MAX_PAGES = 2` and `TARGET_DOCUMENTS = 20`. These are the
checked-in defaults. Read [the scheduler report](source/colab/SCHEDULER_FIX_REPORT.md)
for the measured before/after result and precise benchmark instructions.
Previously added distributed scaffolding is retained; it is disabled for this work.

## Edit and rebuild

Make Colab changes inside **this folder's `source/`**, then run from the repository:

```shell
python Colab_Version/build_bundle.py
```

From `Colab_Version/source/`, the focused checks are:

```shell
python -X utf8 -m unittest discover -s colab/tests -p test_colab_pipeline.py -q
python -X utf8 -m unittest discover -s tests -p test_scheduler_utilization.py -q
python -X utf8 -m unittest discover -s tests -p test_distributed_pipeline.py -q
```

PostgreSQL tests require a disposable database in `TEST_DATABASE_URL`. Ordinary
Colab use needs no database when `DISTRIBUTED_MODE = False`.
