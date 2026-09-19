# AdMetrics Pipeline

An end-to-end ELT pipeline that turns raw digital-advertising event data into analytics-ready reporting tables, built on **Databricks + AWS S3** with a medallion (bronze → silver → gold) Delta Lake architecture.

The pipeline ingests daily ad-platform exports (impressions, clicks, spend, conversions), cleans and validates them with explicit data-quality gates, and models the results into a star schema that analysts can query for campaign performance — CTR, spend efficiency, conversion rates, and channel-level trends.

---

## Why this project

Media agencies receive messy, high-volume exports from ad platforms every day. Before anyone can build a dashboard or answer "how did this campaign perform," someone has to make that data trustworthy, fast to query, and consistently shaped. This project is that layer.

I built it to mirror a production data-engineering workflow: incremental loads, failing loudly on bad data, version-controlled notebooks, and a CI check that runs before anything merges.

---

## Architecture

```
S3 (raw daily drops)
      │
      ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │ ──▶ │   SILVER    │ ──▶ │    GOLD     │
│  raw + load │     │  cleaned &  │     │ star schema │
│   metadata  │     │  conformed  │     │ fact + dims │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │
   ingestion         data-quality        window fns,
   tracking          expectations        aggregations
                                              │
                                              ▼
                                     Analytics / BI
```

- **Bronze** — raw events landed as Delta with ingestion metadata (`load_ts`, `source_file`). Supports incremental daily loads.
- **Silver** — deduped, type-cast, null-handled, and conformed (standardized channel/campaign fields). Data-quality expectations run here and block promotion on failure.
- **Gold** — a star schema: a campaign-events fact table with `dim_campaign`, `dim_channel`, `dim_advertiser`, and `dim_date`. Window functions and aggregations (rolling CTR, running spend, spend rank per channel) live here.

---

## Tech stack

| Layer | Tools |
|---|---|
| Storage | AWS S3, Delta Lake |
| Compute / transform | Databricks, Apache Spark (PySpark), Spark SQL |
| Orchestration | Databricks Jobs (multi-task, scheduled) |
| Language | Python 3.10+, SQL |
| Version control / CI | Git, GitHub, GitHub Actions |
| Testing | pytest |

---

## Dataset

Built on the [Criteo attribution / display-advertising dataset](https://ailab.criteo.com/ressources/) — real, large-scale ad-tech data (millions of rows). Daily drops are simulated by partitioning the source so incremental ingestion and idempotent loads can be demonstrated end to end.

> Swap in any campaign-level ad dataset by adjusting the schema config in `config/`.

---

## Project structure

```
admetrics-pipeline/
│
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── setup.cfg                       # ruff / pytest / coverage config
├── .gitignore
├── .env.example                    # template for local env vars (no secrets committed)
│
├── config/
│   ├── settings.yaml               # S3 paths, catalog/schema names, run params
│   ├── schema_bronze.yaml          # expected raw column names + types
│   ├── schema_silver.yaml          # conformed schema definitions
│   └── quality_rules.yaml          # per-table data-quality thresholds
│
├── notebooks/                      # Databricks notebooks (source-format .py)
│   ├── 00_setup_env.py             # mounts / Unity Catalog volume + catalog bootstrap
│   ├── 01_bronze_ingest.py         # land raw S3 files → bronze Delta + metadata
│   ├── 02_silver_transform.py      # clean, dedupe, conform → silver Delta
│   ├── 03_gold_model.py            # build fact + dimension tables (star schema)
│   ├── 04_gold_metrics.py          # window functions + aggregate reporting tables
│   └── 99_optimize_maintain.py     # OPTIMIZE, ZORDER, VACUUM maintenance
│
├── src/
│   └── admetrics/
│       ├── __init__.py
│       ├── config.py               # load + validate YAML config
│       ├── io.py                   # read/write Delta, S3 helpers
│       ├── schema.py               # StructType defs + star-schema DDL
│       ├── transforms/
│       │   ├── __init__.py
│       │   ├── bronze.py           # add ingestion metadata, raw landing
│       │   ├── silver.py           # dedupe, casting, null handling, conforming
│       │   └── gold.py             # fact/dim builders, window-fn metrics
│       ├── quality/
│       │   ├── __init__.py
│       │   ├── expectations.py     # row-count, null-rate, referential checks
│       │   └── runner.py           # runs rules from quality_rules.yaml, fails loudly
│       └── utils/
│           ├── __init__.py
│           ├── logging.py          # structured logging setup
│           └── spark.py            # SparkSession factory (local vs Databricks)
│
├── sql/
│   ├── ddl/
│   │   ├── dim_campaign.sql
│   │   ├── dim_channel.sql
│   │   ├── dim_advertiser.sql
│   │   ├── dim_date.sql
│   │   └── fct_campaign_events.sql
│   └── reporting/
│       ├── daily_channel_performance.sql
│       ├── rolling_ctr_7d.sql
│       └── spend_rank_by_channel.sql
│
├── jobs/
│   ├── pipeline_job.json           # Databricks multi-task job (bronze→silver→gold)
│   └── maintenance_job.json        # scheduled OPTIMIZE/VACUUM job
│
├── tests/
│   ├── conftest.py                 # local SparkSession fixture
│   ├── unit/
│   │   ├── test_bronze.py
│   │   ├── test_silver.py
│   │   ├── test_gold.py
│   │   └── test_expectations.py
│   ├── integration/
│   │   └── test_end_to_end.py      # sample data through all three layers
│   └── data/
│       ├── sample_raw.csv          # tiny fixture for fast tests
│       └── expected_silver.csv
│
├── data/                           # local sample data (gitignored except samples/)
│   └── samples/
│       └── criteo_sample.csv
│
├── docs/
│   ├── architecture.md             # design decisions, medallion rationale
│   ├── data_dictionary.md          # column-level definitions for gold tables
│   └── images/
│       └── architecture.png
│
├── scripts/
│   ├── deploy_jobs.sh              # push job JSON to workspace via CLI
│   └── seed_s3_daily_drops.py      # simulate incremental daily file drops
│
└── .github/
    └── workflows/
        ├── ci.yml                  # ruff lint + pytest on every push/PR
        └── deploy.yml              # deploy notebooks + jobs on merge to main
```

---

## Data quality

Each layer promotes data only if it passes its checks. Failures stop the job rather than silently corrupting downstream tables.

- Row-count deltas within expected bounds for a daily load
- Null-rate thresholds on key columns (`campaign_id`, `spend`, `event_ts`)
- Referential integrity between fact and dimension keys
- Duplicate-event detection on the natural key

Rules live in `config/quality_rules.yaml` and are enforced by `src/admetrics/quality/runner.py`. This is the part most portfolio pipelines skip — and the part that comes from a QA background. Bad loads are caught at silver before they ever reach reporting.

---

## Running it

**Prerequisites:** a Databricks workspace, an S3 bucket (or Unity Catalog volume), and Databricks Repos connected to this GitHub repo.

1. Copy `.env.example` to `.env` and fill in your values.
2. Configure `config/settings.yaml` with your S3 path and catalog/schema names.
3. Import the repo into Databricks via **Repos → Add Repo**.
4. Deploy the jobs:
   ```bash
   ./scripts/deploy_jobs.sh
   ```
5. Trigger a run, or let the schedule pick up the next daily load.

**Local tests** (transform logic runs without a cluster):
```bash
pip install -r requirements-dev.txt
pytest tests/unit
```

---

## Performance notes

- Gold tables are `OPTIMIZE`d with `ZORDER BY (campaign_id, event_date)` for common filter patterns.
- Fact table partitioned by `event_date` to keep daily reads and merges pruned.
- Incremental loads use Delta `MERGE` for idempotent re-runs.

---

## CI/CD

- **`ci.yml`** runs on every push and PR: `ruff` lint, then `pytest tests/unit` against the pure transform functions in `src/`.
- **`deploy.yml`** runs on merge to `main`: syncs notebooks and job definitions to the Databricks workspace.

Notebooks and modules stay in sync between GitHub and Databricks via Repos, so the same version-controlled code runs in the workspace.

---

## What I'd add next

- Great Expectations or Databricks DLT expectations to formalize the quality layer
- A small dbt or SQL-based semantic layer on top of gold
- Streaming ingestion (Auto Loader) to replace simulated batch drops

---

## Author

**Sajan Shergill** — Data / AI Engineer
[Portfolio](https://sajansshergill.github.io) · [LinkedIn](https://linkedin.com/in/sajanshergill) · [GitHub](https://github.com/sajansshergill)
