# EIA Energy Grid Airflow Pipeline

A production-grade, fully automated ETL pipeline that pulls real US electricity grid data from the EIA (Energy Information Administration) API daily, validates it, transforms it into regional energy summaries, loads it into PostgreSQL, and alerts on failure — with zero manual intervention.

---

## What it does

Every day at midnight, this pipeline:

1. **Extracts** hourly electricity demand and net generation data for all US grid regions from the live EIA API
2. **Validates** the raw data for nulls, negative values, duplicates, missing regions, and implausible values
3. **Transforms** 1,900+ hourly rows into 65+ daily regional summaries — calculating total demand, total generation, and net surplus/deficit per region
4. **Loads** the clean summaries into PostgreSQL using upsert logic (safe to re-run for the same day)
5. **Quality checks** the loaded data directly in the database — row counts, null checks, math verification
6. **Alerts** on any failure via structured JSON logs (production equivalent: Slack, PagerDuty, AWS SNS)

---

## Architecture

EIA API (live) → Extract → Validate → Transform → Load → Quality Check → Alert
↓
PostgreSQL

**Stack:** Python 3.13 · Apache Airflow 3.2.1 · CeleryExecutor · PostgreSQL 16 · Redis · Pandas · Docker Compose

**Airflow setup:** CeleryExecutor with Redis message queue — same architecture used in production at scale. Workers poll Redis for tasks; the scheduler never executes code directly.

---

## Key engineering decisions

**Why CeleryExecutor?**
Tasks are distributed across separate worker processes via Redis, mirroring real production Airflow deployments where tasks may run on different machines.

**Why a 2-day data lag?**
EIA's Net Generation (`NG`) data has a 24–48 hour publication delay. Extracting "yesterday" risks incomplete data. The pipeline deliberately processes data from 2 days ago to guarantee both Demand and Generation are always present before validation runs.

**Why upsert instead of insert?**
`INSERT ... ON CONFLICT DO UPDATE` makes the pipeline idempotent — re-running the same day updates existing rows instead of creating duplicates or failing. Safe to re-run after any failure.

**Pure function + Airflow wrapper pattern**
Every script separates business logic (pure Python function, no Airflow dependency) from the Airflow wrapper (fetches context and secrets, calls the pure function). This means pipeline logic can be unit tested without a running Airflow environment.

**Secrets management**
API keys and credentials are stored as Airflow Variables (encrypted in PostgreSQL) and environment variables in `.env` (gitignored). Nothing sensitive is hardcoded or committed to the repo.

---

## Data

**Source:** [EIA Open Data API](https://www.eia.gov/opendata/) — US government electricity grid data, updated continuously by Balancing Authorities (grid operators).

**Schema — `daily_energy_summary` table:**

| Column             | Type        | Description                                                                 |
| ------------------ | ----------- | --------------------------------------------------------------------------- |
| `date`             | DATE        | Processing date (PK)                                                        |
| `respondent`       | VARCHAR(20) | Grid region code e.g. `ERCO`, `CISO` (PK)                                   |
| `total_demand`     | NUMERIC     | Sum of hourly demand across 24 hours (MWh)                                  |
| `total_generation` | NUMERIC     | Sum of hourly net generation across 24 hours (MWh)                          |
| `net_surplus`      | NUMERIC     | `total_generation - total_demand` — positive = exported, negative = deficit |

Primary key is composite `(date, respondent)` — one row per region per day.

---

## Validation checks (Validate task)

- No null values in any column
- No negative `value` entries (physically impossible for energy demand)
- No duplicate `(period, respondent, type)` combinations
- Both `D` (Demand) and `NG` (Net Generation) types present
- Minimum 30 distinct regions (guards against partial API responses)
- No individual region exceeding 500,000 MWh/hour (excludes known aggregates like `US48`)

---

## Quality checks (Quality Check task)

Runs directly against PostgreSQL after load:

- Row count in DB matches rows in clean CSV
- No nulls in key columns
- Net surplus math verified: `total_generation - total_demand = net_surplus` (±1 MWh tolerance)
- Today's date confirmed present in the table

---

## Project structure

eia-energy-grid-airflow-pipeline/
├── dags/
│ └── energy_pipeline.py # 6-task DAG definition
├── scripts/
│ ├── extract.py # EIA API pull — pure function + Airflow wrapper
│ ├── validate.py # 6 data quality checks on raw data
│ ├── transform.py # Hourly → daily aggregation + net surplus calc
│ ├── load.py # PostgreSQL upsert
│ ├── quality_check.py # Post-load DB verification
│ └── alert.py # on_failure_callback — structured failure logging
├── data/
│ ├── raw/ # Raw CSVs from EIA API (gitignored)
│ └── clean/ # Transformed daily summaries (gitignored)
├── docker-compose.yaml # Full Airflow 3.x stack — CeleryExecutor
├── .env.example # Environment variable template
└── .gitignore

---

## Running locally

**Prerequisites:** Docker Desktop, a free [EIA API key](https://www.eia.gov/opendata/register.php)

```bash
# 1. Clone the repo
git clone https://github.com/SukanyaSolase/eia-energy-grid-airflow-pipeline.git
cd eia-energy-grid-airflow-pipeline

# 2. Set up environment
cp .env.example .env
# Edit .env — add your AIRFLOW_UID, FERNET_KEY, JWT values

# 3. Start Airflow
docker compose up -d

# 4. Open the UI
# http://localhost:8081 — login: airflow / airflow

# 5. Add your EIA API key
# Admin → Variables → + → Key: eia_api_key, Value: your_key

# 6. Trigger the pipeline
# Click energy_pipeline → Trigger
```

---

## Interview talking points

- Built on **Airflow 3.x** — navigated real breaking changes from 2.x (api-server rename, dag-processor as separate service, worker zero-trust architecture via `EXECUTION_API_SERVER_URL`)
- **CeleryExecutor** with Redis — tasks distributed across workers via message queue, same pattern used in production at scale
- **Idempotent pipeline** — safe to re-run any day due to upsert logic; no duplicate data, no failures on re-runs
- **Data lag pattern** — deliberate 2-day offset to handle EIA's generation data publication delay; validates completeness before processing
- **Pure function + wrapper pattern** — business logic testable independently of Airflow
- **Ran unattended** — daily schedule, retry logic, and alert callback mean zero manual intervention required
