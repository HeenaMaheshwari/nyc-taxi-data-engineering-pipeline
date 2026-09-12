# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering pipeline built on the NYC Yellow Taxi trip
dataset, implementing a medallion architecture (Bronze -> Silver -> Gold)
on Databricks, with orchestration via Apache Airflow and a business-facing
dashboard built on the final Gold layer.

## Architecture

```
NYC TLC public dataset (monthly parquet)
        |
        v
Ingestion script (simulates daily batch drops)
        |
        v
Unity Catalog Volume (landing zone)
        |
        v  Databricks Autoloader (incremental, schema-tracked)
        v
BRONZE  -- raw data, schema-enforced, append-only, full lineage metadata
        |
        v  cleaning, validation, deduplication, dimension join
        v
SILVER  -- clean, validated, enriched trip records
        |
        v  business aggregations
        v
GOLD    -- daily_summary | revenue_by_zone | hourly_patterns | payment_type_breakdown
        |
        v
Databricks SQL Dashboard (4 visualizations)

Orchestration: Apache Airflow (Dockerized) -> Databricks Jobs API
```

## Stack

- **Storage**: Unity Catalog Managed Volumes (Databricks Free Edition's
  managed-storage landing zone -- functionally equivalent to an external
  S3/ADLS bucket, see [Design Decisions](#design-decisions))
- **Compute / processing**: Databricks (Serverless), PySpark, Delta Lake
- **Ingestion**: Python (`requests`, `pandas`), simulating daily batch
  arrivals from a monthly source file
- **Orchestration**: Apache Airflow 2.9.3 (Dockerized), calling the
  Databricks Jobs API
- **Dashboard**: Databricks SQL / AI-BI Dashboard
- **Data quality**: explicit validation rules in the Silver layer, with
  before/after row counts logged at each stage

## Repo structure

```
├── ingestion/
│   └── upload_to_volume.py       # Simulates daily batch drops into the landing volume
├── databricks/
│   ├── bronze/bronze_ingest.py   # Autoloader-based incremental ingestion
│   ├── silver/silver_transform.py # Cleaning, validation, dedup, enrichment
│   └── gold/gold_aggregates.py   # Business-level aggregate tables
├── airflow/
│   ├── docker-compose.yml
│   ├── .env.example              # Template -- real .env is gitignored
│   └── dags/taxi_pipeline_dag.py # Orchestrates ingestion -> bronze -> silver -> gold
└── README.md
```

## The medallion layers

**Bronze** (`nyc_taxi_catalog.bronze.taxi_trips_raw`) — raw data landed via
Autoloader, schema-enforced but otherwise untouched. Ingestion metadata
(`_ingested_at`, `_source_file`) is added for lineage. ~2.96M rows for
January 2024.

**Silver** (`nyc_taxi_catalog.silver.taxi_trips_clean`) — cleaned and
validated. Concretely, this run:
- Dropped 18 rows with pickup dates outside the target month (source data
  contained a handful of mistyped dates, e.g. year 2002/2009/2023)
- Dropped 207,687 rows (~7%) failing trip validity checks (negative
  fares/distances, dropoff before pickup, zero passengers)
- Enriched with pickup/dropoff borough and zone names via a join against
  the public NYC TLC taxi zone lookup table (star-schema style: one fact
  table, one dimension table joined twice for pickup/dropoff roles)
- Final: 2,756,919 clean rows (~93% pass-through rate)

**Gold** (`nyc_taxi_catalog.gold.*`) — four business-facing aggregate
tables, each answering a distinct question:
- `daily_summary` — revenue/trips/fare trend by day
- `revenue_by_zone` — revenue and trip count by pickup zone
- `hourly_patterns` — trip volume and fare by hour of day (reveals rush-hour demand)
- `payment_type_breakdown` — revenue/trips by payment method

## Dashboard

Four visualizations built on the Gold tables in a Databricks SQL Dashboard:
daily revenue trend, top zones by revenue, trips by hour of day (showing
a clear early-morning low and evening rush-hour peak), and payment type
breakdown.

*(Add dashboard screenshot here)*

## Design decisions

**Why Unity Catalog Volumes instead of an external S3/ADLS bucket.**
This project was originally scoped for AWS S3, then reconsidered for
Azure Data Lake Storage Gen2 (matching the author's day-to-day work
stack). In practice, the workspace used is Databricks Free Edition,
which runs on serverless compute with Unity-Catalog-managed storage and
does not expose the ability to mount an external cloud storage account.
UC Volumes were used instead as the landing zone -- architecturally,
they play the identical role (a managed path for raw files before Bronze
ingestion); swapping in an external S3/ADLS path on a full workspace
would require no changes to the transformation logic, only the storage
path Autoloader reads from.

**Bronze uses Autoloader (streaming, `trigger(availableNow=True)`);
Silver and Gold are batch.** Bronze's job is efficient incremental file
ingestion, which is exactly Autoloader's purpose. Silver's job
(deduplication) requires reasoning over the *whole* dataset, not just
new arrivals, so a full batch read is the correct choice there, not a
limitation -- though making Silver incremental too (e.g. via Delta
Change Data Feed) is a natural next step.

**Unity Catalog governance over legacy Spark APIs.** Bronze originally
used `input_file_name()` for source-file lineage; UC rejected this
(`UC_COMMAND_NOT_SUPPORTED`) in favor of its own governed
`_metadata.file_path` column. The pipeline uses the UC-native approach.

## Known limitation: Airflow orchestration on Databricks Free Edition

The Airflow DAG (`airflow/dags/taxi_pipeline_dag.py`) is fully built and
correctly configured: it authenticates against the Databricks Jobs API
via a connection, and calls `run-now` on each of the four pipeline jobs
in sequence (ingestion -> bronze -> silver -> gold) with retry logic.

When actually triggered, Databricks returns:
```
"error_code":"FEATURE_DISABLED"
"message":"Triggering new runs for organization <org_id> is currently
disabled temporarily."
```

Root-cause testing confirmed this is a **platform-level restriction**,
not an application bug: manually clicking "Run now" in the Databricks
UI succeeds without issue, while the identical action via the
`/api/2.1/jobs/run-now` API endpoint (which Airflow uses, and which any
external orchestrator would have to use) is blocked. This points to
Databricks Free Edition specifically disabling programmatic/API-based
job triggering, while still permitting manual UI-triggered runs.

**This DAG would orchestrate the full pipeline end-to-end without any
code changes on a standard (non-Free-Edition) Databricks workspace.**
The code, connection setup, and retry/failure handling are all
demonstrated and working up to the point of the platform-level API
restriction.

## Possible next steps

- Swap Unity Catalog Volumes for an external ADLS Gen2/S3 path on a full
  workspace, re-pointing Autoloader's source path
- Make Silver incremental via Delta Change Data Feed
- Add Databricks Genie for natural-language querying over the Gold
  layer
- Add CI (GitHub Actions) for linting/testing the Python ingestion and
  transformation code on every push
- Re-test Airflow orchestration against a non-Free-Edition workspace
