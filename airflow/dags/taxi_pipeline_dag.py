"""
NYC Taxi Pipeline DAG

Orchestrates the full medallion pipeline by triggering Databricks Jobs
in sequence: ingestion -> bronze -> silver -> gold.

Each task calls the Databricks Jobs API's "run now" endpoint and waits
for completion before allowing the next task to start. If any task
fails, downstream tasks are skipped and Airflow marks the DAG run as
failed -- this is what "orchestration with failure handling" means in
practice, versus just running four scripts back to back manually.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# ---------------------------------------------------------------------
# Replace these with the actual Job IDs you noted down from Databricks
# (Jobs & Pipelines -> click into each job -> copy the ID from the URL)
# ---------------------------------------------------------------------
INGESTION_JOB_ID = "649127842787636"
BRONZE_JOB_ID = "425882700869863"
SILVER_JOB_ID = "1096298154035678"
GOLD_JOB_ID = "424327424102312"

# "databricks_default" is the Airflow connection ID we'll configure in
# the Airflow UI (Admin -> Connections) with the host + token from your
# .env file. Keeping credentials in an Airflow Connection, rather than
# hardcoded in the DAG, is the standard secure pattern.
DATABRICKS_CONN_ID = "databricks_default"

default_args = {
    "owner": "heena",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="End-to-end medallion pipeline: ingestion -> bronze -> silver -> gold",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "medallion", "portfolio-project"],
) as dag:

    run_ingestion = DatabricksRunNowOperator(
        task_id="run_ingestion",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=INGESTION_JOB_ID,
    )

    run_bronze = DatabricksRunNowOperator(
        task_id="run_bronze",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_JOB_ID,
    )

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=SILVER_JOB_ID,
    )

    run_gold = DatabricksRunNowOperator(
        task_id="run_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=GOLD_JOB_ID,
    )

    # This is the DAG's core: strict linear ordering. Each stage only
    # starts once the previous one has fully succeeded.
    run_ingestion >> run_bronze >> run_silver >> run_gold
