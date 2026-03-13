"""
Airflow DAG (conceptual) for the store sales pipeline.

This shows how the pieces would be orchestrated in a tool the team already
uses (Airflow), without adding extra complexity to the core project.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="store_sales_daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 3 * * *",  # daily at 03:00
    catchup=False,
    description="Daily ingestion + dbt build for store sales pipeline",
) as dag:
    # In a real deployment these environment variables would be provided
    # via Airflow connections / variables or a secrets backend.

    ingest_files = BashOperator(
        task_id="ingest_files",
        bash_command=(
            "cd /opt/airflow/store-sales-pipeline && "
            "python ingestion/ingest.py --config config/config.yaml"
        ),
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            "cd /opt/airflow/store-sales-pipeline/dbt_project && "
            "dbt run --select silver --profiles-dir ."
        ),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            "cd /opt/airflow/store-sales-pipeline/dbt_project && "
            "dbt run --select gold --profiles-dir ."
        ),
    )

    ingest_files >> dbt_run_silver >> dbt_run_gold

