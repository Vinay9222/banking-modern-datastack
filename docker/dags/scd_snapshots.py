"""
Airflow DAG: SCD2 Snapshots and dbt Marts Transformation
Runs dbt snapshots to track slowly changing dimensions and refreshes analytics marts.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Orchestrate dbt snapshot runs for SCD Type 2 and build analytics data marts",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["dbt", "snapshots", "scd2", "marts"],
) as dag:

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir /home/airflow/.dbt",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir /home/airflow/.dbt",
    )

    dbt_snapshot >> dbt_run_marts