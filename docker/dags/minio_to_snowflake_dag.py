"""
Airflow DAG: MinIO to Snowflake Banking Ingestion
Transfers partitioned Parquet data from MinIO S3 object storage into Snowflake raw staging tables.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment configuration
local_env = Path(__file__).parent / ".env"
root_env = Path(__file__).parent.parent.parent / ".env"

if local_env.exists():
    load_dotenv(dotenv_path=local_env)
elif root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

# -------- MinIO S3 Configuration --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "raw")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Configuration --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "BANKING")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

TABLES = ["customers", "accounts", "transactions"]
logger = logging.getLogger("airflow.task")


def download_from_minio():
    """Download Parquet files from MinIO buckets to local temporary worker storage."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    local_files = {}

    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []

        for obj in objects:
            key = obj["Key"]
            if key.endswith(".parquet"):
                local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
                s3.download_file(BUCKET, key, local_file)
                logger.info("Downloaded %s -> %s", key, local_file)
                local_files[table].append(local_file)

    return local_files


def load_to_snowflake(**kwargs):
    """Stage and load local Parquet files into Snowflake Raw tables via internal stage."""
    ti = kwargs["ti"]
    local_files = ti.xcom_pull(task_ids="download_minio")

    if not local_files:
        logger.info("No Parquet files found in MinIO download stage.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    try:
        for table, files in local_files.items():
            if not files:
                logger.info("No new files for table '%s', skipping.", table)
                continue

            for f in files:
                cur.execute(f"PUT file://{f} @%{table} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                logger.info("Uploaded %s to @%%%s stage", f, table)

            copy_sql = f"""
            COPY INTO {table}
            FROM @%{table}
            FILE_FORMAT=(TYPE=PARQUET)
            ON_ERROR='CONTINUE'
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            """
            cur.execute(copy_sql)
            logger.info("Successfully executed COPY INTO for %s", table)
    finally:
        cur.close()
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Automated ingestion of MinIO Parquet events into Snowflake Raw tables",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["minio", "snowflake", "ingestion", "raw"],
) as dag:

    download_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    download_task >> load_task