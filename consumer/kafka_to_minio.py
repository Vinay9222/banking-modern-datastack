import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer
import pandas as pd

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("kafka_to_minio")

# ---------------------------------------------------------------------
# Load Environment Configuration
# ---------------------------------------------------------------------
# Try local directory .env first, then root project .env
local_env = Path(__file__).parent / ".env"
root_env = Path(__file__).parent.parent / ".env"

if local_env.exists():
    load_dotenv(dotenv_path=local_env)
elif root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "minio-landing-group")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

# ---------------------------------------------------------------------
# Initialize S3 / MinIO Client
# ---------------------------------------------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# Ensure target bucket exists
try:
    existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing_buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        logger.info("Created MinIO bucket '%s'", MINIO_BUCKET)
except Exception as err:
    logger.warning("Could not verify/create MinIO bucket '%s': %s", MINIO_BUCKET, err)


def write_to_minio(table_name: str, records: list) -> None:
    """Write batched records to a partitioned Parquet file in MinIO."""
    if not records:
        return

    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y-%m-%d")
    timestamp_str = now_utc.strftime("%H%M%S%f")
    temp_file = f"{table_name}_{timestamp_str}.parquet"
    s3_key = f"{table_name}/date={date_str}/{table_name}_{timestamp_str}.parquet"

    try:
        df = pd.DataFrame(records)
        df.to_parquet(temp_file, index=False)
        s3.upload_file(temp_file, MINIO_BUCKET, s3_key)
        logger.info("Uploaded %d records to s3://%s/%s", len(records), MINIO_BUCKET, s3_key)
    except Exception as err:
        logger.error("Failed writing records to MinIO: %s", err)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)


def main():
    topics = [
        "banking_server.public.customers",
        "banking_server.public.accounts",
        "banking_server.public.transactions",
    ]

    logger.info("Connecting to Kafka at %s...", KAFKA_BOOTSTRAP)
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    buffer = {topic: [] for topic in topics}
    logger.info("Connected to Kafka. Listening for CDC messages on topics: %s", topics)

    for message in consumer:
        topic = message.topic
        event = message.value
        payload = event.get("payload", {}) if isinstance(event, dict) else {}
        record = payload.get("after") if isinstance(payload, dict) else None

        if record:
            buffer[topic].append(record)
            logger.debug("[%s] -> %s", topic, record)

        if len(buffer[topic]) >= BATCH_SIZE:
            table_name = topic.split(".")[-1]
            write_to_minio(table_name, buffer[topic])
            buffer[topic] = []


if __name__ == "__main__":
    main()