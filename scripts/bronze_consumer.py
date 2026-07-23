
"""
Bronze layer consumer.

Reads raw CDC events from every Kafka topic Debezium produces and writes
them, unmodified, as Delta Lake tables in MinIO. This is the permanent,
replayable record of every change that ever happened in the source system --
the foundation the silver and gold layers get rebuilt from if needed.
"""

import json
import os
from datetime import datetime
import pandas as pd
import boto3
from kafka import KafkaConsumer
from deltalake import write_deltalake

# MinIO connection (S3-compatible)
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ALLOW_HTTP"] = "true"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

def ensure_bucket_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket '{BUCKET}'")
    else:
        print(f"Bucket '{BUCKET}' already exists")

BUCKET = "lakehouse"
TABLES = ["customers", "orders", "order_items", "payments", "products"]
TOPICS = [f"ecom.public.{t}" for t in TABLES]

BATCH_SIZE = 200 
BATCH_TIMEOUT_MS = 5000  # or after this many ms, whichever comes first


def flatten_event(raw_value: bytes, topic: str) -> dict | None:
    """Extract a flat record from a Debezium change event."""
    if raw_value is None:
        return None
    event = json.loads(raw_value.decode("utf-8"))
    payload = event.get("payload", event)  # handles schema-wrapped or plain

    op = payload.get("op")
    after = payload.get("after")
    before = payload.get("before")

    row = after if after is not None else before
    if row is None:
        return None

    row["_cdc_op"] = op  # c=create, u=update, d=delete, r=snapshot read
    row["_cdc_ts_ms"] = payload.get("ts_ms")
    row["_source_table"] = topic.replace("ecom.public.", "")
    row["_ingested_at"] = datetime.utcnow().isoformat()
    return row


def run():
    ensure_bucket_exists()
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers="localhost:29092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze_consumer_group",
        value_deserializer=lambda v: v,  # keep raw, we decode ourselves
    )

    buffers = {table: [] for table in TABLES}
    print("Bronze consumer started. Listening for CDC events...")

    for message in consumer:
        table = message.topic.replace("ecom.public.", "")
        row = flatten_event(message.value, message.topic)
        if row is None:
            continue

        buffers[table].append(row)
        print(f"[{datetime.now()}] Buffered {table} event (op={row['_cdc_op']}), "
              f"buffer size={len(buffers[table])}")

        if len(buffers[table]) >= BATCH_SIZE:
            flush(table, buffers[table])
            buffers[table] = []


def flush(table: str, rows: list[dict]):
    if not rows:
        return
    path = f"s3://{BUCKET}/bronze/{table}"
    df = pd.DataFrame(rows)
    write_deltalake(
        path,
        df,
        mode="append",
        storage_options=STORAGE_OPTIONS,
        schema_mode="merge",
    )
    print(f"[{datetime.now()}] >>> Flushed {len(rows)} rows to {path}")

if __name__ == "__main__":
    run()
