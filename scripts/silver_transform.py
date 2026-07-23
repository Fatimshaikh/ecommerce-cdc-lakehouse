
"""
Silver layer transform.

Reads every bronze Delta table (raw CDC event log) and collapses it into
one clean, current row per primary key -- deduplicating by taking the
latest _cdc_ts_ms per key, and dropping rows whose latest event was a
delete. This is what "current state" means in a CDC-based lakehouse:
bronze is the full history, silver is the present moment.

Note: this version does a full overwrite of each silver table on every run
(simple and correct for a portfolio project). Phase 5 (Airflow) will later
upgrade this to a proper incremental merge for efficiency at scale --
worth mentioning in interviews as a deliberate simplification, not an
oversight.
"""

from datetime import datetime

import pandas as pd
from deltalake import DeltaTable, write_deltalake

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

BUCKET = "lakehouse"

# primary key per table -- this is what we dedupe on
TABLE_PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}


def transform_table(table_name: str, pk: str):
    bronze_path = f"s3://{BUCKET}/bronze/{table_name}"
    silver_path = f"s3://{BUCKET}/silver/{table_name}"

    dt = DeltaTable(bronze_path, storage_options=STORAGE_OPTIONS)
    df = dt.to_pandas()

    if df.empty:
        print(f"[{datetime.now()}] {table_name}: bronze is empty, skipping")
        return

    before_count = len(df)

    # Drop rows whose latest event was a delete -- they shouldn't exist
    # in "current state" silver tables.
    df = df.sort_values("_cdc_ts_ms")
    latest_per_key = df.drop_duplicates(subset=[pk], keep="last")
    deleted_keys = set(
        latest_per_key.loc[latest_per_key["_cdc_op"] == "d", pk]
    )
    silver_df = latest_per_key[~latest_per_key[pk].isin(deleted_keys)]

    write_deltalake(
        silver_path,
        silver_df,
        mode="overwrite",
        storage_options=STORAGE_OPTIONS,
        schema_mode="overwrite",
    )
    print(
        f"[{datetime.now()}] {table_name}: {before_count} bronze events "
        f"-> {len(silver_df)} current silver rows "
        f"({len(deleted_keys)} deleted excluded)"
    )


def run():
    print(f"[{datetime.now()}] Starting silver transform...")
    for table, pk in TABLE_PRIMARY_KEYS.items():
        transform_table(table, pk)
    print(f"[{datetime.now()}] Silver transform complete.")


if __name__ == "__main__":
    run()
