from __future__ import annotations

import os
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import List

import pandas as pd
import sqlalchemy
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.hashing import hash_objects


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=None)
def ingest_vendor_a() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor A."""
    file_path = Path("data/vendor_a.csv")
    if not file_path.is_file():
        raise FileNotFoundError(f"Vendor A data file not found: {file_path}")
    df = pd.read_csv(file_path)
    return df


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=None)
def ingest_vendor_b() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor B."""
    file_path = Path("data/vendor_b.csv")
    if not file_path.is_file():
        raise FileNotFoundError(f"Vendor B data file not found: {file_path}")
    df = pd.read_csv(file_path)
    return df


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=None)
def ingest_vendor_c() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor C."""
    file_path = Path("data/vendor_c.csv")
    if not file_path.is_file():
        raise FileNotFoundError(f"Vendor C data file not found: {file_path}")
    df = pd.read_csv(file_path)
    return df


@task(retries=2, retry_delay_seconds=300)
def cleanse_data(vendor_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Consolidate and cleanse data from all vendors.

    - Normalizes SKU formats (uppercase)
    - Validates shipment dates
    - Enriches with location information (placeholder)
    """
    # Concatenate all vendor data
    df = pd.concat(vendor_dfs, ignore_index=True)

    # Normalize SKU format
    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str).str.upper()

    # Validate shipment dates
    if "shipment_date" in df.columns:
        df["shipment_date"] = pd.to_datetime(df["shipment_date"], errors="coerce")
        df = df.dropna(subset=["shipment_date"])

    # Enrich with location information (dummy enrichment)
    if "location_id" in df.columns:
        location_lookup = pd.DataFrame(
            {
                "location_id": [1, 2, 3],
                "location_name": ["Warehouse A", "Warehouse B", "Warehouse C"],
            }
        )
        df = df.merge(location_lookup, on="location_id", how="left")

    return df


@task(retries=2, retry_delay_seconds=300)
def load_to_db(clean_df: pd.DataFrame) -> int:
    """
    Load cleansed data into PostgreSQL inventory database.

    Returns the number of records upserted.
    """
    db_url = os.getenv(
        "POSTGRESQL_INVENTORY_URL",
        "postgresql+psycopg2://user:password@localhost:5432/inventory",
    )
    engine = sqlalchemy.create_engine(db_url)

    # Simple upsert using pandas to_sql (append). Real upsert would require ON CONFLICT handling.
    clean_df.to_sql(
        name="inventory_shipments",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    return len(clean_df)


@task(retries=2, retry_delay_seconds=300)
def send_summary_email(record_count: int, processing_date: datetime) -> None:
    """
    Send a summary email to the supply chain team with processing details.
    """
    recipient = "supply-chain-team@company.com"
    subject = f"Shipment Data ETL Summary - {processing_date:%Y-%m-%d}"
    body = (
        f"ETL job completed on {processing_date:%Y-%m-%d %H:%M:%S}.\n"
        f"Total records processed and loaded: {record_count}\n"
        "Data quality metrics: placeholder (all records passed validation).\n"
    )

    msg = EmailMessage()
    msg["From"] = os.getenv("SMTP_SENDER", "etl@company.com")
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.send_message(msg)


@flow(name="supply-chain-shipment-etl")
def shipment_etl_flow() -> None:
    """
    Orchestrates the three-stage ETL pipeline:
    1. Parallel extraction from three vendors.
    2. Consolidated transformation.
    3. Sequential load and email notification.
    """
    # Stage 1: Parallel extraction
    futures = [
        ingest_vendor_a.submit(),
        ingest_vendor_b.submit(),
        ingest_vendor_c.submit(),
    ]
    vendor_data = [f.result() for f in futures]

    # Stage 2: Transformation (fan‑in)
    cleaned_df = cleanse_data(vendor_data)

    # Stage 3: Load and notification (sequential)
    loaded_count = load_to_db(cleaned_df)
    send_summary_email(loaded_count, datetime.utcnow())


# Schedule: Daily execution starting 2024‑01‑01 (configure in deployment)
if __name__ == "__main__":
    shipment_etl_flow()