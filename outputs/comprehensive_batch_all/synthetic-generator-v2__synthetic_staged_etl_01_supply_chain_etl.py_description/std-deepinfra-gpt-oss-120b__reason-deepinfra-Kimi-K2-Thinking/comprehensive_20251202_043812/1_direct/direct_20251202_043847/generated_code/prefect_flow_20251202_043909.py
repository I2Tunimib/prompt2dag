from datetime import datetime
from typing import List

import pandas as pd
import sqlalchemy
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import quote


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Ingest raw shipment CSV data from Vendor A.",
)
def ingest_vendor_a() -> pd.DataFrame:
    """Read Vendor A CSV file and return a DataFrame."""
    # Placeholder: generate dummy data
    data = {
        "shipment_id": range(1, 1251),
        "sku": [f"SKU-{i%100:04d}" for i in range(1250)],
        "date": pd.date_range(start="2024-01-01", periods=1250, freq="H"),
        "quantity": [1] * 1250,
        "vendor": ["A"] * 1250,
    }
    return pd.DataFrame(data)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Ingest raw shipment CSV data from Vendor B.",
)
def ingest_vendor_b() -> pd.DataFrame:
    """Read Vendor B CSV file and return a DataFrame."""
    data = {
        "shipment_id": range(1251, 1251 + 980),
        "sku": [f"SKU-{i%100:04d}" for i in range(980)],
        "date": pd.date_range(start="2024-01-01", periods=980, freq="H"),
        "quantity": [2] * 980,
        "vendor": ["B"] * 980,
    }
    return pd.DataFrame(data)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Ingest raw shipment CSV data from Vendor C.",
)
def ingest_vendor_c() -> pd.DataFrame:
    """Read Vendor C CSV file and return a DataFrame."""
    data = {
        "shipment_id": range(2231, 2231 + 1750),
        "sku": [f"SKU-{i%100:04d}" for i in range(1750)],
        "date": pd.date_range(start="2024-01-01", periods=1750, freq="H"),
        "quantity": [3] * 1750,
        "vendor": ["C"] * 1750,
    }
    return pd.DataFrame(data)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Cleanse and normalize shipment data from all vendors.",
)
def cleanse_data(vendor_frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Combine, cleanse, and enrich data from all vendor DataFrames."""
    # Concatenate vendor data
    df = pd.concat(vendor_frames, ignore_index=True)

    # Normalize SKU format (example: uppercase)
    df["sku"] = df["sku"].str.upper()

    # Validate shipment dates (remove future dates)
    today = pd.Timestamp(datetime.utcnow().date())
    df = df[df["date"] <= today]

    # Enrich with location information (placeholder)
    df["location"] = "Warehouse-1"

    # Drop duplicates based on shipment_id
    df = df.drop_duplicates(subset=["shipment_id"])

    return df


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Load cleansed shipment data into PostgreSQL inventory database.",
)
def load_to_db(clean_df: pd.DataFrame) -> int:
    """Upsert records into the inventory_shipments table and return row count."""
    # Example connection string; replace with real credentials in production
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://user:password@localhost:5432/inventory_db"
    )
    # Upsert logic using pandas to_sql with if_exists='append' (simplified)
    clean_df.to_sql(
        name="inventory_shipments",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    return len(clean_df)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Send summary email after successful load.",
)
def send_summary_email(
    processed_date: datetime,
    record_count: int,
    quality_metrics: dict,
) -> None:
    """Compose and send an email summarizing the ETL run."""
    import smtplib
    from email.message import EmailMessage

    subject = f"Supply Chain Shipment ETL Summary - {processed_date.date()}"
    body = (
        f"ETL Run Date: {processed_date}\n"
        f"Total Records Processed: {record_count}\n"
        f"Data Quality Metrics:\n"
    )
    for key, value in quality_metrics.items():
        body += f"  - {key}: {value}\n"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = "no-reply@company.com"
    msg["To"] = "supply-chain-team@company.com"
    msg.set_content(body)

    # Placeholder SMTP configuration; replace with real server details
    with smtplib.SMTP("localhost") as smtp:
        smtp.send_message(msg)


@flow(description="Daily ETL pipeline for supply chain shipment data.")
def shipment_etl_flow() -> None:
    """Orchestrate extraction, transformation, loading, and notification."""
    # Stage 1: Parallel extraction
    future_a = ingest_vendor_a.submit()
    future_b = ingest_vendor_b.submit()
    future_c = ingest_vendor_c.submit()

    # Gather results (fan‑in)
    vendor_frames = [future.result() for future in (future_a, future_b, future_c)]

    # Stage 2: Transformation
    cleaned_df = cleanse_data(vendor_frames)

    # Stage 3: Load and notification
    record_count = load_to_db(cleaned_df)

    # Simple quality metrics example
    quality_metrics = {
        "records_after_cleansing": len(cleaned_df),
        "records_loaded": record_count,
    }

    send_summary_email(
        processed_date=datetime.utcnow(),
        record_count=record_count,
        quality_metrics=quality_metrics,
    )


# Schedule configuration (to be applied in deployment):
# - Cron: 0 2 * * * (daily at 02:00 UTC)
# - Start date: 2024-01-01
# - No catchup, 2 retries with 5‑minute delay

if __name__ == "__main__":
    shipment_etl_flow()