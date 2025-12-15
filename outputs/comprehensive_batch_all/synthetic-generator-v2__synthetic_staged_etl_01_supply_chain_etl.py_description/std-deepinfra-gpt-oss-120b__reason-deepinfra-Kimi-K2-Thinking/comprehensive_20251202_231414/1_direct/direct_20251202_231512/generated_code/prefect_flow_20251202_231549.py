import datetime
import os
import smtplib
from email.message import EmailMessage

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import quote


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Ingest raw shipment CSV data from Vendor A.",
)
def ingest_vendor_a() -> pd.DataFrame:
    """Read Vendor A CSV file into a DataFrame."""
    logger = get_run_logger()
    file_path = os.path.join("data", "vendor_a.csv")
    logger.info("Ingesting Vendor A data from %s", file_path)
    try:
        df = pd.read_csv(file_path)
        logger.info("Vendor A records ingested: %d", len(df))
        return df
    except FileNotFoundError:
        logger.error("Vendor A file not found: %s", file_path)
        return pd.DataFrame()


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Ingest raw shipment CSV data from Vendor B.",
)
def ingest_vendor_b() -> pd.DataFrame:
    """Read Vendor B CSV file into a DataFrame."""
    logger = get_run_logger()
    file_path = os.path.join("data", "vendor_b.csv")
    logger.info("Ingesting Vendor B data from %s", file_path)
    try:
        df = pd.read_csv(file_path)
        logger.info("Vendor B records ingested: %d", len(df))
        return df
    except FileNotFoundError:
        logger.error("Vendor B file not found: %s", file_path)
        return pd.DataFrame()


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Ingest raw shipment CSV data from Vendor C.",
)
def ingest_vendor_c() -> pd.DataFrame:
    """Read Vendor C CSV file into a DataFrame."""
    logger = get_run_logger()
    file_path = os.path.join("data", "vendor_c.csv")
    logger.info("Ingesting Vendor C data from %s", file_path)
    try:
        df = pd.read_csv(file_path)
        logger.info("Vendor C records ingested: %d", len(df))
        return df
    except FileNotFoundError:
        logger.error("Vendor C file not found: %s", file_path)
        return pd.DataFrame()


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Cleanse and normalize combined vendor shipment data.",
)
def cleanse_data(
    df_a: pd.DataFrame, df_b: pd.DataFrame, df_c: pd.DataFrame
) -> pd.DataFrame:
    """Combine and clean data from all vendors."""
    logger = get_run_logger()
    logger.info("Starting data cleansing")
    combined = pd.concat([df_a, df_b, df_c], ignore_index=True)
    logger.info("Combined record count: %d", len(combined))

    # Normalize SKU format (example: uppercase and strip whitespace)
    if "sku" in combined.columns:
        combined["sku"] = combined["sku"].astype(str).str.upper().str.strip()

    # Validate shipment dates
    if "shipment_date" in combined.columns:
        combined["shipment_date"] = pd.to_datetime(
            combined["shipment_date"], errors="coerce"
        )
        before_drop = len(combined)
        combined = combined.dropna(subset=["shipment_date"])
        logger.info(
            "Dropped %d records with invalid shipment dates", before_drop - len(combined)
        )

    # Enrich with location info (placeholder: add dummy column)
    combined["location"] = "UNKNOWN"

    logger.info("Cleansed record count: %d", len(combined))
    return combined


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Load cleansed shipment data into PostgreSQL inventory database.",
)
def load_to_db(clean_df: pd.DataFrame) -> int:
    """Upsert cleaned data into the inventory_shipments table."""
    logger = get_run_logger()
    logger.info("Loading data to PostgreSQL")
    # Placeholder connection string; replace with real credentials
    db_url = os.getenv(
        "POSTGRES_URL",
        "postgresql+psycopg2://user:password@localhost:5432/inventory_db",
    )
    engine = create_engine(db_url)

    try:
        clean_df.to_sql(
            name="inventory_shipments",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        row_count = len(clean_df)
        logger.info("Successfully loaded %d records into inventory_shipments", row_count)
        return row_count
    except SQLAlchemyError as exc:
        logger.error("Database load failed: %s", exc)
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Send summary email after successful load.",
)
def send_summary_email(
    processing_date: datetime.date, record_count: int, quality_metrics: dict | None = None
) -> None:
    """Compose and send a summary email to the supply chain team."""
    logger = get_run_logger()
    logger.info("Preparing summary email")
    recipient = "supply-chain-team@company.com"
    subject = f"Shipment Data ETL Summary - {processing_date.isoformat()}"
    body = (
        f"ETL Process Date: {processing_date.isoformat()}\n"
        f"Total Records Loaded: {record_count}\n"
    )
    if quality_metrics:
        body += "\nData Quality Metrics:\n"
        for key, value in quality_metrics.items():
            body += f"- {key}: {value}\n"

    msg = EmailMessage()
    msg["From"] = "no-reply@company.com"
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    # Placeholder SMTP configuration; replace with real server details
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.send_message(msg)
        logger.info("Summary email sent to %s", recipient)
    except Exception as exc:
        logger.error("Failed to send summary email: %s", exc)
        raise


@flow(name="supply-chain-shipment-etl")
def shipment_etl_flow() -> None:
    """Orchestrate the ETL pipeline for shipment data."""
    logger = get_run_logger()
    logger.info("Starting Shipment ETL Flow")

    # Stage 1: Parallel extraction
    future_a = ingest_vendor_a.submit()
    future_b = ingest_vendor_b.submit()
    future_c = ingest_vendor_c.submit()

    # Stage 2: Transformation (fan‑in)
    cleaned_df = cleanse_data(
        df_a=future_a.result(),
        df_b=future_b.result(),
        df_c=future_c.result(),
    )

    # Stage 3: Load and notification
    loaded_count = load_to_db(cleaned_df)
    processing_date = datetime.date.today()
    # Example quality metrics placeholder
    quality_metrics = {"invalid_dates_removed": 0}
    send_summary_email(
        processing_date=processing_date,
        record_count=loaded_count,
        quality_metrics=quality_metrics,
    )

    logger.info("Shipment ETL Flow completed successfully")


# Schedule: Daily execution starting 2024-01-01 (configure in deployment)
if __name__ == "__main__":
    shipment_etl_flow()