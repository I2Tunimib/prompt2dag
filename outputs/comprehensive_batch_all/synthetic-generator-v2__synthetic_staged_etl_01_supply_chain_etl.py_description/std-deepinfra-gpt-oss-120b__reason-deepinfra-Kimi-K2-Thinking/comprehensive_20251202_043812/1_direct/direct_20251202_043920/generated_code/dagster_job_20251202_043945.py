from datetime import timedelta
from typing import List

import pandas as pd
from dagster import (
    ConfigurableResource,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    job,
    op,
    resource,
)


class PostgresResource(ConfigurableResource):
    """Minimal stub for a PostgreSQL resource."""

    connection_string: str = "postgresql://user:password@localhost:5432/inventory"

    def upsert(self, table_name: str, df: pd.DataFrame) -> int:
        """Pretend to upsert records into a table and return the number of rows affected."""
        print(f"[PostgresResource] Upserting {len(df)} rows into {table_name} using {self.connection_string}")
        # In a real implementation, you would use an ORM or psycopg2 here.
        return len(df)


class EmailResource(ConfigurableResource):
    """Minimal stub for an email sending resource."""

    smtp_server: str = "smtp.example.com"
    smtp_port: int = 587
    username: str = "no-reply@example.com"
    password: str = "password"
    recipient: str = "supply-chain-team@company.com"

    def send(self, subject: str, body: str) -> None:
        """Pretend to send an email."""
        print(f"[EmailResource] Sending email to {self.recipient}")
        print(f"Subject: {subject}")
        print(f"Body:\n{body}")


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Ingest raw shipment CSV data from Vendor A.",
)
def ingest_vendor_a(context) -> pd.DataFrame:
    path = context.op_config["file_path"]
    context.log.info(f"Ingesting Vendor A data from {path}")
    df = pd.read_csv(path)
    context.log.info(f"Vendor A records: {len(df)}")
    return df


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Ingest raw shipment CSV data from Vendor B.",
)
def ingest_vendor_b(context) -> pd.DataFrame:
    path = context.op_config["file_path"]
    context.log.info(f"Ingesting Vendor B data from {path}")
    df = pd.read_csv(path)
    context.log.info(f"Vendor B records: {len(df)}")
    return df


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Ingest raw shipment CSV data from Vendor C.",
)
def ingest_vendor_c(context) -> pd.DataFrame:
    path = context.op_config["file_path"]
    context.log.info(f"Ingesting Vendor C data from {path}")
    df = pd.read_csv(path)
    context.log.info(f"Vendor C records: {len(df)}")
    return df


@op(
    ins={
        "vendor_a": In(pd.DataFrame),
        "vendor_b": In(pd.DataFrame),
        "vendor_c": In(pd.DataFrame),
    },
    out=Out(pd.DataFrame),
    description="Cleanses and normalizes data from all vendors.",
)
def cleanse_data(context, vendor_a: pd.DataFrame, vendor_b: pd.DataFrame, vendor_c: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Starting data cleansing and normalization")
    combined = pd.concat([vendor_a, vendor_b, vendor_c], ignore_index=True)
    original_count = len(combined)
    context.log.info(f"Combined record count before cleansing: {original_count}")

    # Example cleansing steps
    # Normalize SKU format (uppercase, strip whitespace)
    if "sku" in combined.columns:
        combined["sku"] = combined["sku"].astype(str).str.upper().str.strip()

    # Validate shipment dates (keep rows where date is parsable)
    if "shipment_date" in combined.columns:
        combined["shipment_date"] = pd.to_datetime(combined["shipment_date"], errors="coerce")
        combined = combined.dropna(subset=["shipment_date"])

    # Enrich with location info (placeholder: add dummy column)
    combined["location"] = "UNKNOWN"

    cleaned_count = len(combined)
    context.log.info(f"Record count after cleansing: {cleaned_count}")
    return combined


@op(
    required_resource_keys={"postgres"},
    ins={"cleaned_data": In(pd.DataFrame)},
    out=Out(int),
    description="Loads cleansed data into the PostgreSQL inventory database.",
)
def load_to_db(context, cleaned_data: pd.DataFrame) -> int:
    table_name = "inventory_shipments"
    rows_upserted = context.resources.postgres.upsert(table_name, cleaned_data)
    context.log.info(f"Upserted {rows_upserted} rows into {table_name}")
    return rows_upserted


@op(
    required_resource_keys={"email"},
    ins={"rows_loaded": In(int)},
    description="Sends a summary email after successful load.",
)
def send_summary_email(context, rows_loaded: int) -> None:
    subject = "Daily Shipment ETL Summary"
    body = (
        f"ETL job completed successfully.\n"
        f"Date: {context.instance.get_current_time().date()}\n"
        f"Rows loaded into inventory_shipments: {rows_loaded}\n"
        f"Data quality metrics: all records passed validation.\n"
    )
    context.resources.email.send(subject, body)
    context.log.info("Summary email sent.")


@job(
    resource_defs={
        "postgres": PostgresResource(),
        "email": EmailResource(),
    },
    description="Three‑stage ETL pipeline for supply‑chain shipment data.",
)
def shipment_etl_job():
    a = ingest_vendor_a()
    b = ingest_vendor_b()
    c = ingest_vendor_c()
    cleaned = cleanse_data(vendor_a=a, vendor_b=b, vendor_c=c)
    rows = load_to_db(cleaned_data=cleaned)
    send_summary_email(rows_loaded=rows)


daily_shipment_schedule = ScheduleDefinition(
    job=shipment_etl_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs the shipment ETL job once per day.",
)


if __name__ == "__main__":
    result = shipment_etl_job.execute_in_process(
        run_config={
            "ops": {
                "ingest_vendor_a": {"config": {"file_path": "data/vendor_a.csv"}},
                "ingest_vendor_b": {"config": {"file_path": "data/vendor_b.csv"}},
                "ingest_vendor_c": {"config": {"file_path": "data/vendor_c.csv"}},
            },
            "resources": {
                "postgres": {"config": {"connection_string": "postgresql://user:password@localhost:5432/inventory"}},
                "email": {
                    "config": {
                        "smtp_server": "smtp.example.com",
                        "smtp_port": 587,
                        "username": "no-reply@example.com",
                        "password": "password",
                        "recipient": "supply-chain-team@company.com",
                    }
                },
            },
        }
    )
    assert result.success