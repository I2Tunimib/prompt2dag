from datetime import timedelta
from typing import List

import pandas as pd
from dagster import (
    ConfigurableResource,
    InitResourceContext,
    In,
    Out,
    RetryPolicy,
    JobDefinition,
    OpDefinition,
    ResourceDefinition,
    ScheduleDefinition,
    Definitions,
    job,
    op,
    schedule,
)


class DatabaseResource(ConfigurableResource):
    """A stub database resource for upserting data."""

    def upsert(self, table: str, df: pd.DataFrame) -> int:
        """Pretend to upsert rows into a table and return the number of rows."""
        row_count = len(df)
        get_logger().info(f"Upserting {row_count} rows into table '{table}'.")
        # Insert real database logic here.
        return row_count


class EmailResource(ConfigurableResource):
    """A stub email resource for sending notifications."""

    def send(self, to: str, subject: str, body: str) -> None:
        """Pretend to send an email."""
        get_logger().info(f"Sending email to {to} with subject '{subject}'.")
        # Insert real email sending logic here.
        get_logger().debug(f"Email body:\n{body}")


def get_logger():
    """Utility to obtain Dagster logger."""
    from dagster import get_dagster_logger

    return get_dagster_logger()


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    config_schema={"csv_path": str},
    out=Out(pd.DataFrame),
    description="Ingest raw shipment CSV data from Vendor A.",
)
def ingest_vendor_a(context) -> pd.DataFrame:
    path = context.op_config["csv_path"]
    context.log.info(f"Ingesting Vendor A data from {path}")
    df = pd.read_csv(path)
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    config_schema={"csv_path": str},
    out=Out(pd.DataFrame),
    description="Ingest raw shipment CSV data from Vendor B.",
)
def ingest_vendor_b(context) -> pd.DataFrame:
    path = context.op_config["csv_path"]
    context.log.info(f"Ingesting Vendor B data from {path}")
    df = pd.read_csv(path)
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    config_schema={"csv_path": str},
    out=Out(pd.DataFrame),
    description="Ingest raw shipment CSV data from Vendor C.",
)
def ingest_vendor_c(context) -> pd.DataFrame:
    path = context.op_config["csv_path"]
    context.log.info(f"Ingesting Vendor C data from {path}")
    df = pd.read_csv(path)
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    ins={
        "vendor_a": In(pd.DataFrame),
        "vendor_b": In(pd.DataFrame),
        "vendor_c": In(pd.DataFrame),
    },
    out=Out(pd.DataFrame),
    description="Cleanses and normalizes data from all vendors.",
)
def cleanse_data(context, vendor_a: pd.DataFrame, vendor_b: pd.DataFrame, vendor_c: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Starting data cleansing and normalization.")
    # Concatenate all vendor data
    combined = pd.concat([vendor_a, vendor_b, vendor_c], ignore_index=True)

    # Standardize SKU format (uppercase)
    if "sku" in combined.columns:
        combined["sku"] = combined["sku"].astype(str).str.upper()

    # Validate shipment dates and drop invalid rows
    if "shipment_date" in combined.columns:
        combined["shipment_date"] = pd.to_datetime(combined["shipment_date"], errors="coerce")
        before_drop = len(combined)
        combined = combined.dropna(subset=["shipment_date"])
        after_drop = len(combined)
        context.log.info(f"Dropped {before_drop - after_drop} rows with invalid shipment dates.")

    # Enrich with location information (dummy enrichment)
    combined["location"] = "UNKNOWN"

    context.log.info(f"Cleansed dataset contains {len(combined)} records.")
    return combined


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"db"},
    ins={"cleaned_data": In(pd.DataFrame)},
    out=Out(int),
    description="Loads cleansed data into the PostgreSQL inventory database.",
)
def load_to_db(context, cleaned_data: pd.DataFrame) -> int:
    db: DatabaseResource = context.resources.db
    row_count = db.upsert(table="inventory_shipments", df=cleaned_data)
    context.log.info(f"Loaded {row_count} records into the database.")
    return row_count


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"email"},
    ins={"row_count": In(int), "cleaned_data": In(pd.DataFrame)},
    description="Sends a summary email after successful load.",
)
def send_summary_email(context, row_count: int, cleaned_data: pd.DataFrame) -> None:
    email: EmailResource = context.resources.email
    subject = "Daily Shipment ETL Summary"
    body = (
        f"ETL Process Completed Successfully.\n"
        f"Date: {pd.Timestamp.now().date()}\n"
        f"Total Records Processed: {row_count}\n"
        f"Data Quality Metrics:\n"
        f" - Records after cleansing: {len(cleaned_data)}\n"
    )
    email.send(to="supply-chain-team@company.com", subject=subject, body=body)
    context.log.info("Summary email sent.")


@job(
    resource_defs={
        "db": DatabaseResource(),
        "email": EmailResource(),
    },
    description="Three-stage ETL pipeline for supply chain shipment data.",
)
def shipment_etl_job():
    # Extraction stage (parallel)
    a = ingest_vendor_a()
    b = ingest_vendor_b()
    c = ingest_vendor_c()

    # Transformation stage (fan‑in)
    cleaned = cleanse_data(vendor_a=a, vendor_b=b, vendor_c=c)

    # Load stage
    rows_loaded = load_to_db(cleaned_data=cleaned)

    # Notification stage
    send_summary_email(row_count=rows_loaded, cleaned_data=cleaned)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=shipment_etl_job,
    execution_timezone="UTC",
    description="Daily execution of the shipment ETL pipeline.",
)
def daily_shipment_etl_schedule():
    return {}


defs = Definitions(
    jobs=[shipment_etl_job],
    schedules=[daily_shipment_etl_schedule],
    resources={
        "db": DatabaseResource(),
        "email": EmailResource(),
    },
)


if __name__ == "__main__":
    result = shipment_etl_job.execute_in_process(
        run_config={
            "ops": {
                "ingest_vendor_a": {"config": {"csv_path": "data/vendor_a.csv"}},
                "ingest_vendor_b": {"config": {"csv_path": "data/vendor_b.csv"}},
                "ingest_vendor_c": {"config": {"csv_path": "data/vendor_c.csv"}},
            }
        }
    )
    assert result.success