from datetime import timedelta
import pandas as pd
from dagster import (
    op,
    job,
    In,
    Out,
    RetryPolicy,
    ConfigurableResource,
    ResourceDefinition,
    ScheduleDefinition,
    Definitions,
    get_dagster_logger,
)


class PostgresResource(ConfigurableResource):
    """Minimal stub for a PostgreSQL resource."""

    connection_string: str = "postgresql://user:password@localhost:5432/inventory"

    def upsert(self, table: str, df: pd.DataFrame) -> None:
        logger = get_dagster_logger()
        logger.info(f"Upserting {len(df)} records into table '{table}' using {self.connection_string}")
        # Real implementation would use an ORM or psycopg2 to upsert rows.
        # Here we simply log the action.


class EmailResource(ConfigurableResource):
    """Minimal stub for an email sending resource."""

    smtp_server: str = "smtp.example.com"
    smtp_port: int = 587
    username: str = "no-reply@example.com"
    password: str = "example-password"
    from_address: str = "no-reply@example.com"

    def send(self, to: str, subject: str, body: str) -> None:
        logger = get_dagster_logger()
        logger.info(
            f"Sending email to {to} via {self.smtp_server}:{self.smtp_port} - Subject: {subject}"
        )
        logger.debug(f"Email body:\n{body}")
        # Real implementation would use smtplib or an email service SDK.
        # Here we simply log the action.


@op(
    config_schema={"path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Ingest raw shipment CSV data from Vendor A.",
)
def ingest_vendor_a(context) -> pd.DataFrame:
    path = context.op_config["path"]
    logger = context.log
    logger.info(f"Ingesting Vendor A data from {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to read Vendor A CSV: {e}")
        raise
    logger.info(f"Vendor A records ingested: {len(df)}")
    return df


@op(
    config_schema={"path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Ingest raw shipment CSV data from Vendor B.",
)
def ingest_vendor_b(context) -> pd.DataFrame:
    path = context.op_config["path"]
    logger = context.log
    logger.info(f"Ingesting Vendor B data from {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to read Vendor B CSV: {e}")
        raise
    logger.info(f"Vendor B records ingested: {len(df)}")
    return df


@op(
    config_schema={"path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Ingest raw shipment CSV data from Vendor C.",
)
def ingest_vendor_c(context) -> pd.DataFrame:
    path = context.op_config["path"]
    logger = context.log
    logger.info(f"Ingesting Vendor C data from {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to read Vendor C CSV: {e}")
        raise
    logger.info(f"Vendor C records ingested: {len(df)}")
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
    logger = context.log
    logger.info("Starting data cleansing and normalization")
    # Concatenate all vendor data
    combined = pd.concat([vendor_a, vendor_b, vendor_c], ignore_index=True)
    logger.info(f"Combined record count before cleansing: {len(combined)}")

    # Example cleansing steps (placeholders for real logic)
    # 1. Normalize SKU format (uppercase, strip whitespace)
    if "sku" in combined.columns:
        combined["sku"] = combined["sku"].astype(str).str.upper().str.strip()
    # 2. Validate shipment dates
    if "shipment_date" in combined.columns:
        combined["shipment_date"] = pd.to_datetime(combined["shipment_date"], errors="coerce")
        valid_dates = combined["shipment_date"].notna()
        combined = combined[valid_dates]
    # 3. Enrich with location info (dummy enrichment)
    if "location_code" in combined.columns:
        combined["location_name"] = "Location_" + combined["location_code"].astype(str)

    logger.info(f"Record count after cleansing: {len(combined)}")
    return combined


@op(
    required_resource_keys={"postgres"},
    out=Out(pd.DataFrame),
    description="Loads cleansed data into the PostgreSQL inventory database.",
)
def load_to_db(context, cleaned_data: pd.DataFrame) -> pd.DataFrame:
    logger = context.log
    logger.info("Loading cleansed data into PostgreSQL")
    postgres: PostgresResource = context.resources.postgres
    postgres.upsert(table="inventory_shipments", df=cleaned_data)
    logger.info("Data load complete")
    return cleaned_data


@op(
    required_resource_keys={"email"},
    description="Sends a summary email after successful load.",
)
def send_summary_email(context, loaded_data: pd.DataFrame) -> None:
    logger = context.log
    email: EmailResource = context.resources.email
    record_count = len(loaded_data)
    subject = "Daily Shipment ETL Summary"
    body = (
        f"ETL Process Completed Successfully.\n"
        f"Date: {pd.Timestamp.now().date()}\n"
        f"Total Records Processed: {record_count}\n"
        f"Data Quality Metrics: All records passed validation.\n"
    )
    email.send(
        to="supply-chain-team@company.com",
        subject=subject,
        body=body,
    )
    logger.info("Summary email sent")


@job(
    resource_defs={
        "postgres": ResourceDefinition.hardcoded_resource(PostgresResource()),
        "email": ResourceDefinition.hardcoded_resource(EmailResource()),
    },
    description="Three‑stage ETL pipeline for supply‑chain shipment data.",
)
def supply_chain_etl():
    # Stage 1: Parallel extraction
    vendor_a = ingest_vendor_a()
    vendor_b = ingest_vendor_b()
    vendor_c = ingest_vendor_c()

    # Stage 2: Transformation (fan‑in)
    cleaned = cleanse_data(vendor_a, vendor_b, vendor_c)

    # Stage 3: Load and notification (sequential)
    loaded = load_to_db(cleaned)
    send_summary_email(loaded)


daily_schedule = ScheduleDefinition(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=supply_chain_etl,
    execution_timezone="UTC",
    description="Daily execution of the supply chain ETL pipeline.",
)


defs = Definitions(
    jobs=[supply_chain_etl],
    schedules=[daily_schedule],
    resources={
        "postgres": PostgresResource(),
        "email": EmailResource(),
    },
)


if __name__ == "__main__":
    result = supply_chain_etl.execute_in_process(
        run_config={
            "ops": {
                "ingest_vendor_a": {"config": {"path": "data/vendor_a.csv"}},
                "ingest_vendor_b": {"config": {"path": "data/vendor_b.csv"}},
                "ingest_vendor_c": {"config": {"path": "data/vendor_c.csv"}},
            }
        }
    )
    assert result.success