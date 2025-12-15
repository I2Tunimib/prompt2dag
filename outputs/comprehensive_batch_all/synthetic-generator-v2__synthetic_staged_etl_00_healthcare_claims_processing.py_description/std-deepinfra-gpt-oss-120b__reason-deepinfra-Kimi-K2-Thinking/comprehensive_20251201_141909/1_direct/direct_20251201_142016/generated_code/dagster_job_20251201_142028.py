from datetime import datetime, timedelta
import hashlib
import pandas as pd
from dagster import (
    Config,
    InitResourceContext,
    In,
    Out,
    RetryPolicy,
    ResourceDefinition,
    ScheduleDefinition,
    job,
    op,
    get_dagster_logger,
)


class CSVExtractConfig(Config):
    file_path: str = "data.csv"


class WarehouseResource:
    """A minimal stub for a Postgres analytics warehouse."""

    def __init__(self, connection_string: str = "postgresql://user:pass@localhost/db"):
        self.connection_string = connection_string

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        logger = get_dagster_logger()
        logger.info(f"Loading {len(df)} rows into table '{table_name}' using {self.connection_string}")
        # In a real implementation, use SQLAlchemy or psycopg2 to insert the data.
        # Here we simply log the action as a placeholder.


def warehouse_resource(init_context: InitResourceContext) -> WarehouseResource:
    return WarehouseResource()


@op(
    config_schema=CSVExtractConfig,
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Extract patient claims data from a CSV file.",
)
def extract_claims(context) -> pd.DataFrame:
    file_path = context.op_config["file_path"]
    logger = context.log
    logger.info(f"Reading claims CSV from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Extracted {len(df)} claim records")
    return df


@op(
    config_schema=CSVExtractConfig,
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Extract provider data from a CSV file.",
)
def extract_providers(context) -> pd.DataFrame:
    file_path = context.op_config["file_path"]
    logger = context.log
    logger.info(f"Reading providers CSV from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Extracted {len(df)} provider records")
    return df


def _hash_identifier(identifier: str) -> str:
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def _calculate_risk_score(row: pd.Series) -> float:
    # Simplified risk scoring: higher amount and certain procedure codes increase risk.
    base_score = row["claim_amount"] / 1000.0
    high_risk_codes = {"PROC123", "PROC999"}
    if row["procedure_code"] in high_risk_codes:
        base_score *= 1.5
    return min(base_score, 10.0)


@op(
    ins={"claims": In(pd.DataFrame), "providers": In(pd.DataFrame)},
    out=Out(pd.DataFrame),
    description="Join claims and provider data, anonymize PII, and calculate risk scores.",
)
def transform_join(context, claims: pd.DataFrame, providers: pd.DataFrame) -> pd.DataFrame:
    logger = context.log
    logger.info("Joining claims with provider data")
    merged = claims.merge(providers, how="left", on="provider_id", suffixes=("_claim", "_provider"))
    logger.info(f"Merged dataset has {len(merged)} rows")

    logger.info("Anonymizing patient identifiers")
    merged["patient_id_hashed"] = merged["patient_id"].apply(_hash_identifier)
    merged = merged.drop(columns=["patient_id"])

    logger.info("Calculating risk scores")
    merged["risk_score"] = merged.apply(_calculate_risk_score, axis=1)

    # Reorder columns for clarity
    columns_order = [
        "patient_id_hashed",
        "provider_id",
        "provider_name",
        "claim_id",
        "procedure_code",
        "claim_amount",
        "risk_score",
    ]
    result = merged[columns_order]
    logger.info("Transformation complete")
    return result


@op(
    required_resource_keys={"warehouse"},
    description="Load transformed data into the analytics warehouse.",
)
def load_warehouse(context, transformed: pd.DataFrame) -> None:
    logger = context.log
    logger.info("Loading transformed data into warehouse")
    warehouse: WarehouseResource = context.resources.warehouse
    warehouse.load_dataframe(transformed, table_name="claims_fact")
    logger.info("Warehouse load completed")


@op(
    description="Trigger refresh of BI dashboards (Power BI, Tableau).",
)
def refresh_bi(context) -> None:
    logger = context.log
    logger.info("Refreshing Power BI dashboards")
    # Placeholder for Power BI API call
    logger.info("Power BI refresh triggered")

    logger.info("Refreshing Tableau dashboards")
    # Placeholder for Tableau API call
    logger.info("Tableau refresh triggered")


@job(
    resource_defs={"warehouse": ResourceDefinition(resource_fn=warehouse_resource)},
    description="Healthcare claims ETL pipeline with parallel extraction and loading.",
)
def claims_etl_job():
    claims_df = extract_claims()
    providers_df = extract_providers()
    transformed_df = transform_join(claims=claims_df, providers=providers_df)
    load_warehouse(transformed=transformed_df)
    refresh_bi()


daily_schedule = ScheduleDefinition(
    job=claims_etl_job,
    cron_schedule="0 2 * * *",  # Runs daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    execution_timezone="UTC",
    description="Daily execution of the claims ETL pipeline.",
)


if __name__ == "__main__":
    result = claims_etl_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully")
    else:
        print("Pipeline execution failed")