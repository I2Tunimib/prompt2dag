from __future__ import annotations

import hashlib
import os
from typing import Any, Dict

import pandas as pd
from dagster import (
    ConfigurableResource,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    JobDefinition,
    ResourceDefinition,
    op,
    job,
)


class PostgresResource(ConfigurableResource):
    """A minimal stub for a Postgres connection resource."""

    host: str = "localhost"
    port: int = 5432
    database: str = "analytics"
    user: str = "user"
    password: str = "password"

    def execute_query(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        """Placeholder method to simulate query execution."""
        print(f"[Postgres] Executing query: {query}")
        if params:
            print(f"[Postgres] With params: {params}")


postgres_resource = ResourceDefinition.resource(
    lambda: PostgresResource()
)


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Extract patient claims data from a CSV file.",
)
def extract_claims(context) -> pd.DataFrame:
    file_path = context.op_config["file_path"]
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Claims file not found: {file_path}")
    df = pd.read_csv(file_path)
    context.log.info(f"Extracted {len(df)} claim records from {file_path}")
    return df


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Extract provider data from a CSV file.",
)
def extract_providers(context) -> pd.DataFrame:
    file_path = context.op_config["file_path"]
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Providers file not found: {file_path}")
    df = pd.read_csv(file_path)
    context.log.info(f"Extracted {len(df)} provider records from {file_path}")
    return df


def _hash_identifier(identifier: Any) -> str:
    """Hash a patient identifier using SHA-256."""
    return hashlib.sha256(str(identifier).encode("utf-8")).hexdigest()


def _calculate_risk_score(row: pd.Series) -> float:
    """Simple risk scoring based on procedure code and claim amount."""
    amount = float(row.get("claim_amount", 0))
    code = str(row.get("procedure_code", ""))
    code_factor = len(code) * 0.1
    return amount * 0.01 + code_factor


@op(
    ins={"claims": In(pd.DataFrame), "providers": In(pd.DataFrame)},
    out=Out(pd.DataFrame),
    description=(
        "Join claims and provider data, anonymize patient identifiers, "
        "and calculate risk scores."
    ),
)
def transform_join(context, claims: pd.DataFrame, providers: pd.DataFrame) -> pd.DataFrame:
    # Join on provider_id
    merged = claims.merge(providers, on="provider_id", how="left", suffixes=("_claim", "_provider"))
    context.log.info(f"Merged dataset has {len(merged)} rows.")

    # Anonymize patient identifiers
    if "patient_id" in merged.columns:
        merged["patient_hash"] = merged["patient_id"].apply(_hash_identifier)
        merged = merged.drop(columns=["patient_id"])
        context.log.info("Patient identifiers anonymized.")

    # Calculate risk scores
    merged["risk_score"] = merged.apply(_calculate_risk_score, axis=1)
    context.log.info("Risk scores calculated.")

    return merged


@op(
    required_resource_keys={"postgres"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Load transformed data into the analytics warehouse.",
)
def load_warehouse(context, transformed: pd.DataFrame) -> None:
    postgres: PostgresResource = context.resources.postgres
    # Example: Insert into claims_fact table (placeholder logic)
    insert_query = "INSERT INTO claims_fact (columns) VALUES (%s, %s, %s)"  # Simplified
    for _, row in transformed.iterrows():
        # In a real implementation, map row to columns and execute query
        postgres.execute_query(insert_query, params=tuple(row))
    context.log.info(f"Loaded {len(transformed)} rows into the warehouse.")


@op(
    description="Trigger refresh of BI dashboards (Power BI, Tableau).",
)
def refresh_bi(context, transformed: pd.DataFrame) -> None:
    # Placeholder for BI refresh logic
    context.log.info("Refreshing Power BI dashboards...")
    context.log.info("Refreshing Tableau dashboards...")
    context.log.info("BI dashboards refreshed.")


@job(
    description="Healthcare claims processing ETL with parallel extraction and loading stages.",
)
def etl_job():
    claims_df = extract_claims()
    providers_df = extract_providers()
    transformed_df = transform_join(claims=claims_df, providers=providers_df)
    load_warehouse(transformed=transformed_df)
    refresh_bi(transformed=transformed_df)


daily_etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    name="daily_etl_schedule",
    execution_timezone="UTC",
)


defs = Definitions(
    jobs=[etl_job],
    schedules=[daily_etl_schedule],
    resources={"postgres": postgres_resource},
)


if __name__ == "__main__":
    # Example execution with inline config for extraction file paths.
    result = etl_job.execute_in_process(
        run_config={
            "ops": {
                "extract_claims": {"config": {"file_path": "data/claims.csv"}},
                "extract_providers": {"config": {"file_path": "data/providers.csv"}},
            },
            "resources": {
                "postgres": {
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "analytics",
                        "user": "user",
                        "password": "password",
                    }
                }
            },
        }
    )
    assert result.success