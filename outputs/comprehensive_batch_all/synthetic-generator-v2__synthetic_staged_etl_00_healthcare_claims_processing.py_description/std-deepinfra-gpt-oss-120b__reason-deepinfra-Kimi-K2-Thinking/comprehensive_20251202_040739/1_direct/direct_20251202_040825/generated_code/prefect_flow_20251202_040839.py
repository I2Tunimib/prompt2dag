import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import flow, task

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default file locations (adjust as needed)
CLAIMS_CSV_PATH = Path("data/claims.csv")
PROVIDERS_CSV_PATH = Path("data/providers.csv")

# PostgreSQL connection placeholder (replace with real credentials)
POSTGRES_CONNECTION_STRING = "postgresql://user:password@localhost:5432/analytics"


@task(retries=2, retry_delay_seconds=300)
def extract_claims() -> pd.DataFrame:
    """Extract patient claims data from a CSV file."""
    logger.info("Extracting claims data from %s", CLAIMS_CSV_PATH)
    if not CLAIMS_CSV_PATH.is_file():
        raise FileNotFoundError(f"Claims CSV not found at {CLAIMS_CSV_PATH}")
    df = pd.read_csv(CLAIMS_CSV_PATH)
    logger.info("Extracted %d claim records", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def extract_providers() -> pd.DataFrame:
    """Extract provider data from a CSV file."""
    logger.info("Extracting providers data from %s", PROVIDERS_CSV_PATH)
    if not PROVIDERS_CSV_PATH.is_file():
        raise FileNotFoundError(f"Providers CSV not found at {PROVIDERS_CSV_PATH}")
    df = pd.read_csv(PROVIDERS_CSV_PATH)
    logger.info("Extracted %d provider records", len(df))
    return df


def _hash_identifier(identifier: Any) -> str:
    """Hash a patient identifier using SHA-256."""
    return hashlib.sha256(str(identifier).encode("utf-8")).hexdigest()


def _calculate_risk_score(row: pd.Series) -> float:
    """Simple risk scoring based on procedure code and claim amount."""
    amount = row.get("claim_amount", 0.0)
    proc_code = str(row.get("procedure_code", ""))
    if proc_code.startswith("A"):
        return amount * 0.01
    return amount * 0.005


@task(retries=2, retry_delay_seconds=300)
def transform_join(
    claims: pd.DataFrame, providers: pd.DataFrame
) -> pd.DataFrame:
    """Join claims with provider data, anonymize PII, and calculate risk scores."""
    logger.info("Joining claims (%d) with providers (%d)", len(claims), len(providers))

    # Assume both datasets have a 'provider_id' column for joining
    merged = claims.merge(providers, on="provider_id", how="left", suffixes=("_claim", "_prov"))

    # Anonymize patient identifiers
    if "patient_id" in merged.columns:
        merged["patient_hash"] = merged["patient_id"].apply(_hash_identifier)
        merged.drop(columns=["patient_id"], inplace=True)

    # Calculate risk scores
    merged["risk_score"] = merged.apply(_calculate_risk_score, axis=1)

    logger.info("Transformation complete with %d records", len(merged))
    return merged


@task(retries=2, retry_delay_seconds=300)
def load_warehouse(transformed: pd.DataFrame) -> None:
    """Load transformed data into the healthcare analytics warehouse."""
    logger.info("Loading %d records into the analytics warehouse", len(transformed))

    try:
        from sqlalchemy import create_engine

        engine = create_engine(POSTGRES_CONNECTION_STRING)
        transformed.to_sql(
            name="claims_fact",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.info("Data successfully loaded into claims_fact table")
    except Exception as exc:
        logger.error("Failed to load data into warehouse: %s", exc)
        raise


@task(retries=2, retry_delay_seconds=300)
def refresh_bi(transformed: pd.DataFrame) -> None:
    """Trigger refresh of BI dashboards (Power BI, Tableau)."""
    logger.info("Refreshing BI dashboards with latest data")
    # Placeholder for actual BI refresh logic.
    # For Power BI, you might call the REST API; for Tableau, use the Server Client.
    # Here we simply log the action.
    logger.info("BI dashboards refreshed at %s", datetime.utcnow().isoformat())


@flow
def healthcare_claims_etl_flow() -> None:
    """Orchestrate the healthcare claims ETL pipeline with parallel extraction and loading."""
    # Parallel extraction
    claims_future = extract_claims.submit()
    providers_future = extract_providers.submit()

    # Wait for extraction to complete and retrieve results
    claims_df = claims_future.result()
    providers_df = providers_future.result()

    # Transform step (depends on both extracts)
    transformed_df = transform_join(claims_df, providers_df)

    # Parallel loading
    load_warehouse.submit(transformed_df)
    refresh_bi.submit(transformed_df)


if __name__ == "__main__":
    # Note: In production, configure a Prefect deployment with a daily schedule
    # starting on 2024-01-01. The schedule can be set via the Prefect UI or CLI.
    healthcare_claims_etl_flow()