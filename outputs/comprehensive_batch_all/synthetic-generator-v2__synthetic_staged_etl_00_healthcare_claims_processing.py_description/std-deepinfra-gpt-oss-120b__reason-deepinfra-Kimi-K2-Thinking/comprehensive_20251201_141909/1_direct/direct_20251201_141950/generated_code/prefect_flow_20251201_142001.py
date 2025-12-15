from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@task(retries=2, retry_delay_seconds=300)
def extract_claims(csv_path: str = "data/claims.csv") -> pd.DataFrame:
    """Extract patient claims data from a CSV file."""
    path = Path(csv_path)
    if not path.is_file():
        logger.warning("Claims CSV not found at %s. Returning empty DataFrame.", csv_path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    logger.info("Extracted %d claim records from %s.", len(df), csv_path)
    return df


@task(retries=2, retry_delay_seconds=300)
def extract_providers(csv_path: str = "data/providers.csv") -> pd.DataFrame:
    """Extract provider data from a CSV file."""
    path = Path(csv_path)
    if not path.is_file():
        logger.warning("Providers CSV not found at %s. Returning empty DataFrame.", csv_path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    logger.info("Extracted %d provider records from %s.", len(df), csv_path)
    return df


@task
def _hash_identifier(identifier: str) -> str:
    """Hash a string identifier using SHA-256."""
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


@task
def transform_join(
    claims: pd.DataFrame, providers: pd.DataFrame
) -> pd.DataFrame:
    """
    Join claims with provider data, anonymize patient identifiers,
    and calculate a simple risk score.
    """
    if claims.empty or providers.empty:
        logger.warning("One or both input DataFrames are empty. Returning empty DataFrame.")
        return pd.DataFrame()

    # Assume common key is 'provider_id'
    merged = claims.merge(providers, on="provider_id", how="left", suffixes=("_claim", "_prov"))
    logger.info("Merged claims with providers: %d rows.", len(merged))

    # Anonymize patient identifiers
    if "patient_id" in merged.columns:
        merged["patient_id_hashed"] = merged["patient_id"].apply(_hash_identifier.run)
        merged.drop(columns=["patient_id"], inplace=True)
        logger.info("Anonymized patient identifiers.")
    else:
        logger.warning("Column 'patient_id' not found; skipping anonymization.")

    # Simple risk score calculation
    def _risk_score(row: pd.Series) -> float:
        amount = row.get("claim_amount", 0)
        proc_code = str(row.get("procedure_code", ""))
        return float(amount) * 0.01 + len(proc_code) * 0.5

    merged["risk_score"] = merged.apply(_risk_score, axis=1)
    logger.info("Calculated risk scores.")

    return merged


@task(retries=2, retry_delay_seconds=300)
def load_warehouse(
    df: pd.DataFrame,
    conn_str: str = "postgresql://user:password@localhost:5432/analytics",
    table_name: str = "claims_fact",
) -> None:
    """Load the transformed DataFrame into a Postgres analytics warehouse."""
    if df.empty:
        logger.warning("Received empty DataFrame; nothing to load.")
        return

    engine = create_engine(conn_str)
    with engine.begin() as conn:
        df.to_sql(name=table_name, con=conn, if_exists="replace", index=False)
    logger.info("Loaded %d rows into %s.", len(df), table_name)


@task
def refresh_bi(df: pd.DataFrame) -> None:
    """
    Trigger a refresh of BI dashboards.
    In a real implementation this could call Power BI / Tableau APIs.
    """
    if df.empty:
        logger.warning("Empty DataFrame; BI refresh may be unnecessary.")
    logger.info("BI dashboards refreshed with latest data (row count: %d).", len(df))


@flow
def healthcare_claims_etl_flow() -> None:
    """
    Orchestrates the healthcare claims ETL pipeline.
    Execution pattern:
        - Parallel extraction of claims and providers.
        - Transform joins the extracted data.
        - Parallel loading to warehouse and BI refresh.
    """
    # Parallel extraction
    claims_future = extract_claims.submit()
    providers_future = extract_providers.submit()

    # Wait for extraction to complete and retrieve results
    claims_df = claims_future.result()
    providers_df = providers_future.result()

    # Transform step (depends on both extracts)
    transformed_df = transform_join(claims_df, providers_df)

    # Parallel load and BI refresh
    load_future = load_warehouse.submit(transformed_df)
    refresh_future = refresh_bi.submit(transformed_df)

    # Ensure both downstream tasks complete
    load_future.result()
    refresh_future.result()


# Schedule: This flow is intended to run daily starting 2024-01-01.
# Deployment configuration (e.g., via Prefect UI or CLI) should set the appropriate cron schedule.

if __name__ == "__main__":
    healthcare_claims_etl_flow()