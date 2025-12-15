import os
import hashlib
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger

# ----------------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=300)
def extract_claims(csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    Extract patient claims data from a CSV file.

    Args:
        csv_path: Path to the claims CSV file. If None, reads from the
            CLAIMS_CSV_PATH environment variable or defaults to
            ``data/claims.csv``.

    Returns:
        DataFrame containing claims records.
    """
    logger = get_run_logger()
    path = csv_path or os.getenv("CLAIMS_CSV_PATH", "data/claims.csv")
    logger.info(f"Reading claims data from %s", path)
    return pd.read_csv(path)


@task(retries=2, retry_delay_seconds=300)
def extract_providers(csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    Extract provider data from a CSV file.

    Args:
        csv_path: Path to the providers CSV file. If None, reads from the
            PROVIDERS_CSV_PATH environment variable or defaults to
            ``data/providers.csv``.

    Returns:
        DataFrame containing provider records.
    """
    logger = get_run_logger()
    path = csv_path or os.getenv("PROVIDERS_CSV_PATH", "data/providers.csv")
    logger.info(f"Reading provider data from %s", path)
    return pd.read_csv(path)


def _hash_identifier(identifier: str) -> str:
    """
    Hash a string identifier using SHA‑256.

    Args:
        identifier: The identifier to hash.

    Returns:
        Hexadecimal representation of the hash.
    """
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


@task(retries=2, retry_delay_seconds=300)
def transform_join(
    claims_df: pd.DataFrame, providers_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Join claims with provider data, anonymize patient identifiers, and
    calculate a simple risk score.

    Args:
        claims_df: DataFrame with claims.
        providers_df: DataFrame with providers.

    Returns:
        Transformed DataFrame ready for loading.
    """
    logger = get_run_logger()
    logger.info("Joining claims with provider data")
    merged = claims_df.merge(
        providers_df, how="left", on="provider_id", suffixes=("", "_prov")
    )

    logger.info("Anonymizing patient identifiers")
    merged["patient_hash"] = merged["patient_id"].apply(_hash_identifier)
    merged.drop(columns=["patient_id"], inplace=True)

    logger.info("Calculating risk scores")
    # Simple risk score: amount * 0.01 + length of procedure_code * 0.5
    merged["risk_score"] = (
        merged["claim_amount"] * 0.01
        + merged["procedure_code"].astype(str).str.len() * 0.5
    )

    # Reorder columns for clarity
    columns_order = [
        "claim_id",
        "patient_hash",
        "provider_id",
        "provider_name",
        "procedure_code",
        "claim_amount",
        "risk_score",
    ]
    transformed = merged[columns_order]
    logger.info("Transformation complete")
    return transformed


@task(retries=2, retry_delay_seconds=300)
def load_warehouse(transformed_df: pd.DataFrame) -> None:
    """
    Load the transformed data into a PostgreSQL analytics warehouse.

    Args:
        transformed_df: DataFrame produced by ``transform_join``.
    """
    logger = get_run_logger()
    conn_str = os.getenv(
        "POSTGRES_CONN",
        "postgresql://user:password@localhost:5432/analytics",
    )
    engine = create_engine(conn_str)
    logger.info("Loading data into the analytics warehouse")
    transformed_df.to_sql(
        name="claims_fact",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info("Data load complete")


@task(retries=2, retry_delay_seconds=300)
def refresh_bi(transformed_df: pd.DataFrame) -> None:
    """
    Trigger a refresh of BI dashboards (Power BI, Tableau).

    Args:
        transformed_df: DataFrame produced by ``transform_join``.
        (The argument is kept to illustrate dependency; the function
        currently only logs the refresh action.)
    """
    logger = get_run_logger()
    logger.info("Refreshing Power BI and Tableau dashboards")
    # Placeholder for actual refresh logic (e.g., API calls)
    logger.info("BI refresh triggered")


# ----------------------------------------------------------------------
# Flow
# ----------------------------------------------------------------------


@flow(name="healthcare_claims_etl")
def healthcare_claims_etl() -> None:
    """
    Orchestrates the ETL pipeline:
    1. Parallel extraction of claims and provider data.
    2. Join, anonymize, and calculate risk scores.
    3. Parallel loading to the warehouse and BI refresh.
    """
    logger = get_run_logger()
    logger.info("Starting healthcare claims ETL pipeline")

    # Parallel extraction
    claims_future = extract_claims.submit()
    providers_future = extract_providers.submit()

    claims_df = claims_future.result()
    providers_df = providers_future.result()

    # Transformation (depends on both extracts)
    transformed_df = transform_join(claims_df, providers_df)

    # Parallel loading and BI refresh
    load_future = load_warehouse.submit(transformed_df)
    refresh_future = refresh_bi.submit(transformed_df)

    # Ensure both downstream tasks complete
    load_future.result()
    refresh_future.result()

    logger.info("Healthcare claims ETL pipeline completed successfully")


# ----------------------------------------------------------------------
# Execution guard
# ----------------------------------------------------------------------


if __name__ == "__main__":
    # Note: In production, this flow would be deployed with a daily schedule
    # starting on 2024-01-01. The schedule configuration is handled via
    # Prefect deployments, not within the code.
    healthcare_claims_etl()