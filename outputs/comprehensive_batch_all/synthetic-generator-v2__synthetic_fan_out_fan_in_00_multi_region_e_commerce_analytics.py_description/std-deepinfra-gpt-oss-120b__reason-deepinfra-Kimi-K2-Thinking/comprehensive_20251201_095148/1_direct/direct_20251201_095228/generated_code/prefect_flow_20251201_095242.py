import logging
from pathlib import Path
from typing import List

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.states import Failed

# ----------------------------------------------------------------------
# Helper for failure notifications (placeholder for email integration)
# ----------------------------------------------------------------------


def notify_failure(task, state, **kwargs):
    """Log failure and placeholder for email notification."""
    logger = get_run_logger()
    logger.error("Task %s failed with state %s", task.name, state)
    # TODO: integrate email notification here (e.g., using SMTP or an alerting service)
    return state


# ----------------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------------


@task
def start_pipeline() -> None:
    """Mark the start of the pipeline."""
    logger = get_run_logger()
    logger.info("Pipeline started.")


@task(retries=2, retry_delay_seconds=300, on_failure=[notify_failure])
def ingest_region(region: str, source_path: Path) -> pd.DataFrame:
    """
    Ingest sales data for a given region from a CSV file.

    Args:
        region: Name of the region (used for logging).
        source_path: Path to the CSV file.

    Returns:
        DataFrame containing the ingested data.
    """
    logger = get_run_logger()
    logger.info("Ingesting %s sales data from %s", region, source_path)

    if not source_path.is_file():
        logger.warning("Source file %s does not exist. Returning empty DataFrame.", source_path)
        return pd.DataFrame()

    df = pd.read_csv(source_path)
    df["region"] = region
    return df


@task(retries=2, retry_delay_seconds=300, on_failure=[notify_failure])
def convert_us_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass‑through conversion for US regions (already in USD).

    Args:
        df: DataFrame with US sales data.

    Returns:
        Unmodified DataFrame.
    """
    logger = get_run_logger()
    logger.info("US region data already in USD; no conversion applied.")
    return df.copy()


@task(retries=2, retry_delay_seconds=300, on_failure=[notify_failure])
def convert_eu_data(df: pd.DataFrame, rate: float = 1.10) -> pd.DataFrame:
    """
    Convert EU sales from EUR to USD.

    Args:
        df: DataFrame with EU sales data.
        rate: EUR to USD exchange rate (default 1.10).

    Returns:
        DataFrame with sales converted to USD.
    """
    logger = get_run_logger()
    logger.info("Converting EU sales from EUR to USD using rate %.4f", rate)

    if "sales" in df.columns:
        df = df.copy()
        df["sales"] = df["sales"] * rate
    else:
        logger.warning("Column 'sales' not found in EU data; conversion skipped.")
    return df


@task(retries=2, retry_delay_seconds=300, on_failure=[notify_failure])
def convert_apac_data(df: pd.DataFrame, rate: float = 0.0090) -> pd.DataFrame:
    """
    Convert APAC sales from JPY to USD.

    Args:
        df: DataFrame with APAC sales data.
        rate: JPY to USD exchange rate (default 0.0090).

    Returns:
        DataFrame with sales converted to USD.
    """
    logger = get_run_logger()
    logger.info("Converting APAC sales from JPY to USD using rate %.4f", rate)

    if "sales" in df.columns:
        df = df.copy()
        df["sales"] = df["sales"] * rate
    else:
        logger.warning("Column 'sales' not found in APAC data; conversion skipped.")
    return df


@task(retries=2, retry_delay_seconds=300, on_failure=[notify_failure])
def aggregate_data(regional_dfs: List[pd.DataFrame], output_path: Path) -> Path:
    """
    Aggregate regional sales data into a global revenue report.

    Args:
        regional_dfs: List of DataFrames from each region (already in USD).
        output_path: Destination path for the CSV report.

    Returns:
        Path to the generated report.
    """
    logger = get_run_logger()
    logger.info("Aggregating data from %d regions.", len(regional_dfs))

    combined_df = pd.concat(regional_dfs, ignore_index=True)
    total_revenue = combined_df["sales"].sum()
    logger.info("Global total revenue (USD): %.2f", total_revenue)

    # Add a summary row
    summary = pd.DataFrame(
        {
            "region": ["GLOBAL_TOTAL"],
            "sales": [total_revenue],
        }
    )
    report_df = pd.concat([combined_df, summary], ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(output_path, index=False)
    logger.info("Global revenue report written to %s", output_path)

    return output_path


@task
def end_pipeline() -> None:
    """Mark the end of the pipeline."""
    logger = get_run_logger()
    logger.info("Pipeline completed.")


# ----------------------------------------------------------------------
# Flow orchestration
# ----------------------------------------------------------------------


@flow
def ecommerce_analytics_flow():
    """
    Prefect flow that processes daily sales data from four regions,
    converts currencies to USD, and generates a global revenue report.
    """
    logger = get_run_logger()
    logger.info("Starting ecommerce analytics flow.")

    # Start marker
    start_pipeline()

    # Define source file locations (adjust paths as needed)
    base_dir = Path("data")
    sources = {
        "US_EAST": base_dir / "us_east_sales.csv",
        "US_WEST": base_dir / "us_west_sales.csv",
        "EU": base_dir / "eu_sales.csv",
        "APAC": base_dir / "apac_sales.csv",
    }

    # ------------------------------------------------------------------
    # Fan‑out phase 1: Ingestion (parallel)
    # ------------------------------------------------------------------
    ingest_futures = {
        region: ingest_region.submit(region, path) for region, path in sources.items()
    }

    # Wait for ingestion to finish and collect results
    ingested = {region: future.result() for region, future in ingest_futures.items()}

    # ------------------------------------------------------------------
    # Fan‑out phase 2: Currency conversion (parallel)
    # ------------------------------------------------------------------
    conversion_futures = {
        "US_EAST": convert_us_data.submit(ingested["US_EAST"]),
        "US_WEST": convert_us_data.submit(ingested["US_WEST"]),
        "EU": convert_eu_data.submit(ingested["EU"]),
        "APAC": convert_apac_data.submit(ingested["APAC"]),
    }

    # Gather converted DataFrames
    converted = [future.result() for future in conversion_futures.values()]

    # ------------------------------------------------------------------
    # Fan‑in phase: Aggregation
    # ------------------------------------------------------------------
    report_path = Path("output") / "global_revenue_report.csv"
    aggregate_data.submit(converted, report_path)

    # End marker
    end_pipeline()

    logger.info("Ecommerce analytics flow finished.")


# ----------------------------------------------------------------------
# Execution guard
# ----------------------------------------------------------------------


if __name__ == "__main__":
    # Note: In production, configure a Prefect deployment with a daily schedule
    # and disable catch‑up for missed runs.
    ecommerce_analytics_flow()