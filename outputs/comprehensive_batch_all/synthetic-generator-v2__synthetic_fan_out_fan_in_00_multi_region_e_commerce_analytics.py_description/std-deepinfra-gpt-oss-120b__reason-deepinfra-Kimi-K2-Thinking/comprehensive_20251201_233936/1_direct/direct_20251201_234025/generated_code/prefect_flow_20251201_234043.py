import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.states import Failed

# Configure module-level logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def send_failure_email(task_name: str, error: Exception) -> None:
    """
    Placeholder for sending an email notification on task failure.
    Replace with actual email sending logic as needed.
    """
    logger.error("Task %s failed with error: %s", task_name, error)
    # Example: send_email(to="alerts@example.com", subject=f"Task {task_name} failed", body=str(error))


def _failure_handler(task_name: str):
    """
    Returns a callable that can be used as a task failure hook.
    """

    def handler(state):
        if isinstance(state, Failed):
            exc = state.result()
            send_failure_email(task_name, exc)

    return handler


@task(name="Start Pipeline", retries=0)
def start_pipeline() -> None:
    """Marks the start of the pipeline."""
    log = get_run_logger()
    log.info("Pipeline started.")


@task(name="End Pipeline", retries=0)
def end_pipeline() -> None:
    """Marks the end of the pipeline."""
    log = get_run_logger()
    log.info("Pipeline completed.")


@task(
    name="Ingest Region Data",
    retries=3,
    retry_delay_seconds=300,
    on_failure=_failure_handler("Ingest Region Data"),
)
def ingest_region(region: str, file_path: Path) -> pd.DataFrame:
    """
    Reads regional sales data from a CSV file.

    Args:
        region: Name of the region (used for logging).
        file_path: Path to the CSV file.

    Returns:
        DataFrame containing the sales data.
    """
    log = get_run_logger()
    log.info("Ingesting data for %s from %s", region, file_path)

    if not file_path.is_file():
        raise FileNotFoundError(f"CSV file for {region} not found at {file_path}")

    df = pd.read_csv(file_path)
    if "revenue" not in df.columns:
        raise ValueError(f"'revenue' column missing in {region} data.")
    return df


def get_exchange_rate(currency: str) -> float:
    """
    Retrieves a daily exchange rate for the given currency to USD.
    This is a stub implementation; replace with real API calls as needed.

    Args:
        currency: Currency code (e.g., 'EUR', 'JPY').

    Returns:
        Exchange rate as a float.
    """
    rates = {"EUR": 1.10, "JPY": 0.0090}
    rate = rates.get(currency)
    if rate is None:
        raise ValueError(f"No exchange rate configured for currency {currency}")
    return rate


@task(
    name="Convert Currency",
    retries=3,
    retry_delay_seconds=300,
    on_failure=_failure_handler("Convert Currency"),
)
def convert_currency(region: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts regional revenue to USD if needed.

    Args:
        region: Name of the region.
        df: DataFrame with original revenue column.

    Returns:
        DataFrame with a new column 'revenue_usd'.
    """
    log = get_run_logger()
    log.info("Converting currency for %s", region)

    df = df.copy()
    if region in ("US-East", "US-West"):
        df["revenue_usd"] = df["revenue"]
    elif region == "EU":
        rate = get_exchange_rate("EUR")
        df["revenue_usd"] = df["revenue"] * rate
    elif region == "APAC":
        rate = get_exchange_rate("JPY")
        df["revenue_usd"] = df["revenue"] * rate
    else:
        raise ValueError(f"Unknown region {region}")

    return df


@task(
    name="Aggregate Global Revenue",
    retries=3,
    retry_delay_seconds=300,
    on_failure=_failure_handler("Aggregate Global Revenue"),
)
def aggregate_global_revenue(
    regional_data: Dict[str, pd.DataFrame], output_path: Path
) -> Path:
    """
    Aggregates converted regional data into a global revenue report.

    Args:
        regional_data: Mapping of region name to its converted DataFrame.
        output_path: Destination path for the CSV report.

    Returns:
        Path to the generated report.
    """
    log = get_run_logger()
    log.info("Aggregating global revenue report")

    summary_rows: List[Dict[str, any]] = []
    total_revenue = 0.0

    for region, df in regional_data.items():
        region_total = df["revenue_usd"].sum()
        summary_rows.append({"region": region, "revenue_usd": region_total})
        total_revenue += region_total
        log.info("%s revenue (USD): %.2f", region, region_total)

    summary_rows.append({"region": "Global Total", "revenue_usd": total_revenue})
    report_df = pd.DataFrame(summary_rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(output_path, index=False)
    log.info("Global revenue report written to %s", output_path)

    return output_path


@flow(name="Multi-Region E-commerce Analytics")
def ecommerce_analytics_flow() -> None:
    """
    Orchestrates the multi-region e-commerce analytics pipeline.
    Executes ingestion and conversion in parallel, then aggregates results.
    """
    # Start
    start_pipeline.submit()

    # Define file locations (replace with actual paths as needed)
    data_dir = Path("data")
    file_map = {
        "US-East": data_dir / "us_east_sales.csv",
        "US-West": data_dir / "us_west_sales.csv",
        "EU": data_dir / "eu_sales.csv",
        "APAC": data_dir / "apac_sales.csv",
    }

    # Fan‑out Phase 1: Ingestion (parallel)
    ingest_futures = {
        region: ingest_region.submit(region, path) for region, path in file_map.items()
    }

    # Fan‑out Phase 2: Currency conversion (parallel, depends on ingestion)
    convert_futures = {
        region: convert_currency.submit(region, future) for region, future in ingest_futures.items()
    }

    # Gather conversion results
    converted_data: Dict[str, pd.DataFrame] = {
        region: future.result() for region, future in convert_futures.items()
    }

    # Fan‑in: Aggregation
    report_path = Path("output") / "global_revenue_report.csv"
    aggregate_global_revenue.submit(converted_data, report_path)

    # End
    end_pipeline.submit()


if __name__ == "__main__":
    # For local execution; in production, configure a Prefect deployment with a daily schedule.
    ecommerce_analytics_flow()