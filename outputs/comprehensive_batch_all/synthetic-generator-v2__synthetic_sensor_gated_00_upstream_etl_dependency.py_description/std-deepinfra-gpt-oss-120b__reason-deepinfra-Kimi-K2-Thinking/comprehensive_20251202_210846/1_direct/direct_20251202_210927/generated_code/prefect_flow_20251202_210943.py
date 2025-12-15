import time
import logging
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task, get_run_logger

# Constants for file locations
AGGREGATION_MARKER = Path("data/sales_aggregation_done.txt")
CSV_PATH = Path("data/aggregated_sales.csv")
DASHBOARD_PATH = Path("output/executive_dashboard.png")

# Retry configuration
RETRIES = 2
RETRY_DELAY_SECONDS = 300  # 5 minutes


@task(retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def wait_for_sales_aggregation(
    poll_interval: int = 60, timeout_seconds: int = 3600
) -> None:
    """
    Polls for the presence of a marker file indicating that the upstream
    daily_sales_aggregation DAG has completed successfully.

    Args:
        poll_interval: Seconds between successive polls.
        timeout_seconds: Maximum time to wait before raising a timeout.

    Raises:
        TimeoutError: If the marker file does not appear within the timeout.
    """
    logger = get_run_logger()
    start_time = time.time()
    logger.info("Waiting for sales aggregation to complete...")

    while True:
        if AGGREGATION_MARKER.is_file():
            logger.info("Detected sales aggregation completion.")
            return

        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Sales aggregation did not complete within {timeout_seconds} seconds."
            )
        time.sleep(poll_interval)


@task(retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def load_sales_csv() -> pd.DataFrame:
    """
    Loads the aggregated sales CSV, validates its structure and completeness,
    and returns a pandas DataFrame.

    Returns:
        DataFrame containing the sales data.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If required columns are missing or the DataFrame is empty.
    """
    logger = get_run_logger()
    logger.info(f"Loading sales data from {CSV_PATH}")

    if not CSV_PATH.is_file():
        raise FileNotFoundError(f"Aggregated sales CSV not found at {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    required_columns = {"date", "revenue", "product", "region"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    if df.empty:
        raise ValueError("Loaded CSV is empty.")

    logger.info("Sales data loaded and validated successfully.")
    return df


@task(retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def generate_dashboard(df: pd.DataFrame) -> Path:
    """
    Generates an executive dashboard with revenue trends, product performance,
    and regional analysis visualizations, saving it to a PNG file.

    Args:
        df: DataFrame containing validated sales data.

    Returns:
        Path to the generated dashboard image.
    """
    logger = get_run_logger()
    logger.info("Generating executive dashboard...")

    # Ensure output directory exists
    DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    ax_rev_trend, ax_product_perf, ax_region_analysis, ax_unused = axes.flat

    # Revenue trend over time
    revenue_by_date = df.groupby("date")["revenue"].sum()
    revenue_by_date.plot(ax=ax_rev_trend, marker="o")
    ax_rev_trend.set_title("Revenue Trend Over Time")
    ax_rev_trend.set_xlabel("Date")
    ax_rev_trend.set_ylabel("Revenue")

    # Product performance (total revenue per product)
    revenue_by_product = df.groupby("product")["revenue"].sum().sort_values(ascending=False)
    revenue_by_product.plot(kind="bar", ax=ax_product_perf)
    ax_product_perf.set_title("Product Performance")
    ax_product_perf.set_xlabel("Product")
    ax_product_perf.set_ylabel("Revenue")

    # Regional analysis (average revenue per region)
    revenue_by_region = df.groupby("region")["revenue"].mean()
    revenue_by_region.plot(kind="pie", ax=ax_region_analysis, autopct="%1.1f%%")
    ax_region_analysis.set_title("Regional Revenue Distribution")
    ax_region_analysis.set_ylabel("")

    # Hide unused subplot
    ax_unused.axis("off")

    plt.tight_layout()
    plt.savefig(DASHBOARD_PATH)
    plt.close(fig)

    logger.info(f"Dashboard saved to {DASHBOARD_PATH}")
    return DASHBOARD_PATH


def send_failure_email(task_name: str, exc: Exception) -> None:
    """
    Sends an email notification on task failure.
    This implementation logs the failure; replace with actual email logic as needed.

    Args:
        task_name: Name of the task or flow that failed.
        exc: Exception that was raised.
    """
    logger = logging.getLogger("failure_notifier")
    logger.error("Task '%s' failed with exception: %s", task_name, exc)


@flow
def executive_sales_dashboard_flow() -> None:
    """
    Orchestrates the executive sales dashboard pipeline:
    1. Wait for upstream sales aggregation.
    2. Load the aggregated sales CSV.
    3. Generate the executive dashboard.
    """
    try:
        wait_for_sales_aggregation()
        df = load_sales_csv()
        generate_dashboard(df)
    except Exception as exc:  # pragma: no cover
        send_failure_email("executive_sales_dashboard_flow", exc)
        raise


if __name__ == "__main__":
    # Note: In production, configure a Prefect deployment with a daily schedule
    # aligned to the upstream aggregation DAG.
    executive_sales_dashboard_flow()