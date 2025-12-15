import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths and constants
UPSTREAM_MARKER = Path("data/sales_aggregation_complete.txt")
CSV_PATH = Path("data/aggregated_sales.csv")
DASHBOARD_PATH = Path("output/executive_dashboard.png")
REQUIRED_COLUMNS = {"date", "region", "product", "revenue", "units_sold"}

# Email notification placeholder (implement as needed)
def send_failure_email(task_name: str, exc: Exception) -> None:
    """Send a simple email notification on task failure.

    This is a placeholder; replace with real email logic or Prefect notifications.
    """
    logger.error("Task %s failed: %s", task_name, exc)
    # Example implementation could use smtplib or an external service.
    # For brevity, we only log the failure here.


@task(
    retries=2,
    retry_delay_seconds=300,
    name="wait_for_sales_aggregation",
    description="Polls for upstream sales aggregation completion.",
)
def wait_for_sales_aggregation() -> None:
    """Wait until the upstream sales aggregation process signals completion.

    The function checks for the presence of a marker file every 60 seconds,
    timing out after 1 hour.
    """
    poll_interval = 60  # seconds
    timeout = 3600  # seconds
    elapsed = 0

    logger.info("Starting sensor for upstream sales aggregation.")
    while elapsed < timeout:
        if UPSTREAM_MARKER.is_file():
            logger.info("Upstream aggregation marker detected.")
            return
        logger.debug(
            "Marker not found; sleeping %s seconds (elapsed %s seconds).",
            poll_interval,
            elapsed,
        )
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(
        f"Upstream sales aggregation did not complete within {timeout // 60} minutes."
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    name="load_sales_csv",
    description="Loads and validates the aggregated sales CSV.",
)
def load_sales_csv() -> pd.DataFrame:
    """Load the aggregated sales CSV and validate its schema."""
    if not CSV_PATH.is_file():
        raise FileNotFoundError(f"Aggregated sales CSV not found at {CSV_PATH}")

    logger.info("Loading sales data from %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH, parse_dates=["date"])

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    if df.empty:
        raise ValueError("Loaded CSV is empty.")

    logger.info("Sales data loaded successfully with %d rows.", len(df))
    return df


@task(
    retries=2,
    retry_delay_seconds=300,
    name="generate_dashboard",
    description="Generates an executive dashboard from sales data.",
)
def generate_dashboard(sales_df: pd.DataFrame) -> Path:
    """Create visualizations and save an executive dashboard image."""
    logger.info("Generating executive dashboard.")
    plt.figure(figsize=(12, 8))

    # Revenue trend over time
    ax1 = plt.subplot(2, 1, 1)
    revenue_by_date = sales_df.groupby("date")["revenue"].sum()
    revenue_by_date.plot(ax=ax1, marker="o")
    ax1.set_title("Revenue Trend Over Time")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Revenue")

    # Revenue by region
    ax2 = plt.subplot(2, 1, 2)
    revenue_by_region = sales_df.groupby("region")["revenue"].sum().sort_values(ascending=False)
    revenue_by_region.plot(kind="bar", ax=ax2, color="skyblue")
    ax2.set_title("Revenue by Region")
    ax2.set_xlabel("Region")
    ax2.set_ylabel("Revenue")

    plt.tight_layout()
    DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(DASHBOARD_PATH)
    plt.close()
    logger.info("Dashboard saved to %s", DASHBOARD_PATH)
    return DASHBOARD_PATH


@flow(name="executive_sales_dashboard")
def executive_sales_dashboard_flow() -> None:
    """Orchestrates the executive sales dashboard pipeline."""
    try:
        wait_for_sales_aggregation()
        sales_df = load_sales_csv()
        generate_dashboard(sales_df)
    except Exception as exc:  # pragma: no cover
        # Global failure handling; could be expanded with notifications.
        send_failure_email("executive_sales_dashboard_flow", exc)
        raise


if __name__ == "__main__":
    # Local execution; in production, configure a Prefect deployment with a daily schedule.
    executive_sales_dashboard_flow()