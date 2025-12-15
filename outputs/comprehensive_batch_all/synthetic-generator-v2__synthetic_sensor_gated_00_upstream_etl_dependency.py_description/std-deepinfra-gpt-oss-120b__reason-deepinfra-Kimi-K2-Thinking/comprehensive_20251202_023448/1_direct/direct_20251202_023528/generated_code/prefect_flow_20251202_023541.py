import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for file locations
BASE_DIR = Path(__file__).parent
MARKER_FILE = BASE_DIR / "sales_aggregation_done.txt"
CSV_FILE = BASE_DIR / "aggregated_sales.csv"
DASHBOARD_FILE = BASE_DIR / "executive_dashboard.png"


@task(retries=2, retry_delay_seconds=300)
def wait_for_sales_aggregation(
    marker_path: Path = MARKER_FILE,
    poll_interval_seconds: int = 60,
    timeout_seconds: int = 3600,
) -> None:
    """
    Polls for the existence of a marker file indicating that the upstream
    daily sales aggregation DAG has completed successfully.

    Args:
        marker_path: Path to the marker file created by the upstream process.
        poll_interval_seconds: Seconds between successive polls.
        timeout_seconds: Maximum time to wait before raising a TimeoutError.

    Raises:
        TimeoutError: If the marker file does not appear within the timeout.
    """
    logger.info("Waiting for sales aggregation to complete...")
    deadline = datetime.utcnow() + timedelta(seconds=timeout_seconds)

    while datetime.utcnow() < deadline:
        if marker_path.is_file():
            logger.info("Detected completion marker: %s", marker_path)
            return
        logger.debug(
            "Marker not found. Sleeping for %s seconds.", poll_interval_seconds
        )
        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Timed out after {timeout_seconds} seconds waiting for {marker_path}"
    )


@task(retries=2, retry_delay_seconds=300)
def load_sales_csv(csv_path: Path = CSV_FILE) -> pd.DataFrame:
    """
    Loads the aggregated sales CSV file and validates its contents.

    Args:
        csv_path: Path to the aggregated sales CSV file.

    Returns:
        pandas.DataFrame containing the sales data.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If the DataFrame is empty or missing required columns.
    """
    logger.info("Loading sales data from %s", csv_path)

    if not csv_path.is_file():
        raise FileNotFoundError(f"Sales CSV not found at {csv_path}")

    df = pd.read_csv(csv_path)

    if df.empty:
        raise ValueError("Loaded sales CSV is empty")

    required_columns = {"date", "region", "product", "revenue", "units_sold"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in sales CSV: {missing}")

    logger.info("Successfully loaded %d rows of sales data", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def generate_dashboard(
    sales_df: pd.DataFrame,
    output_path: Path = DASHBOARD_FILE,
) -> None:
    """
    Generates an executive dashboard visualizing key sales metrics.

    The dashboard includes:
    - Revenue trend over time.
    - Top 5 products by revenue.
    - Regional revenue distribution.

    The resulting figure is saved as a PNG file.

    Args:
        sales_df: DataFrame containing validated sales data.
        output_path: Destination path for the dashboard image.
    """
    logger.info("Generating executive dashboard...")

    plt.figure(figsize=(12, 8))

    # Revenue trend over time
    plt.subplot(2, 2, 1)
    revenue_by_date = (
        sales_df.groupby("date")["revenue"]
        .sum()
        .reset_index()
        .sort_values("date")
    )
    plt.plot(
        pd.to_datetime(revenue_by_date["date"]),
        revenue_by_date["revenue"],
        marker="o",
    )
    plt.title("Revenue Trend Over Time")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.grid(True)

    # Top 5 products by revenue
    plt.subplot(2, 2, 2)
    top_products = (
        sales_df.groupby("product")["revenue"]
        .sum()
        .nlargest(5)
        .reset_index()
    )
    plt.bar(top_products["product"], top_products["revenue"], color="skyblue")
    plt.title("Top 5 Products by Revenue")
    plt.xlabel("Product")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45)

    # Regional revenue distribution
    plt.subplot(2, 2, 3)
    revenue_by_region = (
        sales_df.groupby("region")["revenue"]
        .sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    plt.pie(
        revenue_by_region["revenue"],
        labels=revenue_by_region["region"],
        autopct="%1.1f%%",
        startangle=140,
    )
    plt.title("Revenue Distribution by Region")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Dashboard saved to %s", output_path)


@flow
def executive_sales_dashboard_flow() -> None:
    """
    Orchestrates the executive sales dashboard pipeline.

    Sequence:
    1. Wait for upstream sales aggregation to complete.
    2. Load the aggregated sales CSV.
    3. Generate the executive dashboard.
    """
    wait_for_sales_aggregation()
    sales_data = load_sales_csv()
    generate_dashboard(sales_data)


if __name__ == "__main__":
    # Note: In production, this flow would be deployed with a daily schedule
    # aligned to the upstream aggregation DAG.
    executive_sales_dashboard_flow()