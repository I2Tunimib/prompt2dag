from datetime import timedelta
import os

import pandas as pd
import matplotlib.pyplot as plt

from dagster import (
    op,
    job,
    RetryPolicy,
    sensor,
    RunRequest,
    SkipReason,
)


@op(
    config_schema={"csv_path": str},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Load aggregated sales CSV and validate its contents.",
)
def load_sales_csv(context) -> pd.DataFrame:
    """Load the sales CSV file and ensure required columns are present."""
    csv_path = context.op_config["csv_path"]
    logger = context.log
    logger.info(f"Loading sales data from %s", csv_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    required_columns = {"date", "region", "product", "revenue"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    logger.info("Loaded %d rows from CSV.", len(df))
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Generate an executive dashboard from sales data.",
)
def generate_dashboard(context, sales_df: pd.DataFrame) -> str:
    """Create a simple revenue-over-time chart and save it as an image."""
    logger = context.log

    df = sales_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    daily_revenue = df.groupby("date")["revenue"].sum()

    plt.figure(figsize=(10, 6))
    daily_revenue.plot()
    plt.title("Daily Revenue")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    output_path = "executive_dashboard.png"
    plt.savefig(output_path)
    plt.close()

    logger.info("Dashboard saved to %s", output_path)
    return output_path


@job(description="End-to-end executive sales dashboard generation pipeline.")
def executive_dashboard_job():
    sales_data = load_sales_csv()
    generate_dashboard(sales_data)


@sensor(job=executive_dashboard_job, minimum_interval_seconds=60)
def sales_aggregation_sensor(context):
    """
    Sensor that waits for the upstream sales aggregation to complete.
    It checks for a marker file ``sales_aggregation_done.txt``.
    When the file exists, it triggers the dashboard job.
    """
    marker_path = "sales_aggregation_done.txt"
    if os.path.exists(marker_path):
        # Assume the aggregated CSV is at a known location.
        csv_path = "aggregated_sales.csv"
        run_config = {
            "ops": {
                "load_sales_csv": {"config": {"csv_path": csv_path}}
            }
        }
        yield RunRequest(run_key=marker_path, run_config=run_config)
    else:
        yield SkipReason("Sales aggregation not yet completed.")


if __name__ == "__main__":
    # Direct execution for testing purposes.
    result = executive_dashboard_job.execute_in_process(
        run_config={
            "ops": {
                "load_sales_csv": {"config": {"csv_path": "aggregated_sales.csv"}}
            }
        }
    )
    print(f"Job succeeded: {result.success}")