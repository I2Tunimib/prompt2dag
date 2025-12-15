from datetime import datetime, timedelta
import os
import time
from typing import Any, Dict, List

import pandas as pd
import matplotlib.pyplot as plt

from dagster import (
    op,
    job,
    RetryPolicy,
    In,
    Out,
    Nothing,
    Output,
    ConfigurableResource,
    resource,
    get_dagster_logger,
)


class FileSystemResource(ConfigurableResource):
    """Simple resource to interact with the local file system."""

    base_path: str = "."

    def resolve_path(self, relative_path: str) -> str:
        """Return an absolute path within the configured base path."""
        return os.path.abspath(os.path.join(self.base_path, relative_path))


@resource
def file_system_resource(init_context) -> FileSystemResource:
    config = init_context.resource_config or {}
    base_path = config.get("base_path", ".")
    return FileSystemResource(base_path=base_path)


@op(
    name="wait_for_sales_aggregation",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(Nothing),
    required_resource_keys={"file_system"},
    config_schema={
        "completion_marker": str,
        "poll_interval_seconds": int,
        "timeout_seconds": int,
    },
)
def wait_for_sales_aggregation(context) -> Nothing:
    """
    Polls for a marker file that indicates the upstream sales aggregation DAG has completed.
    Uses a reschedule‑style loop with configurable interval and timeout.
    """
    logger = get_dagster_logger()
    cfg: Dict[str, Any] = context.op_config
    marker_path = context.resources.file_system.resolve_path(cfg["completion_marker"])
    poll_interval = cfg["poll_interval_seconds"]
    timeout = cfg["timeout_seconds"]
    start_time = datetime.utcnow()

    logger.info(f"Waiting for sales aggregation marker at {marker_path}")

    while True:
        if os.path.exists(marker_path):
            logger.info("Sales aggregation marker detected.")
            break

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        if elapsed > timeout:
            raise RuntimeError(
                f"Timeout of {timeout} seconds exceeded while waiting for sales aggregation."
            )

        logger.debug(
            f"Marker not found. Sleeping for {poll_interval} seconds (elapsed {int(elapsed)}s)."
        )
        time.sleep(poll_interval)

    return Output(value=None)


@op(
    name="load_sales_csv",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    ins={"_start": In(Nothing)},
    out=Out(pd.DataFrame),
    required_resource_keys={"file_system"},
    config_schema={"csv_path": str, "required_columns": List[str]},
)
def load_sales_csv(context, _start: Nothing) -> pd.DataFrame:
    """
    Loads the aggregated sales CSV produced by the upstream process.
    Validates that required columns are present and that the file is not empty.
    """
    logger = get_dagster_logger()
    cfg: Dict[str, Any] = context.op_config
    csv_path = context.resources.file_system.resolve_path(cfg["csv_path"])
    required_columns: List[str] = cfg.get("required_columns", [])

    logger.info(f"Loading sales data from {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Aggregated sales CSV not found at {csv_path}")

    df = pd.read_csv(csv_path)

    if df.empty:
        raise ValueError("Loaded sales CSV is empty.")

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in sales CSV: {missing}")

    logger.info(f"Loaded sales data with {len(df)} rows and columns: {list(df.columns)}")
    return df


@op(
    name="generate_dashboard",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    ins={"sales_df": In(pd.DataFrame)},
    out=Out(Nothing),
    required_resource_keys={"file_system"},
    config_schema={
        "output_path": str,
        "revenue_column": str,
        "date_column": str,
        "product_column": str,
        "region_column": str,
    },
)
def generate_dashboard(context, sales_df: pd.DataFrame) -> Nothing:
    """
    Generates a simple executive dashboard with revenue trends,
    product performance, and regional analysis visualizations.
    Saves the dashboard as an HTML file containing embedded PNG images.
    """
    logger = get_dagster_logger()
    cfg: Dict[str, Any] = context.op_config

    revenue_col = cfg["revenue_column"]
    date_col = cfg["date_column"]
    product_col = cfg["product_column"]
    region_col = cfg["region_column"]
    output_path = context.resources.file_system.resolve_path(cfg["output_path"])

    logger.info("Generating revenue trend plot")
    plt.figure(figsize=(10, 4))
    sales_df.groupby(date_col)[revenue_col].sum().plot()
    plt.title("Revenue Trend Over Time")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    revenue_plot_path = os.path.join(os.path.dirname(output_path), "revenue_trend.png")
    plt.tight_layout()
    plt.savefig(revenue_plot_path)
    plt.close()

    logger.info("Generating product performance plot")
    plt.figure(figsize=(10, 4))
    sales_df.groupby(product_col)[revenue_col].sum().sort_values(ascending=False).plot(kind="bar")
    plt.title("Revenue by Product")
    plt.xlabel("Product")
    plt.ylabel("Revenue")
    product_plot_path = os.path.join(os.path.dirname(output_path), "product_performance.png")
    plt.tight_layout()
    plt.savefig(product_plot_path)
    plt.close()

    logger.info("Generating regional analysis plot")
    plt.figure(figsize=(10, 4))
    sales_df.groupby(region_col)[revenue_col].sum().plot(kind="pie", autopct="%1.1f%%")
    plt.title("Revenue Distribution by Region")
    region_plot_path = os.path.join(os.path.dirname(output_path), "regional_analysis.png")
    plt.tight_layout()
    plt.savefig(region_plot_path)
    plt.close()

    logger.info(f"Saving dashboard HTML to {output_path}")
    html_content = f"""
    <html>
        <head><title>Executive Sales Dashboard</title></head>
        <body>
            <h1>Executive Sales Dashboard</h1>
            <h2>Revenue Trend</h2>
            <img src="{os.path.basename(revenue_plot_path)}" alt="Revenue Trend"/>
            <h2>Product Performance</h2>
            <img src="{os.path.basename(product_plot_path)}" alt="Product Performance"/>
            <h2>Regional Analysis</h2>
            <img src="{os.path.basename(region_plot_path)}" alt="Regional Analysis"/>
        </body>
    </html>
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info("Dashboard generation completed")
    return Output(value=None)


@job(
    resource_defs={"file_system": file_system_resource},
    description="Daily executive sales dashboard pipeline with sensor‑gated execution.",
)
def executive_sales_dashboard_job():
    """
    Orchestrates the executive sales dashboard generation:
    1. Wait for upstream sales aggregation to complete.
    2. Load the aggregated sales CSV.
    3. Generate the executive dashboard.
    """
    wait = wait_for_sales_aggregation()
    sales_df = load_sales_csv(wait)
    generate_dashboard(sales_df)


if __name__ == "__main__":
    # Example configuration; replace paths as needed for your environment.
    run_config = {
        "ops": {
            "wait_for_sales_aggregation": {
                "config": {
                    "completion_marker": "data/sales_aggregation_done.txt",
                    "poll_interval_seconds": 60,
                    "timeout_seconds": 3600,
                }
            },
            "load_sales_csv": {
                "config": {
                    "csv_path": "data/aggregated_sales.csv",
                    "required_columns": ["date", "product", "region", "revenue"],
                }
            },
            "generate_dashboard": {
                "config": {
                    "output_path": "dashboard/executive_dashboard.html",
                    "revenue_column": "revenue",
                    "date_column": "date",
                    "product_column": "product",
                    "region_column": "region",
                }
            },
        },
        "resources": {
            "file_system": {"config": {"base_path": "."}}
        },
    }

    result = executive_sales_dashboard_job.execute_in_process(run_config=run_config)
    if result.success:
        print("Executive sales dashboard pipeline completed successfully.")
    else:
        print("Pipeline failed.")