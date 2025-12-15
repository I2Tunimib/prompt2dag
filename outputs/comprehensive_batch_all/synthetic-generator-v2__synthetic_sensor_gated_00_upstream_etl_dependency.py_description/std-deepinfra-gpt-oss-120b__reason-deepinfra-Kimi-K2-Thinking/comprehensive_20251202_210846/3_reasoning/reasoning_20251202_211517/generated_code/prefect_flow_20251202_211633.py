import time
from datetime import timedelta
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from prefect import flow, task
from prefect.tasks import task_input_hash


def check_external_dag_completion(dag_id: str = "daily_sales_aggregation") -> bool:
    """
    Simulates checking Airflow DAG completion status.
    In production, replace with actual Airflow API call or database query.
    """
    import os
    # Simulate a marker file that the upstream DAG creates on completion
    marker_file = f"/tmp/{dag_id}_complete.marker"
    return os.path.exists(marker_file)


@task(
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=3600,
    description="Wait for daily_sales_aggregation DAG completion with 60s polling"
)
def wait_for_sales_aggregation(polling_interval: int = 60, timeout: int = 3600) -> bool:
    """
    Sensor task that polls for upstream DAG completion.
    Uses reschedule-like behavior by sleeping between polls.
    """
    elapsed = 0
    while elapsed < timeout:
        if check_external_dag_completion():
            return True
        
        time.sleep(polling_interval)
        elapsed += polling_interval
    
    raise TimeoutError(
        f"Upstream DAG 'daily_sales_aggregation' did not complete within {timeout} seconds"
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Load and validate aggregated sales CSV data"
)
def load_sales_csv(csv_path: str) -> pd.DataFrame:
    """
    Loads sales data from CSV and validates format/completeness.
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        raise ValueError(f"Failed to load CSV from {csv_path}: {e}")
    
    # Validate required columns exist
    required_columns = ["date", "revenue", "product_id", "region", "units_sold"]
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Validate data completeness (no empty DataFrame)
    if df.empty:
        raise ValueError("Loaded CSV is empty")
    
    # Validate no nulls in critical columns
    if df[required_columns].isnull().any().any():
        raise ValueError("Critical columns contain null values")
    
    return df


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Generate executive dashboard with sales metrics and visualizations"
)
def generate_dashboard(data: pd.DataFrame, output_path: str) -> str:
    """
    Generates executive dashboard with revenue trends, product performance,
    and regional analysis visualizations.
    """
    # Set style for professional dashboard
    sns.set_style("whitegrid")
    plt.rcParams["figure.figsize"] = (12, 8)
    
    # Create figure with subplots
    fig = plt.figure(figsize=(16, 12))
    fig.suptitle("Executive Sales Dashboard", fontsize=20, fontweight="bold", y=0.98)
    
    # 1. Revenue Trends
    ax1 = plt.subplot(2, 2, 1)
    daily_revenue = data.groupby("date")["revenue"].sum().reset_index()
    daily_revenue["date"] = pd.to_datetime(daily_revenue["date"])
    daily_revenue = daily_revenue.sort_values("date")
    ax1.plot(daily_revenue["date"], daily_revenue["revenue"], marker="o", linewidth=2)
    ax1.set_title("Daily Revenue Trends", fontsize=14, fontweight="bold")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Revenue ($)")
    ax1.tick_params(axis="x", rotation=45)
    
    # 2. Product Performance
    ax2 = plt.subplot(2, 2, 2)
    product_perf = data.groupby("product_id")["revenue"].sum().sort_values(ascending=False).head(10)
    product_perf.plot(kind="bar", ax=ax2, color="skyblue")
    ax2.set_title("Top 10 Products by Revenue", fontsize=14, fontweight="bold")
    ax2.set_xlabel("Product ID")
    ax2.set_ylabel("Revenue ($)")
    ax2.tick_params(axis="x", rotation=45)
    
    # 3. Regional Analysis
    ax3 = plt.subplot(2, 2, 3)
    regional_sales = data.groupby("region")["revenue"].sum().sort_values(ascending=False)
    colors = plt.cm.Set3(range(len(regional_sales)))
    ax3.pie(regional_sales.values, labels=regional_sales.index, autopct="%1.1f%%", colors=colors)
    ax3.set_title("Revenue by Region", fontsize=14, fontweight="bold")
    
    # 4. Units Sold Distribution
    ax4 = plt.subplot(2, 2, 4)
    units_by_region = data.groupby("region")["units_sold"].sum().sort_values(ascending=False)
    units_by_region.plot(kind="bar", ax=ax4, color="lightcoral")
    ax4.set_title("Units Sold by Region", fontsize=14, fontweight="bold")
    ax4.set_xlabel("Region")
    ax4.set_ylabel("Units Sold")
    ax4.tick_params(axis="x", rotation=45)
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    
    # Save dashboard
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    return output_path


@flow(
    name="executive-sales-dashboard-pipeline",
    description="Daily executive sales dashboard generation with sensor-gated execution"
)
def executive_sales_dashboard_pipeline(
    csv_path: str = "/data/sales/aggregated_daily_sales.csv",
    output_path: str = "/dashboards/executive_sales_dashboard.png"
):
    """
    Main flow that orchestrates the executive sales dashboard pipeline.
    Executes sequentially: sensor → data loading → dashboard generation.
    """
    # Step 1: Wait for upstream DAG completion
    # In production, configure schedule via Prefect deployment:
    # e.g., Cron schedule: "0 8 * * *" for daily at 8 AM
    wait_for_sales_aggregation(polling_interval=60, timeout=3600)
    
    # Step 2: Load and validate sales data
    sales_data = load_sales_csv(csv_path)
    
    # Step 3: Generate executive dashboard
    dashboard_path = generate_dashboard(sales_data, output_path)
    
    return dashboard_path


if __name__ == "__main__":
    # Local execution entry point
    # For production deployment, use: prefect deployment build/exec
    result = executive_sales_dashboard_pipeline()
    print(f"Dashboard generated successfully: {result}")