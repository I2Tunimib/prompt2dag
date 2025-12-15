from prefect import flow, task
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional
import sys


@task(name="wait-for-sales-aggregation")
def wait_for_sales_aggregation(
    marker_path: str = "/tmp/daily_sales_aggregation_complete.marker",
    timeout_seconds: int = 3600,
    polling_interval: int = 60,
) -> bool:
    """
    Simulates an ExternalTaskSensor by polling for a completion marker file.
    Uses reschedule pattern with configurable polling interval and timeout.
    """
    elapsed = 0
    marker = Path(marker_path)
    
    while elapsed < timeout_seconds:
        if marker.exists():
            print(f"Upstream daily_sales_aggregation detected as complete.")
            return True
        
        print(f"Waiting for upstream aggregation... elapsed: {elapsed}s")
        time.sleep(polling_interval)
        elapsed += polling_interval
    
    raise TimeoutError(
        f"Upstream daily_sales_aggregation did not complete within {timeout_seconds} seconds"
    )


@task(name="load-sales-csv")
def load_sales_csv(csv_path: str) -> pd.DataFrame:
    """
    Loads aggregated sales CSV data and validates format/completeness.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Sales CSV not found at: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Validate required columns exist
    required_columns = {"date", "revenue", "product", "region"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")
    
    # Validate data completeness
    if df.empty:
        raise ValueError("Sales CSV is empty")
    
    if df.isnull().all().any():
        raise ValueError("Sales CSV contains completely empty columns")
    
    print(f"Successfully loaded {len(df)} rows of sales data")
    return df


@task(name="generate-dashboard")
def generate_dashboard(
    data: pd.DataFrame, output_dir: str = "/tmp/executive_dashboard"
) -> str:
    """
    Generates executive dashboard with revenue trends, product performance,
    and regional analysis visualizations.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Set style for professional dashboard
    sns.set_style("whitegrid")
    plt.rcParams["figure.figsize"] = (12, 8)
    
    # 1. Revenue Trends
    fig, ax = plt.subplots()
    daily_revenue = data.groupby("date")["revenue"].sum()
    daily_revenue.plot(kind="line", ax=ax, marker="o")
    ax.set_title("Daily Revenue Trends")
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue ($)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path / "revenue_trends.png", dpi=300)
    plt.close()
    
    # 2. Product Performance
    fig, ax = plt.subplots()
    product_perf = data.groupby("product")["revenue"].sum().sort_values(ascending=False)
    product_perf.plot(kind="bar", ax=ax)
    ax.set_title("Product Performance")
    ax.set_xlabel("Product")
    ax.set_ylabel("Revenue ($)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path / "product_performance.png", dpi=300)
    plt.close()
    
    # 3. Regional Analysis
    fig, ax = plt.subplots()
    regional_sales = data.groupby("region")["revenue"].sum()
    regional_sales.plot(kind="pie", ax=ax, autopct="%1.1f%%")
    ax.set_title("Revenue by Region")
    ax.set_ylabel("")
    plt.tight_layout()
    plt.savefig(output_path / "regional_analysis.png", dpi=300)
    plt.close()
    
    dashboard_index = output_path / "index.html"
    with open(dashboard_index, "w") as f:
        f.write("""
        <html>
        <head><title>Executive Sales Dashboard</title></head>
        <body>
            <h1>Executive Sales Dashboard</h1>
            <h2>Revenue Trends</h2>
            <img src="revenue_trends.png" alt="Revenue Trends">
            <h2>Product Performance</h2>
            <img src="product_performance.png" alt="Product Performance">
            <h2>Regional Analysis</h2>
            <img src="regional_analysis.png" alt="Regional Analysis">
        </body>
        </html>
        """)
    
    print(f"Dashboard generated at: {output_path}")
    return str(output_path)


@flow(
    name="executive-sales-dashboard-pipeline",
    retries=2,
    retry_delay_seconds=300,
    # Note: Configure daily schedule via deployment: 
    # prefect deployment build --cron "0 8 * * *" ...
)
def executive_sales_dashboard_pipeline(
    csv_path: str = "/tmp/sales_aggregated.csv",
    output_dir: str = "/tmp/executive_dashboard",
    marker_path: str = "/tmp/daily_sales_aggregation_complete.marker",
):
    """
    Sensor-gated pipeline that waits for upstream sales aggregation,
    loads CSV data, and generates executive dashboard visualizations.
    """
    # Step 1: Wait for upstream DAG completion
    wait_for_sales_aggregation(marker_path=marker_path)
    
    # Step 2: Load and validate sales data
    sales_data = load_sales_csv(csv_path)
    
    # Step 3: Generate dashboard
    generate_dashboard(sales_data, output_dir)


if __name__ == "__main__":
    # Local execution entry point
    executive_sales_dashboard_pipeline()