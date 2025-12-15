from prefect import flow, task
import time
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(
    name="wait_for_sales_aggregation",
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=3600
)
def wait_for_sales_aggregation(marker_path: str) -> bool:
    """
    Wait for upstream DAG completion by polling for a marker file.
    Polls every 60 seconds with a 1-hour timeout.
    """
    polling_interval = 60
    max_wait_time = 3600
    elapsed_time = 0
    
    logger.info(f"Waiting for marker file: {marker_path}")
    
    while elapsed_time < max_wait_time:
        if Path(marker_path).exists():
            logger.info("Upstream DAG completion marker found")
            return True
        
        time.sleep(polling_interval)
        elapsed_time += polling_interval
        logger.debug(f"Polling... elapsed time: {elapsed_time}s")
    
    raise TimeoutError(
        f"Timed out after {max_wait_time}s waiting for {marker_path}"
    )


@task(
    name="load_sales_csv",
    retries=2,
    retry_delay_seconds=300
)
def load_sales_csv(upstream_ready: bool, csv_path: str) -> pd.DataFrame:
    """
    Load and validate aggregated sales CSV from upstream output.
    """
    if not upstream_ready:
        raise RuntimeError("Upstream DAG not completed")
    
    logger.info(f"Loading sales data from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        
        if df.empty:
            raise ValueError("Loaded CSV is empty")
        
        required_columns = {
            'date', 'revenue', 'product_id', 'region', 
            'units_sold', 'customer_count'
        }
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        logger.info(f"Loaded {len(df)} rows of sales data")
        return df
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        raise


@task(
    name="generate_dashboard",
    retries=2,
    retry_delay_seconds=300
)
def generate_dashboard(sales_data: pd.DataFrame) -> str:
    """
    Generate executive dashboard with revenue trends, product performance,
    and regional analysis visualizations.
    """
    if sales_data is None or sales_data.empty:
        raise ValueError("No sales data provided")
    
    logger.info("Generating executive dashboard...")
    
    output_dir = Path("/data/dashboards")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    dashboard_path = output_dir / "executive_sales_dashboard.html"
    
    html_content = f"""
    <html>
    <head><title>Executive Sales Dashboard</title></head>
    <body>
        <h1>Daily Executive Sales Dashboard</h1>
        <h2>Key Metrics</h2>
        <ul>
            <li>Total Revenue: ${sales_data['revenue'].sum():,.2f}</li>
            <li>Total Units Sold: {sales_data['units_sold'].sum():,}</li>
            <li>Total Customers: {sales_data['customer_count'].sum():,}</li>
            <li>Average Order Value: ${sales_data['revenue'].sum() / sales_data['customer_count'].sum():.2f}</li>
        </ul>
        <h2>Top Products by Revenue</h2>
        <pre>{sales_data.groupby('product_id')['revenue'].sum().sort_values(ascending=False).head(5).to_string()}</pre>
        <h2>Regional Performance</h2>
        <pre>{sales_data.groupby('region')['revenue'].sum().sort_values(ascending=False).to_string()}</pre>
    </body>
    </html>
    """
    
    dashboard_path.write_text(html_content)
    logger.info(f"Dashboard saved to {dashboard_path}")
    
    return str(dashboard_path)


@flow(name="executive-sales-dashboard")
def executive_sales_dashboard(
    marker_path: str = "/data/aggregated_sales/_SUCCESS",
    csv_path: str = "/data/aggregated_sales/daily_sales.csv"
):
    """
    Daily executive sales dashboard pipeline.
    
    Linear sequence:
    1. Wait for upstream sales aggregation (sensor)
    2. Load and validate CSV data
    3. Generate executive dashboard
    """
    upstream_ready = wait_for_sales_aggregation(marker_path)
    sales_data = load_sales_csv(upstream_ready, csv_path)
    dashboard_path = generate_dashboard(sales_data)
    
    return dashboard_path


if __name__ == '__main__':
    # Local execution for testing
    # 
    # Production deployment with daily schedule (9 AM UTC, no catchup):
    # prefect deployment build executive_sales_dashboard.py:executive_sales_dashboard \
    #   --name "daily-executive-dashboard" \
    #   --cron "0 9 * * *" \
    #   --timezone "UTC" \
    #   --apply
    # 
    # Configure failure notifications via Prefect Cloud Automations
    # or add an automation with email/webhook alerts.
    
    executive_sales_dashboard()