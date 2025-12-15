from dagster import (
    op,
    job,
    sensor,
    RunRequest,
    SkipReason,
    Definitions,
    Config,
    OpExecutionContext,
    RetryPolicy,
    DefaultScheduleStatus,
)
import pandas as pd
import os
from datetime import datetime

# --- Configuration Classes ---
class CSVConfig(Config):
    csv_path: str = "/path/to/sales_data.csv"
    dashboard_output_path: str = "/path/to/dashboard.html"

# --- Resources (stubs) ---
# In a real implementation, you would configure these resources
# with actual email server settings and file system connections.
# For example:
# @resource
# def email_resource(context):
#     return EmailClient(smtp_server="smtp.example.com", ...)

# --- Ops ---
@op(
    description="Loads aggregated sales CSV data and validates format and completeness",
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 5 minute delay
)
def load_sales_csv(context: OpExecutionContext, config: CSVConfig) -> pd.DataFrame:
    """Load and validate sales CSV data."""
    context.log.info(f"Loading sales data from {config.csv_path}")
    
    # Check if file exists
    if not os.path.exists(config.csv_path):
        raise FileNotFoundError(f"CSV file not found: {config.csv_path}")
    
    # Load CSV
    df = pd.read_csv(config.csv_path)
    
    # Validate data
    if df.empty:
        raise ValueError("CSV data is empty")
    
    required_columns = ["date", "revenue", "product", "region"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    context.log.info(f"Successfully loaded {len(df)} rows of sales data")
    return df

@op(
    description="Generates executive dashboard with revenue trends, product performance, and regional analysis",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def generate_dashboard(context: OpExecutionContext, sales_data: pd.DataFrame, config: CSVConfig) -> None:
    """Generate executive dashboard visualizations."""
    context.log.info("Generating executive dashboard")
    
    # In a real implementation, this would generate actual visualizations
    # For example using matplotlib, plotly, or a dashboarding library
    # For now, we'll create a simple HTML report as a stub
    
    total_revenue = sales_data["revenue"].sum()
    top_product = sales_data.groupby("product")["revenue"].sum().idxmax()
    top_region = sales_data.groupby("region")["revenue"].sum().idxmax()
    
    html_content = f"""
    <html>
    <head><title>Executive Sales Dashboard</title></head>
    <body>
        <h1>Executive Sales Dashboard - {datetime.now().strftime('%Y-%m-%d')}</h1>
        <h2>Key Metrics</h2>
        <ul>
            <li>Total Revenue: ${total_revenue:,.2f}</li>
            <li>Top Product: {top_product}</li>
            <li>Top Region: {top_region}</li>
        </ul>
        <h2>Revenue Trends</h2>
        <p>[Revenue trend chart would be here]</p>
        <h2>Product Performance</h2>
        <p>[Product performance chart would be here]</p>
        <h2>Regional Analysis</h2>
        <p>[Regional analysis chart would be here]</p>
    </body>
    </html>
    """
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(config.dashboard_output_path), exist_ok=True)
    
    # Write dashboard
    with open(config.dashboard_output_path, "w") as f:
        f.write(html_content)
    
    context.log.info(f"Dashboard saved to {config.dashboard_output_path}")

# --- Job ---
@job(
    description="Executive sales dashboard pipeline with sensor-gated execution",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def executive_sales_dashboard_job():
    """Define the job dependencies."""
    sales_data = load_sales_csv()
    generate_dashboard(sales_data)

# --- Sensor ---
# In a real implementation, this sensor would poll Airflow's API or database
# to check if daily_sales_aggregation DAG has completed successfully.
# For demonstration, we'll check for a marker file.

@sensor(
    job=executive_sales_dashboard_job,
    minimum_interval_seconds=60,  # 60-second polling intervals
    description="Monitors daily_sales_aggregation DAG for successful completion"
)
def wait_for_sales_aggregation_sensor(context):
    """Sensor to wait for upstream DAG completion."""
    # In production, replace this with actual Airflow DAG monitoring logic
    # For example, query Airflow's metadata DB or API
    marker_file = "/tmp/daily_sales_aggregation_complete.marker"
    
    if os.path.exists(marker_file):
        # Marker file exists, trigger the job
        context.log.info("Upstream DAG completion detected, triggering dashboard generation")
        # Remove marker to prevent re-triggering
        os.remove(marker_file)
        return RunRequest(run_key=f"sales_dashboard_{datetime.now().isoformat()}")
    
    # Otherwise, skip
    return SkipReason("Waiting for upstream daily_sales_aggregation DAG to complete")

# --- Schedule (optional, but mentioned in description) ---
# Since the pipeline is sensor-gated, a schedule might not be necessary.
# However, we can add a daily schedule as a fallback or for documentation.

# --- Definitions ---
# This is the main entry point for Dagster to load your code
defs = Definitions(
    jobs=[executive_sales_dashboard_job],
    sensors=[wait_for_sales_aggregation_sensor],
    # resources={
    #     "email": email_resource,
    # }
)

# --- Launch Pattern ---
if __name__ == "__main__":
    # For local testing, you can execute the job directly
    # In production, this would be triggered by the sensor
    
    # Create a test marker file to simulate upstream completion
    marker_file = "/tmp/daily_sales_aggregation_complete.marker"
    with open(marker_file, "w") as f:
        f.write("complete")
    
    # Execute the sensor
    from dagster import build_sensor_context
    sensor_context = build_sensor_context()
    result = wait_for_sales_aggregation_sensor(sensor_context)
    
    # If the sensor returns a RunRequest, execute the job
    if isinstance(result, RunRequest):
        # Execute the job
        job_result = executive_sales_dashboard_job.execute_in_process(
            run_config={
                "ops": {
                    "load_sales_csv": {
                        "config": {
                            "csv_path": "/tmp/test_sales_data.csv",
                            "dashboard_output_path": "/tmp/test_dashboard.html"
                        }
                    },
                    "generate_dashboard": {
                        "config": {
                            "csv_path": "/tmp/test_sales_data.csv",
                            "dashboard_output_path": "/tmp/test_dashboard.html"
                        }
                    }
                }
            }
        )
        
        if job_result.success:
            print("Job executed successfully!")
        else:
            print("Job failed!")
            # In production, email notification would be sent here
    else:
        print("Sensor skipped execution")