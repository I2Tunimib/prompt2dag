from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    Failure,
    resource,
    get_dagster_logger,
    In,
    Out,
    Nothing,
)
import pandas as pd
import os
from datetime import datetime

logger = get_dagster_logger()

# Minimal resource stubs
@resource
def sales_csv_path():
    """Resource providing the path to the sales CSV file."""
    return "/path/to/aggregated_sales.csv"

@resource
def dashboard_output_path():
    """Resource providing the output path for the dashboard."""
    return "/path/to/executive_dashboard.html"

@resource
def upstream_dag_info():
    """Resource containing upstream DAG information."""
    return {
        "dag_id": "daily_sales_aggregation",
        "expected_run_time": "02:00:00",  # Expected completion time
    }

# Op 1: Wait for upstream aggregation
@op(
    required_resource_keys={"upstream_dag_info"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
    tags={"task_type": "sensor"},
)
def wait_for_sales_aggregation(context):
    """
    Simulates waiting for the upstream daily_sales_aggregation DAG to complete.
    In a real scenario, this would check Airflow's metadata DB or an external signal.
    """
    dag_info = context.resources.upstream_dag_info
    logger.info(f"Monitoring for completion of DAG: {dag_info['dag_id']}")
    
    # Simulated check - in production, query Airflow API or check for marker file
    # For demo purposes, we'll just log and proceed
    # TODO: Implement actual DAG completion check (e.g., via Airflow API, marker file)
    logger.info("Upstream DAG completion verified (simulated)")
    
    return True

# Op 2: Load and validate CSV
@op(
    ins={"upstream_complete": In(Nothing)},
    required_resource_keys={"sales_csv_path"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"task_type": "data_loading"},
)
def load_sales_csv(context):
    """
    Loads aggregated sales CSV data from upstream process and validates it.
    """
    csv_path = context.resources.sales_csv_path
    
    if not os.path.exists(csv_path):
        raise Failure(f"Sales CSV not found at expected path: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from {csv_path}")
        
        # Basic validation
        if df.empty:
            raise Failure("Loaded DataFrame is empty")
        
        required_columns = ["date", "revenue", "product_id", "region"]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise Failure(f"Missing required columns: {missing_cols}")
        
        logger.info("CSV data validation passed")
        return df
        
    except Exception as e:
        raise Failure(f"Failed to load or validate CSV: {str(e)}")

# Op 3: Generate dashboard
@op(
    ins={"sales_data": In(pd.DataFrame)},
    required_resource_keys={"dashboard_output_path"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"task_type": "dashboard_generation"},
)
def generate_dashboard(context, sales_data):
    """
    Generates executive dashboard with revenue trends, product performance, and regional analysis.
    """
    output_path = context.resources.dashboard_output_path
    
    try:
        # Simulate dashboard generation
        # In reality, this would use libraries like matplotlib, plotly, etc.
        logger.info("Generating revenue trends visualization...")
        logger.info("Generating product performance metrics...")
        logger.info("Generating regional analysis...")
        
        # Create a simple HTML dashboard stub
        html_content = f"""
        <html>
        <head><title>Executive Sales Dashboard</title></head>
        <body>
            <h1>Executive Sales Dashboard</h1>
            <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <h2>Data Summary</h2>
            <p>Total Records: {len(sales_data)}</p>
            <p>Total Revenue: ${sales_data['revenue'].sum():,.2f}</p>
            <h2>Revenue Trends</h2>
            <p>[Chart would be here]</p>
            <h2>Product Performance</h2>
            <p>[Metrics would be here]</p>
            <h2>Regional Analysis</h2>
            <p>[Analysis would be here]</p>
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Dashboard generated successfully at {output_path}")
        return output_path
        
    except Exception as e:
        raise Failure(f"Failed to generate dashboard: {str(e)}")

# Define the job
@job(
    tags={"pipeline_type": "executive_dashboard"},
    description="Executive sales dashboard pipeline with sensor-gated execution",
)
def executive_sales_dashboard_job():
    """
    Linear pipeline: wait → load → generate
    """
    # Linear dependency chain
    upstream_complete = wait_for_sales_aggregation()
    sales_data = load_sales_csv(upstream_complete)
    generate_dashboard(sales_data)

# Schedule for daily execution
daily_schedule = ScheduleDefinition(
    job=executive_sales_dashboard_job,
    cron_schedule="0 6 * * *",  # Run daily at 6 AM (adjust as needed)
    execution_timezone="UTC",
    tags={"schedule_type": "daily"},
)

# Optional: Email notification on failure (stub)
# In production, use dagster-slack or custom hooks
# @failure_hook(required_resource_keys={"email_config"})
# def email_notification(context):
#     logger.error(f"Job failed: {context.job_name}")
#     # Send email logic here

# Definitions object to tie everything together
defs = Definitions(
    jobs=[executive_sales_dashboard_job],
    schedules=[daily_schedule],
    resources={
        "sales_csv_path": sales_csv_path,
        "dashboard_output_path": dashboard_output_path,
        "upstream_dag_info": upstream_dag_info,
    },
)

# Launch pattern
if __name__ == "__main__":
    # Execute the job in-process for testing
    result = executive_sales_dashboard_job.execute_in_process()
    if result.success:
        logger.info("Job executed successfully!")
    else:
        logger.error("Job failed!")