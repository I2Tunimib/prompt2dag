from dagster import op, job, resource, RetryPolicy, Failure, success_hook, failure_hook

# Resource for Airflow Sensor
@resource
def airflow_sensor_resource():
    # Simplified resource for Airflow Sensor
    pass

# Success and Failure Hooks
@success_hook
def notify_success(context):
    print(f"Task {context.op.name} succeeded.")

@failure_hook
def notify_failure(context):
    print(f"Task {context.op.name} failed.")

# Op to wait for the daily sales aggregation to complete
@op(required_resource_keys={"airflow_sensor_resource"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_for_sales_aggregation(context):
    """
    Waits for the daily_sales_aggregation DAG to complete successfully.
    """
    # Simulate the sensor logic
    # In a real scenario, this would interact with Airflow's API
    context.log.info("Waiting for daily_sales_aggregation to complete...")
    # Simulate a successful completion
    context.log.info("daily_sales_aggregation completed successfully.")

# Op to load the sales CSV data
@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def load_sales_csv(context):
    """
    Loads and validates the aggregated sales CSV data.
    """
    # Simulate loading and validating CSV data
    context.log.info("Loading and validating sales CSV data...")
    # Simulate successful data load
    context.log.info("Sales CSV data loaded and validated successfully.")

# Op to generate the executive dashboard
@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def generate_dashboard(context):
    """
    Generates the executive dashboard with key sales metrics and visualizations.
    """
    # Simulate dashboard generation
    context.log.info("Generating executive dashboard...")
    # Simulate successful dashboard generation
    context.log.info("Executive dashboard generated successfully.")

# Define the job
@job(
    resource_defs={"airflow_sensor_resource": airflow_sensor_resource},
    hooks=[notify_success, notify_failure],
)
def executive_sales_dashboard_job():
    """
    Executive sales dashboard pipeline that waits for daily sales aggregation,
    loads the aggregated sales data, and generates an executive dashboard.
    """
    sales_aggregation_complete = wait_for_sales_aggregation()
    sales_data_loaded = load_sales_csv(sales_aggregation_complete)
    generate_dashboard(sales_data_loaded)

# Launch pattern
if __name__ == "__main__":
    result = executive_sales_dashboard_job.execute_in_process()