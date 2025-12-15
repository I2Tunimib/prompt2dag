from dagster import op, job, resource, RetryPolicy, Failure, success_hook, failure_hook

# Resources
@resource
def email_notifications():
    def send_email(subject, message):
        # Simplified email sending function
        print(f"Sending email: {subject} - {message}")

    return send_email

# Hooks
@success_hook(required_resource_keys={"email_notifications"})
def notify_success(context):
    context.resources.email_notifications.send_email(
        "Pipeline Success", f"Pipeline {context.pipeline_name} completed successfully."
    )

@failure_hook(required_resource_keys={"email_notifications"})
def notify_failure(context):
    context.resources.email_notifications.send_email(
        "Pipeline Failure", f"Pipeline {context.pipeline_name} failed."
    )

# Ops
@op
def wait_for_sales_aggregation(context):
    """Simulates waiting for an external task to complete."""
    # Simulate waiting for the external task
    context.log.info("Waiting for daily_sales_aggregation to complete...")
    # In a real scenario, this would involve checking the status of the external task
    # For simplicity, we assume it completes successfully after a delay
    import time
    time.sleep(60)  # Simulate 60-second polling interval
    context.log.info("daily_sales_aggregation completed successfully.")

@op
def load_sales_csv(context):
    """Loads and validates the aggregated sales CSV data."""
    context.log.info("Loading and validating sales CSV data...")
    # Simulate loading and validating CSV data
    # In a real scenario, this would involve reading from a file or data store
    # For simplicity, we assume the data is valid
    context.log.info("Sales CSV data loaded and validated successfully.")

@op
def generate_dashboard(context):
    """Generates the executive dashboard with key sales metrics and visualizations."""
    context.log.info("Generating executive dashboard...")
    # Simulate generating the dashboard
    # In a real scenario, this would involve creating visualizations and metrics
    # For simplicity, we assume the dashboard is generated successfully
    context.log.info("Executive dashboard generated successfully.")

# Job
@job(
    resource_defs={"email_notifications": email_notifications},
    hooks=[notify_success, notify_failure],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def executive_sales_dashboard_job():
    sales_aggregation_complete = wait_for_sales_aggregation()
    sales_data_loaded = load_sales_csv(sales_aggregation_complete)
    generate_dashboard(sales_data_loaded)

# Launch pattern
if __name__ == '__main__':
    result = executive_sales_dashboard_job.execute_in_process()