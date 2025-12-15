from dagster import op, job, resource, RetryPolicy, Failure, success_hook, failure_hook

# Resource for email notifications
@resource
def email_notifications_resource(context):
    def send_email(subject, message):
        # Placeholder for sending emails
        print(f"Sending email: {subject} - {message}")
    return send_email

# Success hook for notifications
@success_hook(required_resource_keys={"email_notifications"})
def notify_success(context):
    context.resources.email_notifications.send_email(
        subject="Pipeline Succeeded",
        message=f"Pipeline {context.pipeline_name} completed successfully."
    )

# Failure hook for notifications
@failure_hook(required_resource_keys={"email_notifications"})
def notify_failure(context):
    context.resources.email_notifications.send_email(
        subject="Pipeline Failed",
        message=f"Pipeline {context.pipeline_name} failed at step {context.step_key}."
    )

# Op to wait for the upstream daily sales aggregation to complete
@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"email_notifications"}
)
def wait_for_sales_aggregation(context):
    # Placeholder for the sensor logic
    context.log.info("Waiting for daily_sales_aggregation to complete...")
    # Simulate waiting for the upstream process
    import time
    time.sleep(60)  # Simulate 1-minute wait
    context.log.info("daily_sales_aggregation completed successfully.")

# Op to load the sales CSV data
@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"email_notifications"}
)
def load_sales_csv(context):
    # Placeholder for loading CSV data
    context.log.info("Loading sales CSV data...")
    # Simulate data loading
    import pandas as pd
    data = pd.DataFrame({
        "date": ["2023-10-01", "2023-10-02"],
        "revenue": [10000, 15000],
        "product": ["A", "B"],
        "region": ["North", "South"]
    })
    context.log.info("Sales CSV data loaded successfully.")
    return data

# Op to generate the executive dashboard
@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"email_notifications"}
)
def generate_dashboard(context, sales_data):
    # Placeholder for dashboard generation
    context.log.info("Generating executive dashboard...")
    # Simulate dashboard generation
    context.log.info("Executive dashboard generated successfully.")

# Define the job
@job(
    resource_defs={"email_notifications": email_notifications_resource},
    hooks=[notify_success, notify_failure],
    description="Executive sales dashboard pipeline that waits for daily sales aggregation, loads CSV data, and generates a dashboard."
)
def executive_sales_dashboard_job():
    sales_data = load_sales_csv(wait_for_sales_aggregation())
    generate_dashboard(sales_data)

# Launch pattern
if __name__ == '__main__':
    result = executive_sales_dashboard_job.execute_in_process()