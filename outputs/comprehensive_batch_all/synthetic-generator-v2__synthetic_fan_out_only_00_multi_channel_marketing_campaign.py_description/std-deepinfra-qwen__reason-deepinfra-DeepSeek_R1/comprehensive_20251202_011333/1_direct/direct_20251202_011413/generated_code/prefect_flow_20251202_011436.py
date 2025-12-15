from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import pandas as pd

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def load_customer_segment_csv(file_path: str) -> pd.DataFrame:
    """
    Load customer segment data from a CSV file.
    """
    logger = get_run_logger()
    logger.info(f"Loading customer segment data from {file_path}")
    return pd.read_csv(file_path)

@task(retries=2, retry_delay_seconds=300)
def send_email_campaign(customer_data: pd.DataFrame):
    """
    Send email campaign to premium customer segment.
    """
    logger = get_run_logger()
    logger.info("Sending email campaign to premium customers")
    # Simulate sending emails
    for index, row in customer_data.iterrows():
        if row['segment'] == 'premium':
            logger.info(f"Email sent to {row['email']}")

@task(retries=2, retry_delay_seconds=300)
def send_sms_campaign(customer_data: pd.DataFrame):
    """
    Send SMS campaign to customer segment.
    """
    logger = get_run_logger()
    logger.info("Sending SMS campaign to customers")
    # Simulate sending SMS
    for index, row in customer_data.iterrows():
        logger.info(f"SMS sent to {row['phone']}")

@task(retries=2, retry_delay_seconds=300)
def send_push_notification(customer_data: pd.DataFrame):
    """
    Send push notification campaign to mobile app users.
    """
    logger = get_run_logger()
    logger.info("Sending push notifications to mobile app users")
    # Simulate sending push notifications
    for index, row in customer_data.iterrows():
        logger.info(f"Push notification sent to {row['device_token']}")

@flow(name="Multi-Channel Marketing Campaign", retries=2, retry_delay_seconds=300)
def marketing_campaign_flow(file_path: str):
    """
    Orchestrate the multi-channel marketing campaign pipeline.
    """
    logger = get_run_logger()
    logger.info("Starting multi-channel marketing campaign pipeline")

    # Load customer segment data
    customer_data = load_customer_segment_csv(file_path)

    # Trigger parallel marketing campaigns
    email_task = send_email_campaign.submit(customer_data)
    sms_task = send_sms_campaign.submit(customer_data)
    push_task = send_push_notification.submit(customer_data)

    # Wait for all tasks to complete
    email_task.result()
    sms_task.result()
    push_task.result()

    logger.info("Multi-channel marketing campaign pipeline completed")

if __name__ == '__main__':
    # Example file path
    file_path = "customer_segments.csv"
    marketing_campaign_flow(file_path)

# Deployment/Schedule Configuration (optional)
# Schedule: Daily execution
# Start Date: January 1, 2024
# Catchup: Disabled
# Retry Policy: 2 retries with 5-minute delay between attempts
# Owner: Marketing team
# Pattern: Simple fan-out (no fan-in)
# Parallelism: Maximum 3 tasks running concurrently
# Task Dependencies: Sequential start followed by parallel execution