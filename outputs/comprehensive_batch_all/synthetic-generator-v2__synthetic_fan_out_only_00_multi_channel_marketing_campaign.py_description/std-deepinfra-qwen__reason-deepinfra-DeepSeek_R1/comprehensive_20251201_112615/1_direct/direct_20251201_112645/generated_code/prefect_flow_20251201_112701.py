from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
import pandas as pd

@task
def load_customer_segment_csv(file_path: str) -> pd.DataFrame:
    """Loads customer segment data from a CSV file."""
    return pd.read_csv(file_path)

@task
def send_email_campaign(customer_data: pd.DataFrame) -> None:
    """Sends email campaign to premium customer segment."""
    logger = get_run_logger()
    logger.info("Sending email campaign to premium customers.")
    # Simulate sending emails
    for index, row in customer_data.iterrows():
        if row['segment'] == 'premium':
            logger.info(f"Email sent to {row['email']}")

@task
def send_sms_campaign(customer_data: pd.DataFrame) -> None:
    """Sends SMS campaign to customer segment."""
    logger = get_run_logger()
    logger.info("Sending SMS campaign to customers.")
    # Simulate sending SMS
    for index, row in customer_data.iterrows():
        logger.info(f"SMS sent to {row['phone']}")

@task
def send_push_notification(customer_data: pd.DataFrame) -> None:
    """Sends push notification campaign to mobile app users."""
    logger = get_run_logger()
    logger.info("Sending push notifications to mobile app users.")
    # Simulate sending push notifications
    for index, row in customer_data.iterrows():
        logger.info(f"Push notification sent to {row['device_token']}")

@flow
def marketing_campaign_pipeline(file_path: str):
    """Orchestrates the marketing campaign pipeline."""
    customer_data = load_customer_segment_csv(file_path)
    
    # Submit parallel tasks
    email_task = send_email_campaign.submit(customer_data)
    sms_task = send_sms_campaign.submit(customer_data)
    push_task = send_push_notification.submit(customer_data)
    
    # Wait for all tasks to complete
    email_task.result()
    sms_task.result()
    push_task.result()

if __name__ == '__main__':
    # Schedule: Daily execution
    # Start Date: January 1, 2024
    # Catchup: Disabled
    # Retry Policy: 2 retries with 5-minute delay between attempts
    # Owner: Marketing team
    # Pattern: Simple fan-out (no fan-in)
    # Parallelism: Maximum 3 tasks running concurrently
    deployment = Deployment.build_from_flow(
        flow=marketing_campaign_pipeline,
        name="Daily Marketing Campaign",
        schedule=CronSchedule(cron="0 0 * * *", timezone="UTC", start_date="2024-01-01"),
        work_queue_name="default",
        parameters={"file_path": "customer_segments.csv"},
        retries=2,
        retry_delay_seconds=300,
    )
    deployment.apply()

    # For local execution
    marketing_campaign_pipeline(file_path="customer_segments.csv")