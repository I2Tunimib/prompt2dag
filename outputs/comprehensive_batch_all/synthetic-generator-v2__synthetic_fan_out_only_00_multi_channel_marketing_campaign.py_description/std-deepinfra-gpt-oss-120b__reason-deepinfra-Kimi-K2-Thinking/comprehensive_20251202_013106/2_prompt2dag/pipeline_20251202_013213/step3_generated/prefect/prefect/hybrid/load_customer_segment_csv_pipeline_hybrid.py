from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='load_customer_segment_csv', retries=2)
def load_customer_segment_csv():
    """Task: Load Customer Segment CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='send_email_campaign', retries=2)
def send_email_campaign():
    """Task: Send Email Campaign"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='send_push_notification', retries=2)
def send_push_notification():
    """Task: Send Push Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='send_sms_campaign', retries=2)
def send_sms_campaign():
    """Task: Send SMS Campaign"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="load_customer_segment_csv_pipeline",
    task_runner=ConcurrentTaskRunner()
)
def load_customer_segment_csv_pipeline():
    """Main pipeline flow implementing a fan‑out pattern."""
    # Entry task
    load_customer_segment_csv()

    # Fan‑out: run the three campaign tasks in parallel
    email_future = send_email_campaign.submit()
    sms_future = send_sms_campaign.submit()
    push_future = send_push_notification.submit()

    # Optionally wait for all to complete
    email_future.result()
    sms_future.result()
    push_future.result()


if __name__ == "__main__":
    load_customer_segment_csv_pipeline()