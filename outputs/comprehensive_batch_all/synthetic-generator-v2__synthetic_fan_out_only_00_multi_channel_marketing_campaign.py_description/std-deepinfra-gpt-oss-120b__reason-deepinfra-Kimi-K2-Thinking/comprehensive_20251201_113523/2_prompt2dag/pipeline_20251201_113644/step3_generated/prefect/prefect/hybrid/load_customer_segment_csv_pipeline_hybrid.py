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
    """
    Prefect flow implementing a fan‑out pattern:
    1. Load the customer segment CSV.
    2. In parallel, send email, SMS, and push notifications.
    """
    # Entry task
    load_task = load_customer_segment_csv()

    # Fan‑out tasks that depend on the load task
    send_email_campaign(wait_for=[load_task])
    send_sms_campaign(wait_for=[load_task])
    send_push_notification(wait_for=[load_task])


if __name__ == "__main__":
    load_customer_segment_csv_pipeline()