from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name="load_customer_segment_csv", retries=2)
def load_customer_segment_csv():
    """Task: Load Customer Segment CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="send_email_campaign", retries=2)
def send_email_campaign():
    """Task: Send Email Campaign"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="send_push_notification", retries=2)
def send_push_notification():
    """Task: Send Push Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="send_sms_campaign", retries=2)
def send_sms_campaign():
    """Task: Send SMS Campaign"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="multi_channel_marketing_campaign",
    task_runner=ConcurrentTaskRunner(),
)
def multi_channel_marketing_campaign():
    """Orchestrates a multi‑channel marketing campaign."""
    # Entry task
    segment_future = load_customer_segment_csv.submit()

    # Fan‑out: run the three campaign tasks in parallel, all depending on the segment load
    email_future = send_email_campaign.submit(wait_for=[segment_future])
    sms_future = send_sms_campaign.submit(wait_for=[segment_future])
    push_future = send_push_notification.submit(wait_for=[segment_future])

    # Optionally wait for all to complete before finishing the flow
    email_future.result()
    sms_future.result()
    push_future.result()


if __name__ == "__main__":
    multi_channel_marketing_campaign()