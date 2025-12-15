from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Dict

from prefect import flow, task


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Load customer segment data from a CSV file.",
)
def load_customer_segment_csv(file_path: str) -> List[Dict[str, str]]:
    """
    Read a CSV file containing customer IDs, segments, and contact information.

    Args:
        file_path: Path to the CSV file.

    Returns:
        A list of dictionaries, each representing a customer record.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    records: List[Dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            records.append(row)
    return records


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Send email campaign to premium customers.",
)
def send_email_campaign(customers: List[Dict[str, str]]) -> None:
    """
    Send an email campaign to customers in the premium segment.

    Args:
        customers: List of customer records loaded from CSV.
    """
    premium_customers = [c for c in customers if c.get("segment") == "premium"]
    for customer in premium_customers:
        email = customer.get("email")
        if email:
            # Insert real email sending logic here.
            pass  # placeholder for email API call


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Send SMS campaign to the target segment.",
)
def send_sms_campaign(customers: List[Dict[str, str]]) -> None:
    """
    Send an SMS campaign to the appropriate customer segment.

    Args:
        customers: List of customer records loaded from CSV.
    """
    for customer in customers:
        phone = customer.get("phone")
        if phone:
            # Insert real SMS sending logic here.
            pass  # placeholder for SMS API call


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Send push notification campaign to mobile app users.",
)
def send_push_notification(customers: List[Dict[str, str]]) -> None:
    """
    Send push notifications to customers with a registered device token.

    Args:
        customers: List of customer records loaded from CSV.
    """
    for customer in customers:
        device_token = customer.get("device_token")
        if device_token:
            # Insert real push notification logic here.
            pass  # placeholder for push notification API call


@flow(name="daily_marketing_campaign")
def marketing_campaign_flow(csv_path: str = "customer_segments.csv") -> None:
    """
    Orchestrates the daily marketing campaign workflow.

    The flow loads customer data and then triggers three parallel
    marketing campaigns: email, SMS, and push notifications.

    Schedule (to be configured in deployment):
        - Daily execution starting 2024-01-01
        - No catch‑up
        - Owner: Marketing team
    """
    customers = load_customer_segment_csv(csv_path)

    # Run campaign tasks in parallel using .submit()
    email_future = send_email_campaign.submit(customers)
    sms_future = send_sms_campaign.submit(customers)
    push_future = send_push_notification.submit(customers)

    # Wait for all parallel tasks to finish
    email_future.result()
    sms_future.result()
    push_future.result()


if __name__ == "__main__":
    marketing_campaign_flow()