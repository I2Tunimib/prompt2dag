import csv
import time
from typing import List, Dict, Any
from prefect import flow, task


@task
def load_customer_segment_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Load customer segment data from a CSV file."""
    customers = []
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                customers.append(row)
    except FileNotFoundError:
        print(f"Warning: CSV file not found at {csv_path}")
        return []
    return customers


@task
def send_email_campaign(customer_data: List[Dict[str, Any]]) -> None:
    """Send email campaign to premium customer segment."""
    premium_customers = [
        c for c in customer_data
        if c.get('segment', '').lower() == 'premium' and c.get('email')
    ]
    print(f"Sending email campaign to {len(premium_customers)} premium customers...")
    time.sleep(2)
    print("Email campaign completed.")


@task
def send_sms_campaign(customer_data: List[Dict[str, Any]]) -> None:
    """Send SMS campaign to customers with phone numbers."""
    sms_customers = [c for c in customer_data if c.get('phone')]
    print(f"Sending SMS campaign to {len(sms_customers)} customers...")
    time.sleep(2)
    print("SMS campaign completed.")


@task
def send_push_notification(customer_data: List[Dict[str, Any]]) -> None:
    """Send push notification campaign to mobile app users."""
    push_customers = [c for c in customer_data if c.get('device_token')]
    print(f"Sending push notifications to {len(push_customers)} mobile users...")
    time.sleep(2)
    print("Push notification campaign completed.")


# Schedule: Daily at midnight starting 2024-01-01, no catchup
# Deployment: prefect deployment build --name marketing-campaign \
#   --cron "0 0 * * *" --start-date "2024-01-01T00:00:00" \
#   marketing_campaign_flow:marketing_campaign_flow
@flow(
    name="multi-channel-marketing-campaign",
    retries=2,
    retry_delay_seconds=300
)
def marketing_campaign_flow(csv_path: str = "customer_segments.csv") -> None:
    """
    Multi-channel marketing campaign flow.
    
    Loads customer segments from CSV and triggers parallel campaigns
    across email, SMS, and push notification channels.
    """
    customer_data = load_customer_segment_csv(csv_path)
    
    email_task = send_email_campaign.submit(customer_data)
    sms_task = send_sms_campaign.submit(customer_data)
    push_task = send_push_notification.submit(customer_data)


if __name__ == '__main__':
    marketing_campaign_flow()