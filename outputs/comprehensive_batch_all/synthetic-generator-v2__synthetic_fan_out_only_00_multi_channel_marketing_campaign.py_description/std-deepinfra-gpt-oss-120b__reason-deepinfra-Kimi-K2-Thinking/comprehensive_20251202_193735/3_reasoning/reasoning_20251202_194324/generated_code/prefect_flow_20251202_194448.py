import csv
from typing import List, Dict, Any
from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def load_customer_segment_csv(file_path: str) -> List[Dict[str, Any]]:
    """
    Loads customer segment data from a CSV file.
    
    Expected CSV format: customer_id, segment, contact_info, email, phone, device_token, app_user
    """
    customers = []
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                customers.append(row)
    except FileNotFoundError:
        print(f"Warning: File {file_path} not found. Returning empty customer list.")
        return []
    
    return customers


@task(retries=2, retry_delay_seconds=300)
def send_email_campaign(customers: List[Dict[str, Any]]) -> None:
    """
    Sends email campaign to premium customer segment.
    """
    premium_customers = [c for c in customers if c.get('segment') == 'premium']
    
    if not premium_customers:
        print("No premium customers found for email campaign.")
        return
    
    print(f"Sending email campaign to {len(premium_customers)} premium customers...")
    for customer in premium_customers:
        email = customer.get('email', customer.get('contact_info', 'unknown'))
        print(f"  - Email sent to {customer.get('customer_id', 'unknown')} ({email})")
    
    print("Email campaign completed.")


@task(retries=2, retry_delay_seconds=300)
def send_sms_campaign(customers: List[Dict[str, Any]]) -> None:
    """
    Sends SMS campaign to customers with phone numbers.
    """
    sms_customers = [c for c in customers if c.get('phone')]
    
    if not sms_customers:
        print("No customers with phone numbers found for SMS campaign.")
        return
    
    print(f"Sending SMS campaign to {len(sms_customers)} customers...")
    for customer in sms_customers:
        phone = customer.get('phone', 'unknown')
        print(f"  - SMS sent to {customer.get('customer_id', 'unknown')} ({phone})")
    
    print("SMS campaign completed.")


@task(retries=2, retry_delay_seconds=300)
def send_push_notification(customers: List[Dict[str, Any]]) -> None:
    """
    Sends push notification campaign to mobile app users.
    """
    push_customers = [c for c in customers if c.get('device_token') or c.get('app_user') == 'true']
    
    if not push_customers:
        print("No mobile app users found for push notification campaign.")
        return
    
    print(f"Sending push notification campaign to {len(push_customers)} mobile app users...")
    for customer in push_customers:
        device_token = customer.get('device_token', 'unknown')
        print(f"  - Push notification sent to {customer.get('customer_id', 'unknown')} ({device_token})")
    
    print("Push notification campaign completed.")


@flow(name="multi-channel-marketing-campaign")
def marketing_campaign_flow(csv_path: str = "customer_segments.csv") -> None:
    """
    Multi-channel marketing campaign flow with sequential data loading
    followed by three parallel campaign executions.
    
    Deployment schedule config (run separately):
    - Daily at midnight starting 2024-01-01, no catchup
    - Command: prefect deployment build python_file.py:marketing_campaign_flow --name "daily-marketing-campaign" --schedule "0 0 * * *" --start-date "2024-01-01T00:00:00" --no-catchup
    """
    # Load customer data sequentially
    customers = load_customer_segment_csv(csv_path)
    
    # Submit three marketing campaigns to run in parallel
    # Each campaign depends on successful completion of data loading
    email_task = send_email_campaign.submit(customers)
    sms_task = send_sms_campaign.submit(customers)
    push_task = send_push_notification.submit(customers)
    
    # Flow implicitly waits for all submitted tasks to complete
    # No fan-in pattern - tasks complete independently


if __name__ == '__main__':
    marketing_campaign_flow()