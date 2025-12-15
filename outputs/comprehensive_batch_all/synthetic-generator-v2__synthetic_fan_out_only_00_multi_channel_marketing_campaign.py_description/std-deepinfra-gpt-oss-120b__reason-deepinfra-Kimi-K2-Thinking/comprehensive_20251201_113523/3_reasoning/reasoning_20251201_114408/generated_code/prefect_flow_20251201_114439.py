from prefect import flow, task
import pandas as pd
from typing import Dict, Any
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Loads customer segment data from a CSV file"
)
def load_customer_segment_csv(file_path: str = "customer_segments.csv") -> pd.DataFrame:
    """
    Loads customer segment data from a CSV file containing customer IDs,
    segments, and contact information.
    """
    try:
        logger.info(f"Loading customer segment data from {file_path}")
        # Simulate CSV loading with pandas
        # In production, this would read from actual file path
        df = pd.DataFrame({
            'customer_id': range(1, 101),
            'segment': ['premium'] * 30 + ['standard'] * 70,
            'email': [f"user{i}@example.com" for i in range(1, 101)],
            'phone': [f"+123456789{i:02d}" for i in range(1, 101)],
            'mobile_app_user': [True] * 50 + [False] * 50
        })
        logger.info(f"Successfully loaded {len(df)} customer records")
        return df
    except Exception as e:
        logger.error(f"Failed to load customer segment data: {e}")
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Sends email campaign to premium customer segment"
)
def send_email_campaign(customer_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Sends email campaign to premium customer segment.
    """
    try:
        logger.info("Starting email campaign for premium segment")
        premium_customers = customer_data[customer_data['segment'] == 'premium']
        
        # Simulate email sending
        time.sleep(2)
        success_count = len(premium_customers)
        
        logger.info(f"Email campaign sent to {success_count} premium customers")
        return {
            "channel": "email",
            "status": "success",
            "recipients": success_count,
            "segment": "premium"
        }
    except Exception as e:
        logger.error(f"Email campaign failed: {e}")
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Sends SMS campaign to customer segment"
)
def send_sms_campaign(customer_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Sends SMS campaign to customer segment.
    """
    try:
        logger.info("Starting SMS campaign")
        # Simulate SMS sending
        time.sleep(2)
        success_count = len(customer_data)
        
        logger.info(f"SMS campaign sent to {success_count} customers")
        return {
            "channel": "sms",
            "status": "success",
            "recipients": success_count,
            "segment": "all"
        }
    except Exception as e:
        logger.error(f"SMS campaign failed: {e}")
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Sends push notification campaign to mobile app users"
)
def send_push_notification(customer_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Sends push notification campaign to mobile app users.
    """
    try:
        logger.info("Starting push notification campaign for mobile app users")
        mobile_users = customer_data[customer_data['mobile_app_user'] == True]
        
        # Simulate push notification sending
        time.sleep(2)
        success_count = len(mobile_users)
        
        logger.info(f"Push notification campaign sent to {success_count} mobile users")
        return {
            "channel": "push",
            "status": "success",
            "recipients": success_count,
            "segment": "mobile_app_users"
        }
    except Exception as e:
        logger.error(f"Push notification campaign failed: {e}")
        raise


@flow(
    name="multi-channel-marketing-campaign",
    description="Daily marketing campaign with fan-out pattern across email, SMS, and push channels"
)
def marketing_campaign_flow(csv_path: str = "customer_segments.csv"):
    """
    Orchestrates the multi-channel marketing campaign DAG.
    
    Flow pattern:
    1. Load customer segment data (sequential)
    2. Execute three parallel marketing campaigns (fan-out)
    """
    # Step 1: Load customer data sequentially
    customer_data = load_customer_segment_csv(csv_path)
    
    # Step 2: Submit parallel campaign tasks
    # All three campaigns run concurrently after data loading completes
    email_result = send_email_campaign.submit(customer_data)
    sms_result = send_sms_campaign.submit(customer_data)
    push_result = send_push_notification.submit(customer_data)
    
    # Wait for all campaigns to complete (implicit fan-out, no fan-in required)
    results = {
        "email_campaign": email_result.result(),
        "sms_campaign": sms_result.result(),
        "push_campaign": push_result.result()
    }
    
    logger.info(f"All marketing campaigns completed: {results}")
    return results


if __name__ == "__main__":
    # For local execution and testing
    # To deploy with schedule, use:
    # prefect deployment build marketing_campaign_flow:marketing_campaign_flow \
    #   --name "daily-marketing-campaign" \
    #   --schedule "0 9 * * *" \
    #   --start-date "2024-01-01T00:00:00" \
    #   --timezone "UTC" \
    #   --apply
    marketing_campaign_flow()