from dagster import (
    op,
    job,
    ScheduleDefinition,
    RetryPolicy,
    OpExecutionContext,
    Config,
    In,
    Out,
    get_dagster_logger,
    Definitions,
    DefaultScheduleStatus,
    multiprocess_executor,
)
import pandas as pd
from datetime import datetime
import os

logger = get_dagster_logger()


class CustomerDataConfig(Config):
    """Configuration for customer data loading."""
    csv_path: str = "data/customer_segments.csv"


class CampaignConfig(Config):
    """Base configuration for campaign ops."""
    dry_run: bool = True


@op(
    description="Loads customer segment data from a CSV file containing customer IDs, segments, and contact information.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out={"customer_data": Out(description="Loaded customer data")}
)
def load_customer_segment_csv(context: OpExecutionContext, config: CustomerDataConfig) -> dict:
    """Load customer segment data from CSV."""
    logger.info(f"Loading customer data from {config.csv_path}")
    
    os.makedirs(os.path.dirname(config.csv_path), exist_ok=True)
    
    if not os.path.exists(config.csv_path):
        logger.warning(f"CSV file not found at {config.csv_path}, creating sample data")
        sample_data = pd.DataFrame({
            "customer_id": [1, 2, 3, 4, 5, 6],
            "segment": ["premium", "standard", "premium", "mobile", "premium", "standard"],
            "email": ["user1@example.com", "user2@example.com", "user3@example.com", None, "user5@example.com", "user6@example.com"],
            "phone": ["123-456-7890", "234-567-8901", None, "456-789-0123", "567-890-1234", "678-901-2345"],
            "is_mobile_user": [False, True, False, True, True, False],
            "device_token": [None, "token123", None, "token456", "token789", None]
        })
        sample_data.to_csv(config.csv_path, index=False)
    
    try:
        df = pd.read_csv(config.csv_path)
        data = df.to_dict(orient="records")
        logger.info(f"Successfully loaded {len(data)} customer records")
        return {
            "records": data,
            "count": len(data),
            "columns": list(df.columns)
        }
    except Exception as e:
        logger.error(f"Failed to load customer data: {e}")
        raise


@op(
    description="Sends email campaign to premium customer segment.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    ins={"customer_data": In(description="Customer data from loader")}
)
def send_email_campaign(context: OpExecutionContext, customer_data: dict, config: CampaignConfig):
    """Send email campaign to premium customers."""
    logger.info("Starting email campaign for premium segment")
    
    premium_customers = [
        record for record in customer_data["records"]
        if record.get("segment", "").lower() == "premium" and pd.notna(record.get("email"))
    ]
    
    logger.info(f"Found {len(premium_customers)} premium customers with email")
    
    if config.dry_run:
        logger.info("DRY RUN: Would send emails to premium customers")
        for customer in premium_customers[:5]:
            logger.info(f"  - Email to: {customer.get('email')}")
    else:
        logger.info("Sending actual emails...")
    
    logger.info("Email campaign completed")


@op(
    description="Sends SMS campaign to customer segment.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    ins={"customer_data": In(description="Customer data from loader")}
)
def send_sms_campaign(context: OpExecutionContext, customer_data: dict, config: CampaignConfig):
    """Send SMS campaign to customers."""
    logger.info("Starting SMS campaign")
    
    sms_customers = [
        record for record in customer_data["records"]
        if pd.notna(record.get("phone"))
    ]
    
    logger.info(f"Found {len(sms_customers)} customers with phone numbers")
    
    if config.dry_run:
        logger.info("DRY RUN: Would send SMS to customers")
        for customer in sms_customers[:5]:
            logger.info(f"  - SMS to: {customer.get('phone')}")
    else:
        logger.info("Sending actual SMS...")
    
    logger.info("SMS campaign completed")


@op(
    description="Sends push notification campaign to mobile app users.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    ins={"customer_data": In(description="Customer data from loader")}
)
def send_push_notification(context: OpExecutionContext, customer_data: dict, config: CampaignConfig):
    """Send push notifications to mobile app users."""
    logger.info("Starting push notification campaign")
    
    push_customers = [
        record for record in customer_data["records"]
        if record.get("is_mobile_user", False) and pd.notna(record.get("device_token"))
    ]
    
    logger.info(f"Found {len(push_customers)} mobile app users")
    
    if config.dry_run:
        logger.info("DRY RUN: Would send push notifications")
        for customer in push_customers[:5]:
            logger.info(f"  - Push to device: {customer.get('device_token')}")
    else:
        logger.info("Sending actual push notifications...")
    
    logger.info("Push notification campaign completed")


@job(
    description="Multi-channel marketing campaign pipeline with fan-out pattern.",
    tags={"owner": "marketing_team", "pipeline_type": "marketing_campaign"},
    executor_def=multiprocess_executor
)
def multi_channel_marketing_campaign():
    """Define the marketing campaign job with fan-out pattern."""
    customer_data = load_customer_segment_csv()
    send_email_campaign(customer_data)
    send_sms_campaign(customer_data)
    send_push_notification(customer_data)


marketing_schedule = ScheduleDefinition(
    job=multi_channel_marketing_campaign,
    cron_schedule="0 9 * * *",
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"catchup": "disabled", "owner": "marketing_team"}
)


defs = Definitions(
    jobs=[multi_channel_marketing_campaign],
    schedules=[marketing_schedule]
)


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    
    result = multi_channel_marketing_campaign.execute_in_process(
        run_config={
            "ops": {
                "load_customer_segment_csv": {
                    "config": {
                        "csv_path": "data/customer_segments.csv"
                    }
                },
                "send_email_campaign": {
                    "config": {
                        "dry_run": True
                    }
                },
                "send_sms_campaign": {
                    "config": {
                        "dry_run": True
                    }
                },
                "send_push_notification": {
                    "config": {
                        "dry_run": True
                    }
                }
            },
            "execution": {
                "config": {
                    "multiprocess": {
                        "max_concurrent": 3
                    }
                }
            }
        }
    )
    
    if result.success:
        logger.info("Pipeline executed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step failed: {event.step_key}")