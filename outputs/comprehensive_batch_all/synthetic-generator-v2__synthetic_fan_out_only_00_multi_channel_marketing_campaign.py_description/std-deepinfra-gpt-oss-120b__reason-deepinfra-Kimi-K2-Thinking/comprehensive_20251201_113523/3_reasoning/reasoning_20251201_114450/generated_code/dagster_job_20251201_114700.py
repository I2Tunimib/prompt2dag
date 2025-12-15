from dagster import (
    op,
    job,
    schedule,
    ScheduleEvaluationContext,
    RunRequest,
    RetryPolicy,
    DefaultScheduleStatus,
    Definitions,
    In,
    Out,
    Nothing,
)
import pandas as pd
from datetime import datetime
import time
import logging

# Set up logging
logger = logging.getLogger(__name__)


@op(
    out={"customer_data": Out(pd.DataFrame)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
    tags={"owner": "marketing_team"}
)
def load_customer_segment_csv(context):
    """
    Loads customer segment data from a CSV file.
    
    In a real scenario, this would read from an actual CSV file.
    For demonstration, we'll create sample data.
    """
    # Simulate loading from CSV - in production, use pd.read_csv()
    sample_data = {
        "customer_id": [1, 2, 3, 4, 5, 6],
        "segment": ["premium", "standard", "premium", "mobile", "premium", "mobile"],
        "email": ["user1@example.com", "user2@example.com", "user3@example.com", 
                  "user4@example.com", "user5@example.com", "user6@example.com"],
        "phone": ["123-456-7890", "234-567-8901", "345-678-9012",
                  "456-789-0123", "567-890-1234", "678-901-2345"],
        "mobile_app_user": [False, False, True, True, False, True]
    }
    
    df = pd.DataFrame(sample_data)
    context.log.info(f"Loaded {len(df)} customer records")
    return df


@op(
    ins={"customer_data": In(pd.DataFrame)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing_team"}
)
def send_email_campaign(context, customer_data):
    """
    Sends email campaign to premium customer segment.
    """
    # Filter for premium segment
    premium_customers = customer_data[customer_data["segment"] == "premium"]
    
    if premium_customers.empty:
        context.log.info("No premium customers found for email campaign")
        return
    
    # Simulate sending emails
    context.log.info(f"Sending email campaign to {len(premium_customers)} premium customers")
    for _, customer in premium_customers.iterrows():
        context.log.info(f"  - Email sent to: {customer['email']} (Customer ID: {customer['customer_id']})")
        # Simulate API call
        time.sleep(0.1)
    
    context.log.info("Email campaign completed successfully")


@op(
    ins={"customer_data": In(pd.DataFrame)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing_team"}
)
def send_sms_campaign(context, customer_data):
    """
    Sends SMS campaign to customer segment.
    """
    # For SMS, we'll target all customers with phone numbers
    customers_with_phone = customer_data.dropna(subset=["phone"])
    
    if customers_with_phone.empty:
        context.log.info("No customers with phone numbers found for SMS campaign")
        return
    
    # Simulate sending SMS
    context.log.info(f"Sending SMS campaign to {len(customers_with_phone)} customers")
    for _, customer in customers_with_phone.iterrows():
        context.log.info(f"  - SMS sent to: {customer['phone']} (Customer ID: {customer['customer_id']})")
        # Simulate API call
        time.sleep(0.1)
    
    context.log.info("SMS campaign completed successfully")


@op(
    ins={"customer_data": In(pd.DataFrame)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing_team"}
)
def send_push_notification(context, customer_data):
    """
    Sends push notification campaign to mobile app users.
    """
    # Filter for mobile app users
    mobile_users = customer_data[customer_data["mobile_app_user"] == True]
    
    if mobile_users.empty:
        context.log.info("No mobile app users found for push notification campaign")
        return
    
    # Simulate sending push notifications
    context.log.info(f"Sending push notifications to {len(mobile_users)} mobile app users")
    for _, customer in mobile_users.iterrows():
        context.log.info(f"  - Push notification sent to Customer ID: {customer['customer_id']}")
        # Simulate API call
        time.sleep(0.1)
    
    context.log.info("Push notification campaign completed successfully")


@job(
    tags={"owner": "marketing_team"},
    description="Multi-channel marketing campaign pipeline with fan-out pattern"
)
def marketing_campaign_pipeline():
    """
    Defines the marketing campaign pipeline with fan-out pattern.
    """
    # Load customer data
    customer_data = load_customer_segment_csv()
    
    # Three parallel campaigns that depend on the data loading
    send_email_campaign(customer_data)
    send_sms_campaign(customer_data)
    send_push_notification(customer_data)


@schedule(
    cron_schedule="0 9 * * *",  # Daily at 9 AM
    job=marketing_campaign_pipeline,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_marketing_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule for marketing campaign pipeline.
    Starts on January 1, 2024, with catchup disabled.
    """
    scheduled_date = context.scheduled_execution_time
    
    # Check if the scheduled date is before the start date
    start_date = datetime(2024, 1, 1)
    if scheduled_date < start_date:
        return None
    
    return RunRequest(
        run_key=f"marketing_campaign_{scheduled_date.isoformat()}",
        tags={"scheduled_date": scheduled_date.isoformat()}
    )


# Define the Dagster repository/definitions
defs = Definitions(
    jobs=[marketing_campaign_pipeline],
    schedules=[daily_marketing_schedule],
)


if __name__ == "__main__":
    # Execute the pipeline directly
    result = marketing_campaign_pipeline.execute_in_process()
    
    if result.success:
        print("Pipeline executed successfully!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")