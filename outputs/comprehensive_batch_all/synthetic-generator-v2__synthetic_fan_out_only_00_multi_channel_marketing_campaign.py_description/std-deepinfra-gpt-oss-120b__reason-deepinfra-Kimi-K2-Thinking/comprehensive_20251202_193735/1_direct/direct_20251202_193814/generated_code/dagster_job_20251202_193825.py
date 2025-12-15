from datetime import datetime, timedelta

import pandas as pd
from dagster import (
    DefaultScheduleStatus,
    InProcessExecutor,
    RetryPolicy,
    ScheduleDefinition,
    JobDefinition,
    op,
    job,
    Config,
    ConfigurableResource,
    InitResourceContext,
    get_dagster_logger,
)


class CSVLoaderConfig(Config):
    csv_path: str = "customer_segments.csv"


@op(
    config_schema=CSVLoaderConfig,
    description="Loads customer segment data from a CSV file.",
    tags={"owner": "marketing"},
)
def load_customer_segment_csv(context) -> pd.DataFrame:
    """Read the CSV file containing customer IDs, segments, and contact information."""
    csv_path = context.op_config["csv_path"]
    logger = get_dagster_logger()
    logger.info(f"Loading customer segment data from %s", csv_path)
    try:
        df = pd.read_csv(csv_path)
        logger.info("Loaded %d rows.", len(df))
        return df
    except Exception as exc:
        logger.error("Failed to load CSV: %s", exc)
        raise


def _filter_premium(customers: pd.DataFrame) -> pd.DataFrame:
    """Utility to filter premium customers; placeholder implementation."""
    if "segment" in customers.columns:
        return customers[customers["segment"] == "premium"]
    return customers


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Sends email campaign to premium customer segment.",
    tags={"owner": "marketing"},
)
def send_email_campaign(context, customers: pd.DataFrame) -> None:
    premium_customers = _filter_premium(customers)
    logger = get_dagster_logger()
    logger.info("Sending email campaign to %d premium customers.", len(premium_customers))
    # Placeholder for actual email sending logic.
    for _, row in premium_customers.iterrows():
        logger.debug("Email sent to %s (ID: %s)", row.get("email", "unknown"), row.get("customer_id", "unknown"))


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Sends SMS campaign to the customer segment.",
    tags={"owner": "marketing"},
)
def send_sms_campaign(context, customers: pd.DataFrame) -> None:
    logger = get_dagster_logger()
    logger.info("Sending SMS campaign to %d customers.", len(customers))
    # Placeholder for actual SMS sending logic.
    for _, row in customers.iterrows():
        logger.debug("SMS sent to %s (ID: %s)", row.get("phone", "unknown"), row.get("customer_id", "unknown"))


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Sends push notification campaign to mobile app users.",
    tags={"owner": "marketing"},
)
def send_push_notification(context, customers: pd.DataFrame) -> None:
    logger = get_dagster_logger()
    logger.info("Sending push notifications to %d customers.", len(customers))
    # Placeholder for actual push notification logic.
    for _, row in customers.iterrows():
        logger.debug(
            "Push notification sent to %s (ID: %s)",
            row.get("device_id", "unknown"),
            row.get("customer_id", "unknown"),
        )


executor_def = InProcessExecutor.configured({"max_concurrency": 3})


@job(
    executor_def=executor_def,
    tags={"owner": "marketing"},
    description="Daily fan‑out marketing campaign job.",
)
def marketing_campaign_job():
    customers = load_customer_segment_csv()
    send_email_campaign(customers)
    send_sms_campaign(customers)
    send_push_notification(customers)


daily_marketing_schedule = ScheduleDefinition(
    job=marketing_campaign_job,
    cron_schedule="0 0 * * *",  # Every day at midnight UTC
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily execution of the marketing campaign job.",
)


if __name__ == "__main__":
    result = marketing_campaign_job.execute_in_process(
        run_config={
            "ops": {
                "load_customer_segment_csv": {
                    "config": {"csv_path": "customer_segments.csv"}
                }
            }
        }
    )
    if result.success:
        print("Marketing campaign job completed successfully.")
    else:
        print("Marketing campaign job failed.")