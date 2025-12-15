from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import List, Dict

from dagster import (
    Config,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    JobDefinition,
    OpExecutionContext,
    op,
    job,
    thread_pool_executor,
)

logger = logging.getLogger(__name__)


class LoadCsvConfig(Config):
    """Configuration schema for loading the customer segment CSV."""
    file_path: str = "customer_segments.csv"


@op(
    name="load_customer_segment_csv",
    description="Loads customer segment data from a CSV file.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing"},
    config_schema=LoadCsvConfig,
)
def load_customer_segment_csv(context: OpExecutionContext, config: LoadCsvConfig) -> List[Dict[str, str]]:
    """Read a CSV file and return a list of rows as dictionaries."""
    file_path = Path(config.file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    logger.info("Loading customer segment data from %s", file_path)
    with file_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]

    logger.info("Loaded %d customer records", len(data))
    return data


def _filter_premium(customers: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Utility to filter premium customers for the email campaign."""
    return [c for c in customers if c.get("segment", "").lower() == "premium"]


@op(
    name="send_email_campaign",
    description="Sends email campaign to premium customer segment.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing"},
)
def send_email_campaign(context: OpExecutionContext, customers: List[Dict[str, str]]) -> None:
    premium_customers = _filter_premium(customers)
    logger.info("Sending email campaign to %d premium customers", len(premium_customers))
    # Placeholder for actual email sending logic.
    for customer in premium_customers:
        email = customer.get("email")
        if email:
            logger.debug("Email sent to %s", email)


@op(
    name="send_sms_campaign",
    description="Sends SMS campaign to all customers in the segment.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing"},
)
def send_sms_campaign(context: OpExecutionContext, customers: List[Dict[str, str]]) -> None:
    logger.info("Sending SMS campaign to %d customers", len(customers))
    # Placeholder for actual SMS sending logic.
    for customer in customers:
        phone = customer.get("phone")
        if phone:
            logger.debug("SMS sent to %s", phone)


@op(
    name="send_push_notification",
    description="Sends push notification campaign to mobile app users.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "marketing"},
)
def send_push_notification(context: OpExecutionContext, customers: List[Dict[str, str]]) -> None:
    logger.info("Sending push notifications to %d customers", len(customers))
    # Placeholder for actual push notification logic.
    for customer in customers:
        device_id = customer.get("device_id")
        if device_id:
            logger.debug("Push notification sent to device %s", device_id)


@job(
    name="marketing_campaign_job",
    description="Fan‑out marketing campaign job that loads customer data and triggers parallel campaigns.",
    executor_def=thread_pool_executor,
    tags={"owner": "marketing"},
)
def marketing_campaign_job():
    customers = load_customer_segment_csv()
    send_email_campaign(customers)
    send_sms_campaign(customers)
    send_push_notification(customers)


daily_marketing_schedule = ScheduleDefinition(
    job=marketing_campaign_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    tags={"owner": "marketing"},
    default_status=None,  # Disabled by default; enable manually if needed
)


defs = Definitions(
    jobs=[marketing_campaign_job],
    schedules=[daily_marketing_schedule],
)


if __name__ == "__main__":
    result = marketing_campaign_job.execute_in_process(
        run_config={"ops": {"load_customer_segment_csv": {"config": {"file_path": "customer_segments.csv"}}}}
    )
    if result.success:
        logger.info("Marketing campaign job completed successfully.")
    else:
        logger.error("Marketing campaign job failed.")