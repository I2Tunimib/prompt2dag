from pathlib import Path
from typing import Any

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def load_customer_segment_csv(csv_path: str) -> pd.DataFrame:
    """Load customer segment data from a CSV file.

    Args:
        csv_path: Path to the CSV file containing customer IDs, segments,
            and contact information.

    Returns:
        DataFrame with the loaded customer data.
    """
    logger = get_run_logger()
    logger.info("Loading customer segment data from %s", csv_path)

    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(path)
    logger.info("Loaded %d records", len(df))
    return df


@task(
    retries=2,
    retry_delay_seconds=300,
)
def send_email_campaign(customer_data: pd.DataFrame) -> None:
    """Send email campaign to the premium customer segment.

    Args:
        customer_data: DataFrame containing customer information.
    """
    logger = get_run_logger()
    premium_customers = customer_data[customer_data["segment"] == "premium"]
    logger.info(
        "Sending email campaign to %d premium customers", len(premium_customers)
    )
    # Placeholder for actual email sending logic
    # e.g., email_service.send_bulk(premium_customers["email"])
    logger.info("Email campaign completed")


@task(
    retries=2,
    retry_delay_seconds=300,
)
def send_sms_campaign(customer_data: pd.DataFrame) -> None:
    """Send SMS campaign to the target customer segment.

    Args:
        customer_data: DataFrame containing customer information.
    """
    logger = get_run_logger()
    sms_customers = customer_data[customer_data["segment"] != "opt_out"]
    logger.info("Sending SMS campaign to %d customers", len(sms_customers))
    # Placeholder for actual SMS sending logic
    # e.g., sms_service.send_bulk(sms_customers["phone_number"])
    logger.info("SMS campaign completed")


@task(
    retries=2,
    retry_delay_seconds=300,
)
def send_push_notification(customer_data: pd.DataFrame) -> None:
    """Send push notification campaign to mobile app users.

    Args:
        customer_data: DataFrame containing customer information.
    """
    logger = get_run_logger()
    push_customers = customer_data[customer_data["has_app"] == True]
    logger.info(
        "Sending push notification to %d mobile app users", len(push_customers)
    )
    # Placeholder for actual push notification logic
    # e.g., push_service.send_bulk(push_customers["device_id"])
    logger.info("Push notification campaign completed")


@flow
def marketing_campaign_flow(csv_path: str = "customer_segments.csv") -> None:
    """Orchestrates the multi-channel marketing campaign.

    The flow loads customer data and then runs three campaign tasks in parallel.
    """
    logger = get_run_logger()
    logger.info("Starting marketing campaign flow")

    # Load data
    customer_df = load_customer_segment_csv(csv_path)

    # Run campaigns in parallel
    email_future = send_email_campaign.submit(customer_df)
    sms_future = send_sms_campaign.submit(customer_df)
    push_future = send_push_notification.submit(customer_df)

    # Wait for all to complete (optional, ensures flow doesn't finish early)
    email_future.result()
    sms_future.result()
    push_future.result()

    logger.info("Marketing campaign flow completed")


# Note: Deployment configuration (e.g., daily schedule starting 2024-01-01,
# catchup disabled, owner metadata) should be defined in Prefect UI or via
# deployment YAML/CLI, not in code.

if __name__ == "__main__":
    marketing_campaign_flow()