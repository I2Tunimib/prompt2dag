from pathlib import Path
from datetime import timedelta

import pandas as pd
from prefect import flow, task, get_run_logger

# Note: Deployment schedule (daily, start 2024-01-01, no catchup) should be configured
# in the Prefect UI or via a deployment script, not directly in this module.


@task(retries=2, retry_delay_seconds=300)
def load_customer_segment_csv(csv_path: str) -> pd.DataFrame:
    """Load customer segment data from a CSV file.

    Args:
        csv_path: Path to the CSV file containing customer IDs, segments,
            and contact information.

    Returns:
        DataFrame with the loaded customer data.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
    """
    logger = get_run_logger()
    path = Path(csv_path)

    if not path.is_file():
        logger.error("CSV file not found: %s", csv_path)
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(path)
    logger.info("Loaded %d rows from %s", len(df), csv_path)
    return df


@task(retries=2, retry_delay_seconds=300)
def send_email_campaign(customers: pd.DataFrame) -> None:
    """Send email campaign to premium customers.

    Args:
        customers: DataFrame containing customer data.
    """
    logger = get_run_logger()
    premium_customers = customers[customers["segment"] == "premium"]
    logger.info(
        "Sending email campaign to %d premium customers", len(premium_customers)
    )
    # Insert real email sending logic here.
    # For example: email_service.send_bulk(premium_customers["email"])
    logger.debug("Email campaign completed.")


@task(retries=2, retry_delay_seconds=300)
def send_sms_campaign(customers: pd.DataFrame) -> None:
    """Send SMS campaign to the target segment.

    Args:
        customers: DataFrame containing customer data.
    """
    logger = get_run_logger()
    sms_customers = customers[customers["segment"] != "opt_out"]
    logger.info(
        "Sending SMS campaign to %d customers", len(sms_customers)
    )
    # Insert real SMS sending logic here.
    # For example: sms_service.send_bulk(sms_customers["phone_number"])
    logger.debug("SMS campaign completed.")


@task(retries=2, retry_delay_seconds=300)
def send_push_notification(customers: pd.DataFrame) -> None:
    """Send push notification campaign to mobile app users.

    Args:
        customers: DataFrame containing customer data.
    """
    logger = get_run_logger()
    push_customers = customers[customers["has_app"] == True]  # noqa: E712
    logger.info(
        "Sending push notification to %d app users", len(push_customers)
    )
    # Insert real push notification logic here.
    # For example: push_service.send_bulk(push_customers["device_id"])
    logger.debug("Push notification campaign completed.")


@flow
def marketing_campaign_flow(csv_path: str = "customer_segments.csv") -> None:
    """Orchestrate loading customer data and launching parallel marketing campaigns.

    Args:
        csv_path: Path to the CSV file with customer segment data.
    """
    logger = get_run_logger()
    logger.info("Starting marketing campaign flow.")

    customers = load_customer_segment_csv(csv_path)

    # Fan‑out: run campaign tasks in parallel.
    email_future = send_email_campaign.submit(customers)
    sms_future = send_sms_campaign.submit(customers)
    push_future = send_push_notification.submit(customers)

    # Wait for all parallel tasks to finish.
    email_future.result()
    sms_future.result()
    push_future.result()

    logger.info("Marketing campaign flow completed.")


if __name__ == "__main__":
    marketing_campaign_flow()