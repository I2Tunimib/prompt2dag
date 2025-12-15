from datetime import datetime, timedelta

import pandas as pd
from dagster import (
    In,
    Nothing,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    JobDefinition,
    get_dagster_logger,
    job,
    multiprocess_executor,
    op,
)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    config_schema={"file_path": str},
    description="Load customer segment data from a CSV file.",
    out={"customer_df": In(pd.DataFrame)},
)
def load_customer_segment_csv(context):
    """Read a CSV containing customer IDs, segments, and contact information."""
    file_path = context.op_config.get("file_path", "customer_segments.csv")
    logger = get_dagster_logger()
    logger.info(f"Loading customer segment data from %s", file_path)

    try:
        df = pd.read_csv(file_path)
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to read CSV: %s", exc)
        raise

    logger.info("Loaded %d rows", len(df))
    return df


def _log_campaign(context, channel_name: str, df: pd.DataFrame, segment_filter: str):
    logger = get_dagster_logger()
    filtered = df[df["segment"] == segment_filter] if segment_filter else df
    logger.info(
        "Sending %s campaign to %d customers (segment=%s)",
        channel_name,
        len(filtered),
        segment_filter or "all",
    )
    # Placeholder for real integration with email/SMS/push services.
    return Nothing


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Send email campaign to premium customers.",
    ins={"customer_df": In(pd.DataFrame)},
    out={"result": Out(Nothing)},
)
def send_email_campaign(context, customer_df: pd.DataFrame):
    return _log_campaign(context, "email", customer_df, segment_filter="premium")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Send SMS campaign to all customers.",
    ins={"customer_df": In(pd.DataFrame)},
    out={"result": Out(Nothing)},
)
def send_sms_campaign(context, customer_df: pd.DataFrame):
    return _log_campaign(context, "SMS", customer_df, segment_filter="")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Send push notification campaign to mobile app users.",
    ins={"customer_df": In(pd.DataFrame)},
    out={"result": Out(Nothing)},
)
def send_push_notification(context, customer_df: pd.DataFrame):
    return _log_campaign(context, "push notification", customer_df, segment_filter="")


@job(
    executor_def=multiprocess_executor.configured({"max_concurrent": 3}),
    description="Fan‑out marketing campaign job.",
)
def marketing_campaign_job():
    """Load data then run three campaigns in parallel."""
    df = load_customer_segment_csv()
    send_email_campaign(df)
    send_sms_campaign(df)
    send_push_notification(df)


marketing_campaign_schedule = ScheduleDefinition(
    job=marketing_campaign_job,
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Daily execution of the marketing campaign job.",
)


defs = Definitions(
    jobs=[marketing_campaign_job],
    schedules=[marketing_campaign_schedule],
)


if __name__ == "__main__":
    result = marketing_campaign_job.execute_in_process()
    if result.success:
        print("Marketing campaign job completed successfully.")
    else:
        print("Marketing campaign job failed.")