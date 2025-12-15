from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Failed
from prefect_email import EmailServerCredentials, email_send_message
from datetime import timedelta, datetime
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def analyze_transactions():
    """
    Reads and processes daily transaction CSV files, calculates risk scores,
    and returns a mock risk score of 0.75.
    """
    logger = get_run_logger()
    logger.info("Analyzing transactions...")
    # Mock risk score for demonstration
    risk_score = 0.75
    logger.info(f"Calculated risk score: {risk_score}")
    return risk_score


@task
def route_transaction(risk_score):
    """
    Applies risk model logic to route transactions based on a 0.8 risk threshold.
    """
    logger = get_run_logger()
    logger.info("Routing transaction based on risk score...")
    if risk_score > 0.8:
        return "manual_review"
    else:
        return "auto_approve"


@task
def route_to_manual_review():
    """
    Sends transactions to the manual review queue for analyst inspection.
    """
    logger = get_run_logger()
    logger.info("Routing transaction to manual review...")
    # Mock manual review queue interaction
    logger.info("Transaction sent to manual review queue.")


@task
def route_to_auto_approve():
    """
    Auto-approves transactions for payment processing.
    """
    logger = get_run_logger()
    logger.info("Auto-approving transaction...")
    # Mock payment processing interaction
    logger.info("Transaction auto-approved.")


@task(trigger="none_failed")
def send_notification():
    """
    Sends a completion notification to the fraud detection team.
    """
    logger = get_run_logger()
    logger.info("Sending completion notification...")
    # Mock email notification
    logger.info("Notification sent to fraud detection team.")


@flow(name="Fraud Detection Triage Pipeline")
def fraud_detection_pipeline():
    """
    Orchestrates the fraud detection triage pipeline.
    """
    risk_score = analyze_transactions()
    route = route_transaction(risk_score)

    if route == "manual_review":
        route_to_manual_review.submit()
    else:
        route_to_auto_approve.submit()

    send_notification()


if __name__ == "__main__":
    # Schedule: Daily execution via @daily interval
    # Start Date: January 1, 2024
    # Catchup: Disabled to prevent historical backfills
    # Retry Policy: 2 retries with 5-minute delays between attempts
    # Failure Handling: Email notifications on task failures only
    # Trigger Rule: Uses none_failed for merge task to ensure execution after either branch path

    schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1))
    deployment = Deployment.build_from_flow(
        flow=fraud_detection_pipeline,
        name="Daily Fraud Detection Pipeline",
        schedule=schedule,
        catchup=False,
        work_queue_name="default",
        parameters={},
        tags=["fraud-detection"],
        description="Daily fraud detection triage pipeline",
    )
    deployment.apply()

    # For local execution
    fraud_detection_pipeline()