from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Failed
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from datetime import datetime, timedelta
import pandas as pd

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def analyze_transactions(file_path: str) -> float:
    """
    Reads and processes daily transaction CSV files, calculates risk scores.
    Returns a mock risk score of 0.75.
    """
    logger = get_run_logger()
    logger.info(f"Analyzing transactions from {file_path}")
    # Mock processing and risk score calculation
    risk_score = 0.75
    logger.info(f"Calculated risk score: {risk_score}")
    return risk_score

@task
def route_transaction(risk_score: float) -> str:
    """
    Applies risk model logic to route transactions based on risk threshold.
    """
    logger = get_run_logger()
    logger.info(f"Routing transaction with risk score: {risk_score}")
    if risk_score > 0.8:
        return "manual_review"
    else:
        return "auto_approve"

@task
def route_to_manual_review(transaction_id: str):
    """
    Sends transaction to manual review queue for analyst inspection.
    """
    logger = get_run_logger()
    logger.info(f"Routing transaction {transaction_id} to manual review")
    # Mock manual review queue interaction
    logger.info(f"Transaction {transaction_id} sent to manual review queue")

@task
def route_to_auto_approve(transaction_id: str):
    """
    Auto-approves transaction for payment processing.
    """
    logger = get_run_logger()
    logger.info(f"Auto-approving transaction {transaction_id}")
    # Mock payment processing system interaction
    logger.info(f"Transaction {transaction_id} auto-approved for payment processing")

@task(trigger="none_failed")
def send_notification(transaction_id: str, route: str):
    """
    Sends completion notification to the fraud detection team.
    """
    logger = get_run_logger()
    logger.info(f"Sending notification for transaction {transaction_id} (route: {route})")
    # Mock notification system interaction
    logger.info(f"Notification sent for transaction {transaction_id}")

@flow(retries=2, retry_delay_seconds=300)
def fraud_detection_triage(file_path: str):
    """
    Fraud detection triage pipeline that processes daily transaction batches.
    """
    logger = get_run_logger()
    logger.info("Starting fraud detection triage pipeline")

    risk_score = analyze_transactions(file_path)
    route = route_transaction(risk_score)

    if route == "manual_review":
        route_to_manual_review.submit("transaction_id")
    else:
        route_to_auto_approve.submit("transaction_id")

    send_notification("transaction_id", route)

if __name__ == "__main__":
    # Example file path for local execution
    file_path = "path/to/transactions.csv"
    fraud_detection_triage(file_path)

# Deployment configuration (optional)
# deployment = Deployment.build_from_flow(
#     flow=fraud_detection_triage,
#     name="daily_fraud_detection",
#     schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024, 1, 1), end_date=None, timezone="UTC"),
#     catchup=False,
#     work_queue_name="default",
#     parameters={"file_path": "path/to/transactions.csv"},
#     tags=["fraud_detection"],
# )
# deployment.apply()