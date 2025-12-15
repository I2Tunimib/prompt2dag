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
    df = pd.read_csv(file_path)
    # Mock risk score calculation
    risk_score = 0.75
    return risk_score

@task
def route_transaction(risk_score: float) -> str:
    """
    Applies risk model logic and conditionally routes execution based on risk threshold comparison.
    """
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
    logger.info(f"Transaction {transaction_id} routed to manual review.")
    # Simulate manual review process
    return "Manual review completed"

@task
def route_to_auto_approve(transaction_id: str):
    """
    Auto-approves transaction for payment processing.
    """
    logger = get_run_logger()
    logger.info(f"Transaction {transaction_id} auto-approved.")
    # Simulate auto-approval process
    return "Auto-approval completed"

@task(trigger="none_failed")
def send_notification(result: str):
    """
    Sends completion notification to fraud detection team.
    """
    logger = get_run_logger()
    logger.info(f"Notification sent: {result}")

@flow(retries=2, retry_delay_seconds=300)
def fraud_detection_pipeline(file_path: str):
    """
    Orchestrates the fraud detection triage pipeline.
    """
    risk_score = analyze_transactions(file_path)
    route = route_transaction(risk_score)

    if route == "manual_review":
        result = route_to_manual_review.submit("transaction_id")
    else:
        result = route_to_auto_approve.submit("transaction_id")

    send_notification(result.wait())

if __name__ == "__main__":
    # Example file path
    file_path = "daily_transactions.csv"
    fraud_detection_pipeline(file_path)

# Deployment configuration (optional)
# deployment = Deployment.build_from_flow(
#     flow=fraud_detection_pipeline,
#     name="Daily Fraud Detection",
#     schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024, 1, 1), end_date=None),
#     work_queue_name="default",
#     catchup=False,
#     parameters={"file_path": "daily_transactions.csv"},
# )
# deployment.apply()