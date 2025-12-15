from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import time
from typing import Literal


@task(retries=2, retry_delay_seconds=300)
def analyze_transactions() -> float:
    """
    Simulates reading and processing daily transaction CSV files.
    Returns a mock risk score.
    """
    print("Processing 1500 transactions from daily CSV batch...")
    time.sleep(2)
    risk_score = 0.75
    print(f"Calculated risk score: {risk_score}")
    return risk_score


@task(retries=2, retry_delay_seconds=300)
def route_transaction(risk_score: float) -> Literal["manual_review", "auto_approve"]:
    """
    Determines routing path based on risk score threshold.
    """
    threshold = 0.8
    if risk_score > threshold:
        print(f"Risk score {risk_score} exceeds threshold {threshold}, routing to manual review")
        return "manual_review"
    else:
        print(f"Risk score {risk_score} is within threshold {threshold}, routing to auto-approval")
        return "auto_approve"


@task(retries=2, retry_delay_seconds=300)
def route_to_manual_review(risk_score: float) -> str:
    """
    Sends transaction to manual review queue for analyst inspection.
    """
    print(f"Sending transaction with risk score {risk_score} to manual review queue")
    time.sleep(1)
    return "manual_review_complete"


@task(retries=2, retry_delay_seconds=300)
def route_to_auto_approve(risk_score: float) -> str:
    """
    Auto-approves transaction for payment processing.
    """
    print(f"Auto-approving transaction with risk score {risk_score} for payment processing")
    time.sleep(1)
    return "auto_approve_complete"


@task(retries=2, retry_delay_seconds=300)
def send_notification(branch_result: str) -> None:
    """
    Sends completion notification to fraud detection team.
    """
    print(f"Sending notification to fraud detection team: {branch_result}")
    time.sleep(1)


@flow(
    name="fraud-detection-triage-pipeline",
    task_runner=ConcurrentTaskRunner(),
    # Schedule configuration (apply at deployment):
    # prefect deployment build --name daily-triage --cron "0 0 * * *" \
    #   --start-date 2024-01-01T00:00:00 --no-catchup
    # Email alerts for task failures configured via Prefect UI/Automations
)
def fraud_detection_triage_pipeline() -> None:
    """
    Fraud detection triage pipeline with conditional routing.
    Implements branch-merge pattern with risk-based decision point.
    """
    risk_score = analyze_transactions()
    routing_decision = route_transaction(risk_score)
    
    branch_result = None
    if routing_decision == "manual_review":
        branch_result = route_to_manual_review.submit(risk_score)
    else:
        branch_result = route_to_auto_approve.submit(risk_score)
    
    send_notification(branch_result)


if __name__ == "__main__":
    fraud_detection_triage_pipeline()