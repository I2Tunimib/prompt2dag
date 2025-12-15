from prefect import flow, task
from typing import Literal


@task(retries=2, retry_delay_seconds=300)
def analyze_transactions() -> float:
    """Read and process daily transaction CSV, calculate risk scores."""
    print("Processing daily transaction CSV file with 1500 transactions...")
    risk_score = 0.75  # Mock risk score per pipeline description
    print(f"Calculated risk score: {risk_score}")
    return risk_score


@task(retries=2, retry_delay_seconds=300)
def route_transaction(risk_score: float) -> Literal["manual_review", "auto_approve"]:
    """Apply risk model logic and determine routing path based on 0.8 threshold."""
    threshold = 0.8
    print(f"Evaluating risk score {risk_score} against threshold {threshold}")
    
    if risk_score > threshold:
        print("Routing to manual review queue")
        return "manual_review"
    else:
        print("Routing to auto-approval")
        return "auto_approve"


@task(retries=2, retry_delay_seconds=300)
def route_to_manual_review(risk_score: float) -> str:
    """Send high-risk transaction to manual review queue for analyst inspection."""
    print(f"High-risk transaction (score: {risk_score}) sent to manual review queue")
    return "manual_review_completed"


@task(retries=2, retry_delay_seconds=300)
def route_to_auto_approve(risk_score: float) -> str:
    """Auto-approve low-risk transaction for payment processing."""
    print(f"Low-risk transaction (score: {risk_score}) auto-approved for payment processing")
    return "auto_approve_completed"


@task(retries=2, retry_delay_seconds=300)
def send_notification(branch_result: str, risk_score: float) -> None:
    """Merge point: send completion notification to fraud detection team."""
    print(f"Sending notification to fraud detection team...")
    print(f"Transaction processed - Risk Score: {risk_score}, Path: {branch_result}")
    print("Notification sent successfully")


@flow(name="fraud-detection-triage-pipeline")
def fraud_detection_triage_pipeline():
    """
    Fraud detection triage pipeline processing daily transaction batches
    through conditional routing based on risk scoring.
    
    Schedule: Daily execution starting January 1, 2024 (catchup=False)
    Retry Policy: 2 retries with 5-minute delays
    Failure Handling: Email notifications on task failures (configure in Prefect UI/Blocks)
    Trigger Rule: none_failed for merge task (default behavior)
    """
    # Step 1: Analyze transactions and calculate risk score
    risk_score = analyze_transactions()
    
    # Step 2: Determine routing path based on risk threshold
    route_decision = route_transaction(risk_score)
    
    # Step 3: Conditional branch execution (mutually exclusive paths)
    # Only one branch will execute based on the route_decision
    if route_decision == "manual_review":
        branch_result = route_to_manual_review(risk_score)
    else:
        branch_result = route_to_auto_approve(risk_score)
    
    # Step 4: Merge point - send notification after branch completion
    # This task depends on the branch task and will run with none_failed trigger
    send_notification(branch_result, risk_score)


if __name__ == "__main__":
    # Local execution for testing
    # For scheduled deployment, configure schedule in deployment:
    # prefect deployment build ... --schedule "0 0 * * *" --anchor-date 2024-01-01
    fraud_detection_triage_pipeline()