from prefect import flow, task
import time


@task(
    retries=2,
    retry_delay_seconds=300,
    name="analyze_transactions"
)
def analyze_transactions() -> float:
    """
    Simulates reading and processing daily transaction CSV files.
    Processes 1500 transactions and returns a mock risk score.
    """
    print("Reading daily transaction CSV batch...")
    print("Processing 1500 transactions...")
    time.sleep(2)
    
    risk_score = 0.75
    print(f"Calculated aggregate risk score: {risk_score}")
    
    return risk_score


@task(
    retries=2,
    retry_delay_seconds=300,
    name="route_to_manual_review"
)
def route_to_manual_review(risk_score: float) -> str:
    """Routes high-risk transactions to manual review queue."""
    print(f"High risk detected (score: {risk_score})")
    print("Routing to manual review queue for analyst inspection...")
    time.sleep(1)
    return "manual_review"


@task(
    retries=2,
    retry_delay_seconds=300,
    name="route_to_auto_approve"
)
def route_to_auto_approve(risk_score: float) -> str:
    """Auto-approves low-risk transactions for payment processing."""
    print(f"Low risk detected (score: {risk_score})")
    print("Auto-approving for payment processing...")
    time.sleep(1)
    return "auto_approve"


@task(
    retries=2,
    retry_delay_seconds=300,
    name="send_notification"
)
def send_notification(risk_score: float, route_taken: str):
    """
    Sends completion notification to fraud detection team.
    Merge point that executes after either branch completes.
    """
    print(f"Preparing notification for fraud detection team...")
    print(f"Route taken: {route_taken}")
    print(f"Risk score: {risk_score}")
    time.sleep(1)
    print("Notification sent to fraud detection team")


@flow(name="fraud-detection-triage-pipeline")
def fraud_detection_triage_flow():
    """
    Fraud detection triage pipeline that processes daily transaction batches
    through conditional routing based on risk scoring.
    
    Schedule: Daily at midnight starting 2024-01-01, catchup disabled
    Retry Policy: 2 retries with 5-minute delays
    Failure Handling: Email notifications on task failures (configure via Prefect automations)
    """
    # Step 1: Analyze transactions and calculate risk score
    risk_score = analyze_transactions()
    
    # Step 2: Conditional routing based on risk threshold (0.8)
    # Branches are mutually exclusive - only one path executes
    
    if risk_score > 0.8:
        # High risk path - route to manual review
        branch_future = route_to_manual_review.submit(risk_score)
        route_taken = "manual_review"
    else:
        # Low risk path - auto approve
        branch_future = route_to_auto_approve.submit(risk_score)
        route_taken = "auto_approve"
    
    # Wait for branch to complete and get result (none_failed behavior)
    # This ensures the branch task succeeded before proceeding
    branch_future.result()
    
    # Step 3: Merge point - send notification after branch completes
    send_notification(risk_score, route_taken)


if __name__ == "__main__":
    # Local execution for testing
    fraud_detection_triage_flow()