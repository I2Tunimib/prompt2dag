from dagster import op, job, In, Out, RetryPolicy, ScheduleDefinition, Definitions
import pandas as pd
from datetime import datetime

# Mock resources for external systems
class MockManualReviewQueue:
    def submit(self, transaction_id, risk_score):
        print(f"Submitted transaction {transaction_id} to manual review queue with risk score {risk_score}")

class MockPaymentProcessor:
    def approve(self, transaction_id, risk_score):
        print(f"Auto-approved transaction {transaction_id} with risk score {risk_score}")

class MockEmailNotifier:
    def send_failure_alert(self, task_name, error):
        print(f"FAILURE ALERT: Task {task_name} failed with error: {error}")
    
    def send_notification(self, message):
        print(f"NOTIFICATION: {message}")

# Resources
@op
def manual_review_queue_resource():
    return MockManualReviewQueue()

@op
def payment_processor_resource():
    return MockPaymentProcessor()

@op
def email_notifier_resource():
    return MockEmailNotifier()

# Ops
@op(
    out={"risk_score": Out(float), "transaction_data": Out(pd.DataFrame)},
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 2 retries, 5 min delay
)
def analyze_transactions(context):
    """
    Reads and processes daily transaction CSV files, calculates risk scores.
    Returns mock risk score of 0.75 and transaction data.
    """
    # Mock: Simulate reading CSV with 1500 transactions
    context.log.info("Reading daily transaction CSV file...")
    
    # Create mock data
    data = {
        "transaction_id": range(1, 1501),
        "amount": [100 + i * 0.5 for i in range(1500)],
        "risk_score": [0.75] * 1500  # Mock uniform risk score
    }
    df = pd.DataFrame(data)
    
    # Mock risk score calculation
    risk_score = 0.75  # As per description
    
    context.log.info(f"Processed 1500 transactions with risk score: {risk_score}")
    return risk_score, df

@op(
    ins={"risk_score": In(float)},
    out={"route_decision": Out(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def route_transaction(context, risk_score: float):
    """
    Applies risk model logic and determines routing path based on 0.8 threshold.
    Returns 'manual_review' if risk > 0.8, otherwise 'auto_approve'.
    """
    context.log.info(f"Routing decision for risk score: {risk_score}")
    
    if risk_score > 0.8:
        context.log.info("Routing to manual review")
        return "manual_review"
    else:
        context.log.info("Routing to auto-approval")
        return "auto_approve"

@op(
    ins={"risk_score": In(float), "transaction_data": In(pd.DataFrame)},
    required_resource_keys={"manual_review_queue", "email_notifier"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def route_to_manual_review(context, risk_score: float, transaction_data: pd.DataFrame):
    """
    Executes only when risk score exceeds 0.8.
    Sends transactions to manual review queue for analyst inspection.
    """
    # Only process if risk score > 0.8
    if risk_score <= 0.8:
        context.log.info("Risk score <= 0.8, skipping manual review")
        return
    
    context.log.info(f"Sending {len(transaction_data)} transactions to manual review")
    
    try:
        queue = context.resources.manual_review_queue
        # In real scenario, would filter high-risk transactions
        for tx_id in transaction_data["transaction_id"].head(5):  # Mock: only first 5
            queue.submit(tx_id, risk_score)
        
        context.log.info("Manual review submission completed")
    except Exception as e:
        context.resources.email_notifier.send_failure_alert("route_to_manual_review", str(e))
        raise

@op(
    ins={"risk_score": In(float), "transaction_data": In(pd.DataFrame)},
    required_resource_keys={"payment_processor", "email_notifier"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def route_to_auto_approve(context, risk_score: float, transaction_data: pd.DataFrame):
    """
    Executes only when risk score is 0.8 or lower.
    Auto-approves transactions for payment processing.
    """
    # Only process if risk score <= 0.8
    if risk_score > 0.8:
        context.log.info("Risk score > 0.8, skipping auto-approval")
        return
    
    context.log.info(f"Auto-approving {len(transaction_data)} transactions")
    
    try:
        processor = context.resources.payment_processor
        # In real scenario, would process all transactions
        for tx_id in transaction_data["transaction_id"].head(5):  # Mock: only first 5
            processor.approve(tx_id, risk_score)
        
        context.log.info("Auto-approval completed")
    except Exception as e:
        context.resources.email_notifier.send_failure_alert("route_to_auto_approve", str(e))
        raise

@op(
    ins={"manual_review_result": In(bool), "auto_approve_result": In(bool)},
    required_resource_keys={"email_notifier"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def send_notification(context, manual_review_result: bool, auto_approve_result: bool):
    """
    Merge point task that executes after either branch completes.
    Sends completion notification to fraud detection team.
    """
    context.log.info("Preparing final notification")
    
    # Determine which path was taken
    if manual_review_result:
        message = "Fraud detection pipeline completed: Transactions routed to manual review"
    elif auto_approve_result:
        message = "Fraud detection pipeline completed: Transactions auto-approved"
    else:
        message = "Fraud detection pipeline completed: No action taken"
    
    try:
        context.resources.email_notifier.send_notification(message)
        context.log.info(f"Notification sent: {message}")
    except Exception as e:
        context.resources.email_notifier.send_failure_alert("send_notification", str(e))
        raise

# Job
@job(
    resource_defs={
        "manual_review_queue": manual_review_queue_resource,
        "payment_processor": payment_processor_resource,
        "email_notifier": email_notifier_resource
    }
)
def fraud_detection_triage_pipeline():
    """
    Fraud detection triage pipeline that processes daily transaction batches
    through conditional routing based on risk scoring.
    """
    # Step 1: Analyze transactions
    risk_score, transaction_data = analyze_transactions()
    
    # Step 2: Route based on risk score
    route_decision = route_transaction(risk_score)
    
    # Step 3: Execute both branches (mutually exclusive internally)
    manual_review_result = route_to_manual_review(risk_score, transaction_data)
    auto_approve_result = route_to_auto_approve(risk_score, transaction_data)
    
    # Step 4: Send notification after either branch (none_failed trigger equivalent)
    send_notification(manual_review_result, auto_approve_result)

# Schedule
fraud_detection_schedule = ScheduleDefinition(
    job=fraud_detection_triage_pipeline,
    cron_schedule="0 0 * * *",  # Daily at midnight
    execution_timezone="UTC"
)

# Definitions
defs = Definitions(
    jobs=[fraud_detection_triage_pipeline],
    schedules=[fraud_detection_schedule]
)

# Launch pattern
if __name__ == "__main__":
    # Execute the job in process for testing
    result = fraud_detection_triage_pipeline.execute_in_process(
        run_config={
            "execution": {
                "config": {
                    "start_date": "2024-01-01",
                    "catchup": False
                }
            }
        }
    )