from dagster import op, job, In, Out, RetryPolicy, ScheduleDefinition, Definitions
import pandas as pd
from datetime import datetime
import time

# Minimal resource stubs for external integrations
# In production, these would be configured with real credentials
# @resource
# def email_resource(context):
#     """Email client for failure notifications and team alerts"""
#     return EmailClient(smtp_server="smtp.example.com")
#
# @resource
# def review_queue_resource(context):
#     """Manual review queue client for high-risk transactions"""
#     return ReviewQueueClient(connection_string="...")
#
# @resource
# def payment_processor_resource(context):
#     """Payment processing system client for auto-approved transactions"""
#     return PaymentProcessorClient(api_key="...")


@op(
    out={"risk_score": Out(float)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
    tags={"description": "Reads and processes daily transaction CSV files"}
)
def analyze_transactions(context):
    """
    Reads and processes daily transaction CSV files containing 1500 transactions,
    calculates risk scores, and returns a mock risk score of 0.75.
    """
    context.log.info("Reading daily transaction CSV file...")
    transaction_count = 1500
    
    # Mock risk calculation - returning 0.75 as per description
    risk_score = 0.75
    context.log.info(f"Processed {transaction_count} transactions, calculated risk score: {risk_score}")
    
    return risk_score


@op(
    ins={"risk_score": In(float)},
    out={"routing_decision": Out(dict)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"description": "Applies risk model logic and determines routing path"}
)
def route_transaction(context, risk_score: float):
    """
    Applies risk model logic and conditionally routes execution based on risk threshold.
    Returns routing decision dict with risk score, decision, and threshold.
    """
    context.log.info(f"Routing transaction with risk score: {risk_score}")
    
    threshold = 0.8
    decision = "manual_review" if risk_score > threshold else "auto_approve"
    
    context.log.info(f"Routing decision: {decision} (threshold: {threshold})")
    return {
        "risk_score": risk_score,
        "decision": decision,
        "threshold": threshold
    }


@op(
    ins={"routing_decision": In(dict)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"description": "Sends transaction to manual review queue for analyst inspection"}
)
def route_to_manual_review(context, routing_decision: dict):
    """
    Executes only when risk score exceeds 0.8, sends transaction to manual review queue.
    """
    if routing_decision["decision"] != "manual_review":
        context.log.info("Skipping manual review branch (not selected by router)")
        return None
    
    risk_score = routing_decision["risk_score"]
    context.log.info(f"Sending transaction with risk score {risk_score} to manual review queue")
    
    # Simulate sending to review queue
    # In production: context.resources.review_queue.submit(transaction_id, risk_score)
    time.sleep(1)  # Simulate API call
    context.log.info("Transaction successfully queued for manual review")
    
    return {"status": "manual_review_queued", "risk_score": risk_score}


@op(
    ins={"routing_decision": In(dict)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"description": "Auto-approves transaction for payment processing"}
)
def route_to_auto_approve(context, routing_decision: dict):
    """
    Executes only when risk score is 0.8 or lower, auto-approves transaction.
    """
    if routing_decision["decision"] != "auto_approve":
        context.log.info("Skipping auto-approval branch (not selected by router)")
        return None
    
    risk_score = routing_decision["risk_score"]
    context.log.info(f"Auto-approving transaction with risk score {risk_score} for payment processing")
    
    # Simulate sending to payment processor
    # In production: context.resources.payment_processor.approve(transaction_id, risk_score)
    time.sleep(1)  # Simulate API call
    context.log.info("Transaction successfully auto-approved")
    
    return {"status": "auto_approved", "risk_score": risk_score}


@op(
    ins={
        "manual_review_result": In(object, default_value=None),
        "auto_approve_result": In(object, default_value=None)
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"description": "Merge point that sends completion notification to fraud detection team"}
)
def send_notification(context, manual_review_result, auto_approve_result):
    """
    Merge point task that executes after either branch completes,
    sends completion notification to fraud detection team.
    """
    context.log.info("Preparing completion notification for fraud detection team")
    
    # Determine which path was taken
    if manual_review_result:
        path_taken = "manual_review"
        risk_score = manual_review_result["risk_score"]
    elif auto_approve_result:
        path_taken = "auto_approve"
        risk_score = auto_approve_result["risk_score"]
    else:
        path_taken = "unknown"
        risk_score = "N/A"
    
    context.log.info(f"Path taken: {path_taken}, Risk score: {risk_score}")
    context.log.info("Sending notification to fraud detection team")
    
    # Simulate email notification
    # In production: context.resources.email.send(...)
    time.sleep(1)
    context.log.info("Notification sent successfully")


@job(
    tags={"description": "Fraud detection triage pipeline with conditional routing"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def fraud_detection_triage_job():
    """
    Fraud detection triage pipeline that processes daily transaction batches
    through conditional routing based on risk scoring.
    """
    # Step 1: Analyze transactions and calculate risk score
    risk_score = analyze_transactions()
    
    # Step 2: Route based on risk score threshold
    routing_decision = route_transaction(risk_score)
    
    # Step 3: Parallel branches (mutually exclusive based on routing decision)
    manual_review_result = route_to_manual_review(routing_decision)
    auto_approve_result = route_to_auto_approve(routing_decision)
    
    # Step 4: Merge point - send notification after both branches complete
    # Using object inputs with default values to handle the mutually exclusive nature
    send_notification(manual_review_result, auto_approve_result)


# Schedule: Daily execution starting 2024-01-01, catchup disabled
# Note: In Dagster, start_date and catchup are configured at the scheduler level
# when deploying