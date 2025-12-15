from dagster import op, job, In, Out, RetryPolicy, Failure, get_dagster_logger
from dagster.utils import file_relative_path
import pandas as pd

logger = get_dagster_logger()

@op(
    out={"risk_score": Out(dagster_type=float, description="Calculated risk score")},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def analyze_transactions(context):
    """
    Reads and processes daily transaction CSV files, calculates risk scores.
    Mocks a risk score of 0.75 for demonstration purposes.
    """
    # Mock reading a CSV file and calculating a risk score
    risk_score = 0.75
    logger.info(f"Calculated risk score: {risk_score}")
    return risk_score

@op(
    ins={"risk_score": In(dagster_type=float, description="Calculated risk score")},
    out={
        "manual_review": Out(dagster_type=bool, description="Flag for manual review"),
        "auto_approve": Out(dagster_type=bool, description="Flag for auto-approval"),
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_transaction(context, risk_score):
    """
    Applies risk model logic to route transactions based on risk threshold.
    """
    if risk_score > 0.8:
        logger.info("Transaction routed to manual review")
        yield Output(True, "manual_review")
        yield Output(False, "auto_approve")
    else:
        logger.info("Transaction auto-approved")
        yield Output(False, "manual_review")
        yield Output(True, "auto_approve")

@op(
    ins={"manual_review_flag": In(dagster_type=bool, description="Flag for manual review")},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_to_manual_review(context, manual_review_flag):
    """
    Sends transaction to manual review queue for analyst inspection.
    """
    if manual_review_flag:
        logger.info("Transaction sent to manual review queue")
        # Mock sending to manual review queue
        pass

@op(
    ins={"auto_approve_flag": In(dagster_type=bool, description="Flag for auto-approval")},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_to_auto_approve(context, auto_approve_flag):
    """
    Auto-approves transaction for payment processing.
    """
    if auto_approve_flag:
        logger.info("Transaction auto-approved for payment processing")
        # Mock auto-approval process
        pass

@op(
    ins={
        "manual_review_flag": In(dagster_type=bool, description="Flag for manual review"),
        "auto_approve_flag": In(dagster_type=bool, description="Flag for auto-approval"),
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def send_notification(context, manual_review_flag, auto_approve_flag):
    """
    Sends completion notification to the fraud detection team.
    """
    logger.info("Sending completion notification to fraud detection team")
    # Mock sending notification
    pass

@job(
    resource_defs={
        # Add any required resources here
    },
    tags={
        "dagster/pipeline/schedule_interval": "@daily",
        "dagster/pipeline/start_date": "2024-01-01",
        "dagster/pipeline/catchup": "False",
    },
)
def fraud_detection_triage_pipeline():
    risk_score = analyze_transactions()
    manual_review_flag, auto_approve_flag = route_transaction(risk_score)
    
    manual_review = route_to_manual_review(manual_review_flag)
    auto_approve = route_to_auto_approve(auto_approve_flag)
    
    send_notification(manual_review, auto_approve)

if __name__ == "__main__":
    result = fraud_detection_triage_pipeline.execute_in_process()