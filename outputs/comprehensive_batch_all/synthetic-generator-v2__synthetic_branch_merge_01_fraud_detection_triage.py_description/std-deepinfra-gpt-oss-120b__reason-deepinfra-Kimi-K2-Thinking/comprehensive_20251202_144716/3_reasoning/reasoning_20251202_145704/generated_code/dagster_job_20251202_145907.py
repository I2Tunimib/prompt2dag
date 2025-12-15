from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    resource,
    get_dagster_logger,
    Nothing,
)

# Resource stubs for external integrations
@resource
def email_service_resource():
    """Stub for email notification service."""
    class EmailService:
        def send_failure_notification(self, task_name, error):
            logger = get_dagster_logger()
            logger.info(f"[EMAIL STUB] Failure in {task_name}: {error}")
            
        def send_completion_notification(self):
            logger = get_dagster_logger()
            logger.info("[EMAIL STUB] Completion notification sent to fraud detection team")
    
    return EmailService()

@resource
def manual_review_queue_resource():
    """Stub for manual review queue system."""
    class ManualReviewQueue:
        def send_for_review(self, transaction_data):
            logger = get_dagster_logger()
            logger.info("[QUEUE STUB] Transaction sent to manual review queue")
    
    return ManualReviewQueue()

@resource
def payment_processing_resource():
    """Stub for payment processing system."""
    class PaymentProcessing:
        def auto_approve(self, transaction_data):
            logger = get_dagster_logger()
            logger.info("[PAYMENT STUB] Transaction auto-approved for payment")
    
    return PaymentProcessing()

# Ops with retry policies
@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,  # 5 minutes in seconds
    )
)
def analyze_transactions() -> float:
    """Reads and processes daily transaction CSV files, calculates risk scores."""
    logger = get_dagster_logger()
    logger.info("Processing daily transaction CSV file with 1500 transactions")
    
    # Simulate reading and processing CSV
    # In production, would read from actual data source
    mock_risk_score = 0.75  # Mock value as per pipeline description
    
    logger.info(f"Calculated risk score: {mock_risk_score}")
    return mock_risk_score

@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    )
)
def route_transaction(context, risk_score: float) -> str:
    """Applies risk model logic and determines routing path based on 0.8 threshold."""
    logger = get_dagster_logger()
    
    if risk_score > 0.8:
        logger.info(f"Risk score {risk_score} exceeds threshold 0.8, routing to manual review")
        return "manual_review"
    else:
        logger.info(f"Risk score {risk_score} within threshold 0.8, routing to auto-approve")
        return "auto_approve"

@op(
    retry_policy=RetryPolicy(