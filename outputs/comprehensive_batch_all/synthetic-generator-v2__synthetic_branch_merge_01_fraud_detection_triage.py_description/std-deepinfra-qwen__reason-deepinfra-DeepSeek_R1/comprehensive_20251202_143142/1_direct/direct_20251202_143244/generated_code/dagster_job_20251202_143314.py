from dagster import op, job, In, Out, RetryPolicy, Failure, success_hook, failure_hook
import pandas as pd

# Resources and Config
# Example resource for email notifications
# class EmailResource:
#     def send_email(self, subject, body):
#         pass

# Example config for job
# class FraudDetectionConfig:
#     def __init__(self, csv_path: str, risk_threshold: float = 0.8):
#         self.csv_path = csv_path
#         self.risk_threshold = risk_threshold

# Ops
@op(out={"risk_score": Out(float)})
def analyze_transactions(context):
    """Reads and processes daily transaction CSV files, calculates risk scores."""
    csv_path = "path/to/daily_transactions.csv"  # Example path
    df = pd.read_csv(csv_path)
    # Mock risk score calculation
    risk_score = 0.75
    return risk_score

@op(ins={"risk_score": In(float)}, out={"is_high_risk": Out(bool)})
def route_transaction(context, risk_score):
    """Applies risk model logic and conditionally routes execution based on risk threshold."""
    risk_threshold = 0.8
    is_high_risk = risk_score > risk_threshold
    return is_high_risk

@op
def route_to_manual_review(context):
    """Sends transaction to manual review queue for analyst inspection."""
    context.log.info("Transaction routed to manual review.")

@op
def route_to_auto_approve(context):
    """Auto-approves transaction for payment processing."""
    context.log.info("Transaction auto-approved.")

@op
def send_notification(context):
    """Sends completion notification to fraud detection team."""
    context.log.info("Notification sent to fraud detection team.")

# Hooks
@success_hook
def notify_success(context):
    context.log.info("Pipeline completed successfully.")

@failure_hook
def notify_failure(context):
    context.log.info("Pipeline failed.")

# Job
@job(
    resource_defs={
        # "email": EmailResource(),
    },
    config={
        # "ops": {
        #     "analyze_transactions": {
        #         "config": {
        #             "csv_path": "path/to/daily_transactions.csv",
        #             "risk_threshold": 0.8
        #         }
        #     }
        # }
    },
    tags={"dagster/pipeline": "fraud_detection"},
    description="Fraud detection triage pipeline for daily transaction batches.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    hooks=[notify_success, notify_failure]
)
def fraud_detection_pipeline():
    risk_score = analyze_transactions()
    is_high_risk = route_transaction(risk_score)

    manual_review = route_to_manual_review().with_hooks({notify_failure})
    auto_approve = route_to_auto_approve().with_hooks({notify_failure})

    send_notification(
        manual_review if is_high_risk else auto_approve
    )

# Launch pattern
if __name__ == '__main__':
    result = fraud_detection_pipeline.execute_in_process()