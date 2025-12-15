from dagster import op, job, In, Out, RetryPolicy, Failure, success, graph, resource, Field, String, Int, daily_schedule, ScheduleDefinition, ScheduleEvaluationContext

# Resources
@resource(config_schema={"email": Field(String, default_value="fraud_team@example.com")})
def email_resource(context):
    return context.resource_config["email"]

# Ops
@op(out={"risk_score": Out(dagster_type=float)})
def analyze_transactions(context):
    """Reads and processes daily transaction CSV files, calculates risk scores."""
    # Mocking the risk score for demonstration
    risk_score = 0.75
    context.log.info(f"Calculated risk score: {risk_score}")
    return risk_score

@op(ins={"risk_score": In(dagster_type=float)}, out={"manual_review": Out(dagster_type=bool)})
def route_transaction(context, risk_score):
    """Applies risk model logic to route transactions based on risk threshold."""
    threshold = 0.8
    manual_review = risk_score > threshold
    context.log.info(f"Routing to manual review: {manual_review}")
    return manual_review

@op
def route_to_manual_review(context):
    """Sends transaction to manual review queue for analyst inspection."""
    context.log.info("Transaction routed to manual review queue.")

@op
def route_to_auto_approve(context):
    """Auto-approves transaction for payment processing."""
    context.log.info("Transaction auto-approved for payment processing.")

@op
def send_notification(context):
    """Sends completion notification to fraud detection team."""
    context.log.info("Sending completion notification to fraud detection team.")

# Job
@job(
    resource_defs={"email": email_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Fraud detection triage pipeline for daily transaction batches."
)
def fraud_detection_pipeline():
    risk_score = analyze_transactions()
    manual_review = route_transaction(risk_score)
    
    manual_review_path = route_to_manual_review().with_hooks({manual_review})
    auto_approve_path = route_to_auto_approve().with_hooks({~manual_review})
    
    send_notification(manual_review_path, auto_approve_path)

# Schedule
@daily_schedule(
    pipeline_name="fraud_detection_pipeline",
    start_date="2024-01-01",
    execution_time="00:00",
    execution_timezone="UTC",
    name="daily_fraud_detection",
    should_execute=lambda _context: True,
    tags={},
    description="Daily execution of the fraud detection triage pipeline."
)
def daily_fraud_detection_schedule(context: ScheduleEvaluationContext):
    return {}

# Launch pattern
if __name__ == '__main__':
    result = fraud_detection_pipeline.execute_in_process()