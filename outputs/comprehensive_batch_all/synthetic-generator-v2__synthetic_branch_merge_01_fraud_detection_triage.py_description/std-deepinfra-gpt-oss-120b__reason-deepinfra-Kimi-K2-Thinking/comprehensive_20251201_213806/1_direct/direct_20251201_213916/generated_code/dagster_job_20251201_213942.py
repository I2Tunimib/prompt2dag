from dagster import (
    op,
    job,
    In,
    Out,
    Nothing,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    Definitions,
    DagsterSkipReason,
)


@op(
    out=Out(float),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Read transaction CSV and calculate a mock risk score.",
)
def analyze_transactions(context) -> float:
    context.log.info("Analyzing transactions from CSV...")
    # Mock implementation: return a fixed risk score.
    risk_score = 0.75
    context.log.info(f"Calculated mock risk score: {risk_score}")
    return risk_score


@op(
    description="Pass through the risk score and log it for debugging.",
)
def route_transaction(context, risk_score: float) -> float:
    context.log.info(f"Risk score received: {risk_score}")
    return risk_score


@op(
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Send high‑risk transactions to manual review.",
)
def route_to_manual_review(context, risk_score: float) -> Nothing:
    if risk_score > 0.8:
        context.log.info("Routing transaction to manual review queue.")
        # Placeholder for queue integration.
        return None
    else:
        raise DagsterSkipReason("Risk score below threshold; skipping manual review.")


@op(
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Auto‑approve low‑risk transactions.",
)
def route_to_auto_approve(context, risk_score: float) -> Nothing:
    if risk_score <= 0.8:
        context.log.info("Auto‑approving transaction for payment processing.")
        # Placeholder for payment processing integration.
        return None
    else:
        raise DagsterSkipReason("Risk score above threshold; skipping auto‑approval.")


@op(
    ins={
        "manual_done": In(Nothing, description="Completion of manual review branch."),
        "auto_done": In(Nothing, description="Completion of auto‑approve branch."),
    },
    description="Notify the fraud detection team after processing completes.",
)
def send_notification(context) -> None:
    context.log.info("Sending final notification to the fraud detection team.")
    # Placeholder for notification integration.
    return None


@job
def fraud_detection_job():
    """Daily fraud detection triage pipeline."""
    risk_score = analyze_transactions()
    routed_score = route_transaction(risk_score)
    manual_done = route_to_manual_review(routed_score)
    auto_done = route_to_auto_approve(routed_score)
    send_notification(manual_done, auto_done)


daily_fraud_detection_schedule = ScheduleDefinition(
    job=fraud_detection_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC.
    default_status=DefaultScheduleStatus.RUNNING,
    description="Schedule for the daily fraud detection triage pipeline.",
)


defs = Definitions(
    jobs=[fraud_detection_job],
    schedules=[daily_fraud_detection_schedule],
)


if __name__ == "__main__":
    result = fraud_detection_job.execute_in_process()
    assert result.success, "Job execution failed."