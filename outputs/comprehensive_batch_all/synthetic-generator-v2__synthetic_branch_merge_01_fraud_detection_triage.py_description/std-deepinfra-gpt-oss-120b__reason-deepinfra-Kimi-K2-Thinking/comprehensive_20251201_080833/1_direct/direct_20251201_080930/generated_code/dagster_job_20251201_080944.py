from datetime import datetime, timezone

from dagster import (
    DefaultScheduleStatus,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
    schedule,
)


@op(
    out=Out(float),
    description="Read daily transaction CSV and calculate a mock risk score.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def analyze_transactions(context) -> float:
    """
    Mock implementation: reads a CSV (omitted) and returns a fixed risk score.
    """
    # In a real implementation, you would read a CSV file here.
    risk_score = 0.75
    context.log.info(f"Analyzed transactions, computed risk score: {risk_score}")
    return risk_score


@op(
    ins={"risk_score": In(float)},
    out=Out(float),
    description="Pass through the risk score for downstream routing.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_transaction(context, risk_score: float) -> float:
    """
    Simple pass‑through op that could contain additional routing logic.
    """
    context.log.info(f"Routing decision based on risk score: {risk_score}")
    return risk_score


@op(
    ins={"risk_score": In(float)},
    out=Out(str),
    description="Send high‑risk transactions to manual review.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_to_manual_review(context, risk_score: float) -> str:
    """
    Executes only when risk_score > 0.8. Returns a status message.
    """
    if risk_score > 0.8:
        # Placeholder for sending to a manual review queue.
        context.log.info(
            f"Risk score {risk_score} exceeds threshold; routing to manual review."
        )
        status = "sent_to_manual_review"
    else:
        context.log.info(
            f"Risk score {risk_score} does not exceed threshold; skipping manual review."
        )
        status = "manual_review_skipped"
    return status


@op(
    ins={"risk_score": In(float)},
    out=Out(str),
    description="Auto‑approve low‑risk transactions.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_to_auto_approve(context, risk_score: float) -> str:
    """
    Executes only when risk_score <= 0.8. Returns a status message.
    """
    if risk_score <= 0.8:
        # Placeholder for auto‑approval logic.
        context.log.info(
            f"Risk score {risk_score} within threshold; auto‑approving transaction."
        )
        status = "auto_approved"
    else:
        context.log.info(
            f"Risk score {risk_score} exceeds threshold; skipping auto‑approval."
        )
        status = "auto_approval_skipped"
    return status


@op(
    ins={
        "manual_status": In(str),
        "auto_status": In(str),
    },
    description="Send a completion notification after either branch finishes.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def send_notification(context, manual_status: str, auto_status: str) -> None:
    """
    Merge point that notifies the fraud detection team.
    """
    context.log.info(
        f"Notification: manual review status = {manual_status}, auto‑approval status = {auto_status}"
    )
    # Placeholder for sending an email or Slack message to the team.
    context.log.info("Fraud detection team notified of pipeline completion.")


@job(
    description="Fraud detection triage pipeline with conditional routing and merge.",
)
def fraud_detection_job():
    risk = analyze_transactions()
    routed_score = route_transaction(risk)
    manual_status = route_to_manual_review(routed_score)
    auto_status = route_to_auto_approve(routed_score)
    send_notification(manual_status, auto_status)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=fraud_detection_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    catchup=False,
    description="Daily execution of the fraud detection triage pipeline.",
)
def daily_fraud_detection_schedule():
    # No additional run config needed for this simple example.
    return {}


if __name__ == "__main__":
    result = fraud_detection_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")