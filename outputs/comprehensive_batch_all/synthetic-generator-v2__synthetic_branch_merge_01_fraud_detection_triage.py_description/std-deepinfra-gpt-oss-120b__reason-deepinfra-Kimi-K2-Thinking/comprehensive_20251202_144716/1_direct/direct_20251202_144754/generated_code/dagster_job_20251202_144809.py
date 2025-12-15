from datetime import timedelta
from typing import Optional

from dagster import (
    In,
    Nothing,
    Out,
    RetryPolicy,
    SkipReason,
    ScheduleDefinition,
    DefaultScheduleStatus,
    job,
    op,
    resource,
    ScheduleDefinition,
    schedule,
)


@resource
def email_resource(_):
    """Simple email resource stub."""
    class EmailClient:
        def send_email(self, subject: str, body: str, to: str):
            # In a real implementation, integrate with an email service.
            print(f"Sending email to {to}: {subject}\\n{body}")

    return EmailClient()


@op(
    out=Out(float),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Read daily transaction CSV and compute a mock risk score.",
)
def analyze_transactions() -> float:
    # Placeholder for CSV reading and risk calculation.
    # Returns a mock risk score of 0.75.
    risk_score = 0.75
    print(f"Analyzed transactions, mock risk score: {risk_score}")
    return risk_score


@op(
    out=Out(float),
    description="Pass through the risk score for routing decisions.",
)
def route_transaction(risk_score: float) -> float:
    print(f"Routing decision based on risk score: {risk_score}")
    return risk_score


@op(
    out=Out(Nothing),
    description="Send high‑risk transaction to manual review queue.",
)
def route_to_manual_review(risk_score: float) -> Nothing:
    if risk_score > 0.8:
        # Simulate sending to a manual review queue.
        print(f"Risk score {risk_score} > 0.8 – routing to manual review.")
        # Insert integration code here.
        return Nothing
    else:
        raise SkipReason("Risk score not high enough for manual review.")


@op(
    out=Out(Nothing),
    description="Auto‑approve low‑risk transaction.",
)
def route_to_auto_approve(risk_score: float) -> Nothing:
    if risk_score <= 0.8:
        # Simulate auto‑approval.
        print(f"Risk score {risk_score} <= 0.8 – auto‑approving transaction.")
        # Insert integration code here.
        return Nothing
    else:
        raise SkipReason("Risk score too high for auto‑approval.")


@op(
    required_resource_keys={"email"},
    ins={
        "manual": In(Nothing, optional=True),
        "auto": In(Nothing, optional=True),
    },
    description="Notify the fraud detection team after processing completes.",
)
def send_notification(context, manual: Optional[Nothing] = None, auto: Optional[Nothing] = None) -> None:
    # This op runs after either branch completes (skipped branches are considered successful).
    subject = "Daily Fraud Detection Pipeline Completed"
    body = "The daily fraud detection pipeline has finished processing."
    to = "fraud-team@example.com"
    context.resources.email.send_email(subject, body, to)
    print("Notification sent to fraud detection team.")


@job(
    resource_defs={"email": email_resource},
    description="Fraud detection triage pipeline with conditional routing.",
)
def fraud_detection_job():
    risk = analyze_transactions()
    routed_risk = route_transaction(risk)
    manual = route_to_manual_review(routed_risk)
    auto = route_to_auto_approve(routed_risk)
    send_notification(manual=manual, auto=auto)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=fraud_detection_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily schedule for the fraud detection pipeline.",
)
def daily_fraud_detection_schedule():
    return {}


if __name__ == "__main__":
    result = fraud_detection_job.execute_in_process()
    assert result.success