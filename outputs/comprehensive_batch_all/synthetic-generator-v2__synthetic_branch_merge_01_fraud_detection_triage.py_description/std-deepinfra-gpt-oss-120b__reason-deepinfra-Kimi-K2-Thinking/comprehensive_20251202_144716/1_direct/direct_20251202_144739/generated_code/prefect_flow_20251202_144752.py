from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


@task(
    retries=2,
    retry_delay_seconds=300,
    name="Analyze Transactions",
    description="Read daily CSV and compute a mock risk score.",
)
def analyze_transactions(csv_path: str) -> float:
    """
    Simulate reading a CSV file and calculating a risk score.

    Args:
        csv_path: Path to the transaction CSV file.

    Returns:
        A mock risk score (float). In a real implementation this would be
        calculated from the data.
    """
    # Placeholder implementation – return a fixed mock score.
    logger.info("Analyzing transactions from %s", csv_path)
    mock_risk_score = 0.75
    logger.info("Computed mock risk score: %s", mock_risk_score)
    return mock_risk_score


@task(
    name="Route Transaction",
    description="Determine which execution path to take based on risk score.",
)
def route_transaction(risk_score: float) -> str:
    """
    Decide which branch to follow.

    Args:
        risk_score: The risk score produced by ``analyze_transactions``.

    Returns:
        A string identifier for the chosen branch: ``"manual"`` or ``"auto"``.
    """
    branch = "manual" if risk_score > 0.8 else "auto"
    logger.info("Risk score %s leads to %s branch", risk_score, branch)
    return branch


@task(
    name="Route to Manual Review",
    description="Send high‑risk transaction to manual review queue.",
)
def route_to_manual_review(risk_score: float) -> None:
    """
    Placeholder for sending a transaction to a manual review queue.

    Args:
        risk_score: The risk score for logging/context.
    """
    logger.info(
        "Routing transaction with risk score %s to manual review queue.", risk_score
    )
    # Insert integration with the manual review system here.


@task(
    name="Route to Auto Approve",
    description="Auto‑approve low‑risk transaction for payment processing.",
)
def route_to_auto_approve(risk_score: float) -> None:
    """
    Placeholder for auto‑approving a transaction.

    Args:
        risk_score: The risk score for logging/context.
    """
    logger.info(
        "Auto‑approving transaction with risk score %s for payment processing.",
        risk_score,
    )
    # Insert integration with the payment processing system here.


@task(
    name="Send Notification",
    description="Notify the fraud detection team after processing completes.",
)
def send_notification(branch: str) -> None:
    """
    Send a completion notification to the fraud detection team.

    Args:
        branch: The execution path that was taken ("manual" or "auto").
    """
    logger.info(
        "Sending completion notification: transaction processed via %s path.", branch
    )
    # Insert integration with the notification/email system here.


@flow(name="Fraud Detection Triage")
def fraud_detection_triage(csv_path: str = "transactions.csv") -> None:
    """
    Orchestrates the fraud detection triage pipeline.

    The flow:
    1. Analyze transactions and compute a risk score.
    2. Route based on the risk score to either manual review or auto‑approve.
    3. After the chosen branch completes, send a notification.

    Args:
        csv_path: Path to the daily transaction CSV file.
    """
    risk_score = analyze_transactions(csv_path)
    branch = route_transaction(risk_score)

    # Branch execution – only one path runs, but we use .submit() for consistency.
    if branch == "manual":
        manual_future = route_to_manual_review.submit(risk_score)
        manual_future.result()  # Wait for completion before notification.
        send_notification("manual")
    else:
        auto_future = route_to_auto_approve.submit(risk_score)
        auto_future.result()
        send_notification("auto")


# Schedule configuration (to be applied in deployment):
# - Interval: daily
# - Start date: 2024-01-01
# - Catchup: disabled
# - Retry policy: 2 retries with 5‑minute delay (configured per task)
# - Failure handling: email notifications can be added via Prefect alerts.

if __name__ == "__main__":
    fraud_detection_triage()