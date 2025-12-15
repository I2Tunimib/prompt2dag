from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

logger = logging.getLogger(__name__)


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    description="Read daily transaction CSV, compute a mock risk score.",
)
def analyze_transactions(csv_path: str | Path = "daily_transactions.csv") -> float:
    """
    Reads a CSV file containing daily transactions, performs a mock analysis,
    and returns a risk score.

    If the CSV does not exist, a dummy DataFrame is created to simulate the
    processing of 1,500 transactions.

    Returns
    -------
    float
        Mock risk score (fixed at 0.75 for demonstration).
    """
    path = Path(csv_path)
    if path.is_file():
        df = pd.read_csv(path)
        logger.info("Loaded %d transactions from %s", len(df), path)
    else:
        # Simulate a DataFrame with 1500 rows
        df = pd.DataFrame(
            {
                "transaction_id": range(1, 1501),
                "amount": [100.0] * 1500,
                "currency": ["USD"] * 1500,
            }
        )
        logger.warning(
            "CSV %s not found. Generated dummy data with %d transactions.",
            path,
            len(df),
        )

    # Placeholder for real risk model computation
    risk_score = 0.75
    logger.info("Computed mock risk score: %.2f", risk_score)
    return risk_score


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Determine routing based on risk score.",
)
def route_transaction(risk_score: float) -> str:
    """
    Decide which downstream path to take based on the risk score.

    Parameters
    ----------
    risk_score : float
        The risk score produced by ``analyze_transactions``.

    Returns
    -------
    str
        ``"manual"`` if risk_score > 0.8, otherwise ``"auto"``.
    """
    branch = "manual" if risk_score > 0.8 else "auto"
    logger.info(
        "Routing decision: risk_score=%.2f -> %s_review", risk_score, branch
    )
    return branch


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Send transaction to manual review queue.",
)
def route_to_manual_review() -> None:
    """
    Placeholder for sending the transaction to a manual review queue.
    """
    logger.info("Transaction routed to manual review queue for analyst inspection.")


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Auto‑approve transaction for payment processing.",
)
def route_to_auto_approve() -> None:
    """
    Placeholder for auto‑approving the transaction.
    """
    logger.info("Transaction auto‑approved and sent to payment processing system.")


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Notify fraud detection team after processing completes.",
)
def send_notification() -> None:
    """
    Sends a completion notification to the fraud detection team.
    """
    logger.info(
        "Fraud detection pipeline completed. Notification sent to the team."
    )


@flow(
    description=(
        "Daily fraud detection triage pipeline. "
        "Scheduled to run once per day (see deployment configuration)."
    )
)
def fraud_detection_pipeline(csv_path: str | Path = "daily_transactions.csv") -> None:
    """
    Orchestrates the fraud detection workflow.

    The flow executes the following steps:
    1. Analyze transactions and compute a risk score.
    2. Determine routing based on the risk score.
    3. Execute either the manual review or auto‑approval branch.
    4. Send a final notification to the fraud detection team.
    """
    risk_score = analyze_transactions(csv_path)
    branch = route_transaction(risk_score)

    if branch == "manual":
        manual_future = route_to_manual_review.submit()
        manual_future.result()
    else:
        auto_future = route_to_auto_approve.submit()
        auto_future.result()

    send_notification()


if __name__ == "__main__":
    # Local execution entry point.
    fraud_detection_pipeline()