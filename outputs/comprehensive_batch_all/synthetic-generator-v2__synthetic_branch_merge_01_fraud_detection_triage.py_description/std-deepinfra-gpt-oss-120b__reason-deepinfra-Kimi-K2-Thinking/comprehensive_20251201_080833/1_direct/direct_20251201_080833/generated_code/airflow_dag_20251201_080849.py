"""Fraud detection triage pipeline DAG.

Processes daily transaction batches, calculates a risk score, routes
transactions based on the score, and sends a final notification.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}


def analyze_transactions(**kwargs):
    """Read transaction CSV and push a mock risk score to XCom."""
    # In a real implementation, read the CSV file here.
    risk_score = 0.75  # Mocked risk score
    kwargs["ti"].xcom_push(key="risk_score", value=risk_score)
    print(f"Analyzed transactions, risk_score={risk_score}")


def decide_route(**kwargs):
    """Branch based on risk score from analyze_transactions."""
    ti = kwargs["ti"]
    risk_score = ti.xcom_pull(key="risk_score", task_ids="analyze_transactions")
    print(f"Risk score retrieved: {risk_score}")

    if risk_score is None:
        raise ValueError("Risk score not found in XCom.")

    if risk_score > 0.8:
        return "route_to_manual_review"
    return "route_to_auto_approve"


def route_to_manual_review(**kwargs):
    """Send high‑risk transaction to manual review queue."""
    # Placeholder for queue integration.
    print("Routing transaction to manual review queue.")


def route_to_auto_approve(**kwargs):
    """Auto‑approve low‑risk transaction for payment processing."""
    # Placeholder for payment system integration.
    print("Auto‑approving transaction for payment processing.")


def send_notification(**kwargs):
    """Notify the fraud detection team that processing is complete."""
    # Placeholder for notification logic (e.g., email, Slack).
    print("Sending completion notification to fraud detection team.")


with DAG(
    dag_id="fraud_detection_triage",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "triage"],
) as dag:
    analyze = PythonOperator(
        task_id="analyze_transactions",
        python_callable=analyze_transactions,
    )

    branch = BranchPythonOperator(
        task_id="route_transaction",
        python_callable=decide_route,
    )

    manual = PythonOperator(
        task_id="route_to_manual_review",
        python_callable=route_to_manual_review,
    )

    auto = PythonOperator(
        task_id="route_to_auto_approve",
        python_callable=route_to_auto_approve,
    )

    notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # Define task dependencies
    analyze >> branch
    branch >> [manual, auto] >> notify