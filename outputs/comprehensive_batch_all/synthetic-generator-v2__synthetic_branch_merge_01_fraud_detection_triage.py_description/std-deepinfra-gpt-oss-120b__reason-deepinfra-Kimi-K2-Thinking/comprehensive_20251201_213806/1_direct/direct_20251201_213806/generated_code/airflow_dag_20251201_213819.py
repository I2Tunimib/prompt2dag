from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def analyze_transactions(**context):
    """
    Mock analysis of daily transaction CSV files.
    Calculates a risk score and pushes it to XCom.
    """
    # In a real implementation, read CSV and compute risk scores.
    risk_score = 0.75  # Mocked risk score
    context["ti"].xcom_push(key="risk_score", value=risk_score)
    return risk_score


def route_transaction(**context):
    """
    Branching logic based on risk score.
    Returns the task_id of the next branch.
    """
    risk_score = context["ti"].xcom_pull(key="risk_score", task_ids="analyze_transactions")
    if risk_score is None:
        raise ValueError("Risk score not found in XCom.")
    if risk_score > 0.8:
        return "route_to_manual_review"
    return "route_to_auto_approve"


def route_to_manual_review(**context):
    """
    Sends high‑risk transaction to manual review queue.
    """
    # Placeholder for integration with a message queue or service.
    print("Transaction routed to manual review queue.")


def route_to_auto_approve(**context):
    """
    Auto‑approves low‑risk transaction for payment processing.
    """
    # Placeholder for integration with payment processing system.
    print("Transaction auto‑approved for payment processing.")


def send_notification(**context):
    """
    Sends a completion notification to the fraud detection team.
    """
    # Placeholder for email or messaging notification.
    print("Fraud detection pipeline completed. Notification sent to team.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}

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
        provide_context=True,
    )

    branch = BranchPythonOperator(
        task_id="route_transaction",
        python_callable=route_transaction,
        provide_context=True,
    )

    manual_review = PythonOperator(
        task_id="route_to_manual_review",
        python_callable=route_to_manual_review,
        provide_context=True,
    )

    auto_approve = PythonOperator(
        task_id="route_to_auto_approve",
        python_callable=route_to_auto_approve,
        provide_context=True,
    )

    notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    analyze >> branch
    branch >> [manual_review, auto_approve] >> notify