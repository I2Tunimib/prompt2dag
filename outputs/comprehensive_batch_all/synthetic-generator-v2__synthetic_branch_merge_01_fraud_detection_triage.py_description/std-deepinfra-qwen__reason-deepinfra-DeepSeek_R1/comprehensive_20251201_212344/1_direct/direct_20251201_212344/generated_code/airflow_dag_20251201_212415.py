from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['fraud_detection_team@example.com']
}

def analyze_transactions():
    """Reads and processes daily transaction CSV files, calculates risk scores, and returns a mock risk score."""
    # Mock risk score for demonstration purposes
    return 0.75

def route_transaction(**kwargs):
    """Branches transactions based on the risk score threshold of 0.8."""
    ti = kwargs['ti']
    risk_score = ti.xcom_pull(task_ids='analyze_transactions')
    if risk_score > 0.8:
        return 'route_to_manual_review'
    else:
        return 'route_to_auto_approve'

def route_to_manual_review():
    """Sends high-risk transactions to the manual review queue for analyst inspection."""
    print("Transaction routed to manual review queue.")

def route_to_auto_approve():
    """Auto-approves transactions for payment processing."""
    print("Transaction auto-approved for payment processing.")

def send_notification():
    """Sends a completion notification to the fraud detection team."""
    print("Notification sent to fraud detection team.")

with DAG(
    dag_id='fraud_detection_triage_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Fraud detection triage pipeline for daily transaction batches.'
) as dag:

    analyze_transactions_task = PythonOperator(
        task_id='analyze_transactions',
        python_callable=analyze_transactions
    )

    route_transaction_task = BranchPythonOperator(
        task_id='route_transaction',
        python_callable=route_transaction,
        provide_context=True
    )

    route_to_manual_review_task = PythonOperator(
        task_id='route_to_manual_review',
        python_callable=route_to_manual_review
    )

    route_to_auto_approve_task = PythonOperator(
        task_id='route_to_auto_approve',
        python_callable=route_to_auto_approve
    )

    send_notification_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    analyze_transactions_task >> route_transaction_task
    route_transaction_task >> route_to_manual_review_task
    route_transaction_task >> route_to_auto_approve_task
    route_to_manual_review_task >> send_notification_task
    route_to_auto_approve_task >> send_notification_task