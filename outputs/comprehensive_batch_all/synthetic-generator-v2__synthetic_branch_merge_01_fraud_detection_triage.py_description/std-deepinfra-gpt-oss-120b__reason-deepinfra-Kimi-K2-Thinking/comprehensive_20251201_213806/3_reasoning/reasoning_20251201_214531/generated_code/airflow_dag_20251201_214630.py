from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'fraud-detection-team',
    'depends_on_past': False,
    'email': ['fraud-team@example.com'],  # Configure with actual team email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def analyze_transactions(**context):
    """Reads daily transaction CSV files and calculates risk scores."""
    logger.info("Processing daily transaction batch")
    
    # Simulate reading transaction CSV file
    # Production path: /data/transactions/daily/{{ ds }}.csv
    transaction_count = 1500
    logger.info(f"Analyzing {transaction_count} transactions")
    
    # Mock risk score calculation
    risk_score = 0.75
    logger.info(f"Calculated risk score: {risk_score}")
    
    return risk_score


def route_transaction(**context):
    """Routes execution based on risk score threshold comparison."""
    ti = context['ti']
    risk_score = ti.xcom_pull(task_ids='analyze_transactions')
    
    if risk_score is None:
        raise ValueError("No risk score found in XCom")
    
    logger.info(f"Routing based on risk score: {risk_score}")
    
    if risk_score > 0.8:
        logger.info("Routing to manual review")
        return 'route_to_manual_review'
    else:
        logger.info("Routing to auto-approval")
        return 'route_to_auto_approve'


def route_to_manual_review(**context):
    """Sends transaction to manual review queue for analyst inspection."""
    logger.info("Sending transaction to manual review queue")
    # Production: integrate with manual review system API
    return "Transaction routed to manual review"


def route_to_auto_approve(**context):
    """Auto-approves transaction for payment processing."""
    logger.info("Auto-approving transaction for payment processing")
    # Production: integrate with payment processing system API
    return "Transaction auto-approved"


def send_notification(**context):
    """Sends completion notification to fraud detection team."""
    logger.info("Sending completion notification to fraud detection team")
    # Production: integrate with email/notification service
    return "Notification sent"


with DAG(
    'fraud_detection_triage_pipeline',
    default_args=default_args,
    description='Daily fraud detection triage pipeline with conditional routing',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fraud_detection', 'triage'],
) as dag:

    analyze_task = PythonOperator(
        task_id='analyze_transactions',
        python_callable=analyze_transactions,
    )

    route_task = BranchPythonOperator(
        task_id='route_transaction',
        python_callable=route_transaction,
    )

    manual_review_task = PythonOperator(
        task_id='route_to_manual_review',
        python_callable=route_to_manual_review,
    )

    auto_approve_task = PythonOperator(
        task_id='route_to_auto_approve',
        python_callable=route_to_auto_approve,
    )

    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    analyze_task >> route_task
    route_task >> [manual_review_task, auto_approve_task]
    [manual_review_task, auto_approve_task] >> notify_task