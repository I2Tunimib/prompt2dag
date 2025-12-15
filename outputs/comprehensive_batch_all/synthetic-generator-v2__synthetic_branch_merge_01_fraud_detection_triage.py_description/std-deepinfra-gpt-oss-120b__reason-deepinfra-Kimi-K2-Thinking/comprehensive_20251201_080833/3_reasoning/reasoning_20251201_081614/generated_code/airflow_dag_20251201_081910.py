from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def analyze_transactions(**kwargs):
    """
    Simulates reading daily transaction CSV and calculating risk score.
    Returns mock risk score of 0.75 via XCom.
    """
    mock_risk_score = 0.75
    kwargs['ti'].xcom_push(key='risk_score', value=mock_risk_score)
    return mock_risk_score


def route_transaction(**kwargs):
    """
    Branch function that routes based on risk score threshold.
    Returns task_id of next task to execute.
    """
    risk_score = kwargs['ti'].xcom_pull(
        task_ids='analyze_transactions',
        key='risk_score'
    )
    
    if risk_score is None:
        return 'route_to_manual_review'
    
    if risk_score > 0.8:
        return 'route_to_manual_review'
    else:
        return 'route_to_auto_approve'


def route_to_manual_review(**kwargs):
    """
    Simulates sending high-risk transaction to manual review queue.
    """
    risk_score = kwargs['ti'].xcom_pull(
        task_ids='analyze_transactions',
        key='risk_score'
    )
    print(f"Routing transaction with risk score {risk_score} to manual review queue")
    return "Sent to manual review"


def route_to_auto_approve(**kwargs):
    """
    Simulates auto-approving low-risk transaction for payment processing.
    """
    risk_score = kwargs['ti'].xcom_pull(
        task_ids='analyze_transactions',
        key='risk_score'
    )
    print(f"Auto-approving transaction with risk score {risk_score} for payment processing")
    return "Auto-approved"


def send_notification(**kwargs):
    """
    Sends completion notification to fraud detection team.
    Executes after either branch completes.
    """
    print("Sending completion notification to fraud detection team")
    return "Notification sent"


default_args = {
    'owner': 'fraud-detection-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['fraud-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fraud_detection_triage_pipeline',
    default_args=default_args,
    description='Daily fraud detection triage pipeline with risk-based routing',
    schedule_interval='@daily',
    catchup=False,
    tags=['fraud', 'detection', 'triage'],
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