from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['fraud-team@example.com'],
}

with DAG(
    dag_id='fraud_detection_triage_pipeline',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['fraud_detection', 'triage'],
) as dag:
    
    def analyze_transactions(**context):
        """Simulate reading daily transaction CSV and calculating risk score."""
        # In production, read from /path/to/daily/transactions.csv
        # Process 1500 transactions and calculate actual risk scores
        risk_score = 0.75  # Mock risk score for demonstration
        return risk_score
    
    def route_transaction(**context):
        """Route transaction based on risk score threshold."""
        ti = context['ti']
        risk_score = ti.xcom_pull(task_ids='analyze_transactions')
        
        if risk_score > 0.8:
            return 'route_to_manual_review'
        return 'route_to_auto_approve'
    
    def route_to_manual_review(**context):
        """Send high-risk transaction to manual review queue."""
        print("Transaction routed to manual review queue for analyst inspection")
    
    def route_to_auto_approve(**context):
        """Auto-approve low-risk transaction for payment processing."""
        print("Transaction auto-approved for payment processing")
    
    def send_notification(**context):
        """Send completion notification to fraud detection team."""
        print("Sending completion notification to fraud detection team")
    
    analyze_transactions_task = PythonOperator(
        task_id='analyze_transactions',
        python_callable=analyze_transactions,
    )
    
    route_transaction_task = BranchPythonOperator(
        task_id='route_transaction',
        python_callable=route_transaction,
    )
    
    route_to_manual_review_task = PythonOperator(
        task_id='route_to_manual_review',
        python_callable=route_to_manual_review,
    )
    
    route_to_auto_approve_task = PythonOperator(
        task_id='route_to_auto_approve',
        python_callable=route_to_auto_approve,
    )
    
    send_notification_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        trigger_rule='none_failed',
    )
    
    analyze_transactions_task >> route_transaction_task
    route_transaction_task >> [route_to_manual_review_task, route_to_auto_approve_task]
    [route_to_manual_review_task, route_to_auto_approve_task] >> send_notification_task