from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import csv
import random
from typing import List, Dict, Any

default_args = {
    'owner': 'content-moderation-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

CSV_FILE_PATH = '/tmp/user_content.csv'

def scan_csv(**context: Any) -> str:
    """Scans CSV file and returns metadata about content items."""
    # Generate mock content for demonstration
    mock_content = [
        {'user_id': f'user_{i}', 'content': f'content_{i}', 'toxicity_score': random.uniform(0.0, 1.0)}
        for i in range(10)
    ]
    
    # Write mock data to CSV
    with open(CSV_FILE_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['user_id', 'content', 'toxicity_score'])
        writer.writeheader()
        writer.writerows(mock_content)
    
    # Read and process CSV
    with open(CSV_FILE_PATH, 'r') as f:
        reader = csv.DictReader(f)
        items = list(reader)
        total_items = len(items)
    
    ti = context['ti']
    ti.xcom_push(key='content_items', value=items)
    ti.xcom_push(key='total_items', value=total_items)
    
    return f"Scanned {total_items} content items"

def toxicity_check(**context: Any) -> str:
    """Evaluates toxicity level and routes to appropriate downstream task."""
    ti = context['ti']
    items = ti.xcom_pull(task_ids='scan_csv', key='content_items')
    
    if not items:
        raise ValueError("No content items found")
    
    toxicity_scores = [float(item.get('toxicity_score', 0)) for item in items]
    avg_toxicity = sum(toxicity_scores) / len(toxicity_scores)
    
    threshold = 0.7
    if avg_toxicity >= threshold:
        return 'remove_and_flag_content'
    else:
        return 'publish_content'

def remove_and_flag_content(**context: Any) -> str:
    """Removes toxic content and flags user accounts for review."""
    ti = context['ti']
    items = ti.xcom_pull(task_ids='scan_csv', key='content_items')
    
    toxic_items = [item for item in items if float(item.get('toxicity_score', 0)) >= 0.7]
    flagged_users = list({item['user_id'] for item in toxic_items})
    
    result = {
        'action': 'remove_and_flag',
        'toxic_items_count': len(toxic_items),
        'flagged_users': flagged_users
    }
    ti.xcom_push(key='moderation_result', value=result)
    
    return f"Removed {len(toxic_items)} toxic items, flagged {len(flagged_users)} users"

def publish_content(**context: Any) -> str:
    """Publishes safe content to the platform."""
    ti = context['ti']
    items = ti.xcom_pull(task_ids='scan_csv', key='content_items')
    
    safe_items = [item for item in items if float(item.get('toxicity_score', 0)) < 0.7]
    published_count = len(safe_items)
    
    result = {
        'action': 'publish',
        'published_items_count': published_count
    }
    ti.xcom_push(key='moderation_result', value=result)
    
    return f"Published {published_count} safe items"

def audit_log(**context: Any) -> str:
    """Creates final audit log entry after moderation completes."""
    ti = context['ti']
    
    remove_result = ti.xcom_pull(task_ids='remove_and_flag_content', key='moderation_result')
    publish_result = ti.xcom_pull(task_ids='publish_content', key='moderation_result')
    
    if remove_result:
        action_taken = 'remove_and_flag'
        details = remove_result
    elif publish_result:
        action_taken = 'publish'
        details = publish_result
    else:
        raise ValueError("No moderation result found")
    
    audit_entry = {
        'action_taken': action_taken,
        'details': details,
        'timestamp': context['execution_date'].isoformat()
    }
    
    print(f"Audit Log Entry: {audit_entry}")
    
    return "Audit log created successfully"

with DAG(
    dag_id='content_moderation_pipeline',
    default_args=default_args,
    description='Content moderation pipeline with toxicity checking and conditional routing',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['content-moderation', 'toxicity-detection'],
) as dag:
    
    task_scan_csv = PythonOperator(
        task_id='scan_csv',
        python_callable=scan_csv,
    )
    
    task_toxicity_check = BranchPythonOperator(
        task_id='toxicity_check',
        python_callable=toxicity_check,
    )
    
    task_remove_and_flag_content = PythonOperator(
        task_id='remove_and_flag_content',
        python_callable=remove_and_flag_content,
    )
    
    task_publish_content = PythonOperator(
        task_id='publish_content',
        python_callable=publish_content,
    )
    
    task_audit_log = PythonOperator(
        task_id='audit_log',
        python_callable=audit_log,
        trigger_rule='one_success',
    )
    
    task_scan_csv >> task_toxicity_check
    task_toxicity_check >> [task_remove_and_flag_content, task_publish_content]
    task_remove_and_flag_content >> task_audit_log
    task_publish_content >> task_audit_log