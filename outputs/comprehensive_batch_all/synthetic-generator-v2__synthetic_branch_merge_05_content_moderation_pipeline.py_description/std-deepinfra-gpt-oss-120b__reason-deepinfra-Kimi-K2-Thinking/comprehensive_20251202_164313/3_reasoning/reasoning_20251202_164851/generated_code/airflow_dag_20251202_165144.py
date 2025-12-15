from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import csv
import json
from typing import List, Dict, Any

CSV_FILE_PATH = '/tmp/user_content.csv'
TOXICITY_THRESHOLD = 0.7

default_args = {
    'owner': 'content-moderation-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def scan_csv(**context: Any) -> str:
    """Scans CSV file and returns metadata about user-generated content."""
    content_items: List[Dict[str, str]] = []
    
    try:
        with open(CSV_FILE_PATH, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                content_items.append(dict(row))
    except FileNotFoundError:
        with open(CSV_FILE_PATH, 'w', newline='') as csvfile:
            fieldnames = ['user_id', 'content_id', 'content_text']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for i in range(10):
                writer.writerow({
                    'user_id': f'user_{i:03d}',
                    'content_id': f'content_{i:03d}',
                    'content_text': f'Sample content text {i}'
                })
        
        with open(CSV_FILE_PATH, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                content_items.append(dict(row))
    
    item_count = len(content_items)
    ti = context['task_instance']
    ti.xcom_push(key='item_count', value=item_count)
    ti.xcom_push(key='content_items', value=content_items)
    
    return f"Found {item_count} content items"

def toxicity_check(**context: Any) -> str:
    """Evaluates toxicity level and routes to appropriate downstream task."""
    ti = context['task_instance']
    content_items = ti.xcom_pull(task_ids='scan_csv', key='content_items') or []
    
    if not content_items:
        return 'publish_content'
    
    toxicity_scores = []
    for item in content_items:
        content_id = item.get('content_id', '')
        score = (hash(content_id) % 1000) / 1000.0
        toxicity_scores.append(score)
    
    avg_toxicity = sum(toxicity_scores) / len(toxicity_scores)
    ti.xcom_push(key='toxicity_score', value=avg_toxicity)
    ti.xcom_push(key='toxicity_scores', value=toxicity_scores)
    
    if avg_toxicity > TOXICITY_THRESHOLD:
        return 'remove_and_flag_content'
    else:
        return 'publish_content'

def remove_and_flag_content(**context: Any) -> str:
    """Removes toxic content and flags user accounts."""
    ti = context['task_instance']
    content_items = ti.xcom_pull(task_ids='scan_csv', key='content_items') or []
    toxicity_scores = ti.xcom_pull(task_ids='toxicity_check', key='toxicity_scores') or []
    
    removed_items = []
    flagged_users = set()
    
    for item, score in zip(content_items, toxicity_scores):
        if score > TOXICITY_THRESHOLD:
            removed_items.append({
                'content_id': item.get('content_id'),
                'user_id': item.get('user_id'),
                'toxicity_score': score
            })
            flagged_users.add(item.get('user_id'))
    
    ti.xcom_push(key='action', value='remove_and_flag')
    ti.xcom_push(key='removed_items', value=removed_items)
    ti.xcom_push(key='flagged_users', value=list(flagged_users))
    
    return f"Removed {len(removed_items)} items, flagged {len(flagged_users)} users"

def publish_content(**context: Any) -> str:
    """Publishes safe content to the platform."""
    ti = context['task_instance']
    content_items = ti.xcom_pull(task_ids='scan_csv', key='content_items') or []
    toxicity_scores = ti.xcom_pull(task_ids='toxicity_check', key='toxicity_scores') or []
    
    published_items = []
    
    for item, score in zip(content_items, toxicity_scores):
        if score <= TOXICITY_THRESHOLD:
            published_items.append({
                'content_id': item.get('content_id'),
                'user_id': item.get('user_id'),
                'toxicity_score': score
            })
    
    ti.xcom_push(key='action', value='publish')
    ti.xcom_push(key='published_items', value=published_items)
    
    return f"Published {len(published_items)} items"

def audit_log(**context: Any) -> str:
    """Creates a unified audit log entry after processing."""
    ti = context['task_instance']
    
    item_count = ti.xcom_pull(task_ids='scan_csv', key='item_count') or 0
    toxicity_score = ti.xcom_pull(task_ids='toxicity_check', key='toxicity_score') or 0.0
    
    action = ti.xcom_pull(task_ids='remove_and_flag_content', key='action')
    if action == 'remove_and_flag':
        removed_items = ti.xcom_pull(task_ids='remove_and_flag_content', key='removed_items') or []
        flagged_users = ti.xcom_pull(task_ids='remove_and_flag_content', key='flagged_users') or []
        published_items = []
    else:
        action = ti.xcom_pull(task_ids='publish_content', key='action') or 'unknown'
        published_items = ti.xcom_pull(task_ids='publish_content', key='published_items') or []
        removed_items = []
        flagged_users = []
    
    audit_entry = {
        'timestamp': context['execution_date'].isoformat(),
        'item_count': item_count,
        'avg_toxicity_score': round(toxicity_score, 4),
        'action_taken': action,
        'removed_items_count': len(removed_items),
        'flagged_users_count': len(flagged_users),
        'published_items_count': len(published_items),
        'details': {
            'removed_items': removed_items,
            'flagged_users': flagged_users,
            'published_items': published_items
        }
    }
    
    print(f"AUDIT_LOG: {json.dumps(audit_entry, indent=2)}")
    ti.xcom_push(key='audit_entry', value=audit_entry)
    
    return "Audit log created successfully"

with DAG(
    dag_id='content_moderation_pipeline',
    default_args=default_args,
    description='Content moderation pipeline with toxicity checking and conditional routing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['content', 'moderation', 'toxicity'],
) as dag:
    
    scan_csv_task = PythonOperator(
        task_id='scan_csv',
        python_callable=scan_csv,
    )
    
    toxicity_check_task = BranchPythonOperator(
        task_id='toxicity_check',
        python_callable=toxicity_check,
    )
    
    remove_and_flag_content_task = PythonOperator(
        task_id='remove_and_flag_content',
        python_callable=remove_and_flag_content,
    )
    
    publish_content_task = PythonOperator(
        task_id='publish_content',
        python_callable=publish_content,
    )
    
    audit_log_task = PythonOperator(
        task_id='audit_log',
        python_callable=audit_log,
        trigger_rule='none_failed_min_one_success',
    )
    
    scan_csv_task >> toxicity_check_task
    toxicity_check_task >> [remove_and_flag_content_task, publish_content_task]
    remove_and_flag_content_task >> audit_log_task
    publish_content_task >> audit_log_task