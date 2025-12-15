from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import csv
import random
import os
import json

default_args = {
    'owner': 'content-moderation-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def scan_csv(**context):
    """Scans CSV file and returns metadata."""
    csv_path = '/tmp/user_content.csv'
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        content_items = list(reader)
    
    total_items = len(content_items)
    context['task_instance'].xcom_push(key='content_items', value=content_items)
    context['task_instance'].xcom_push(key='total_items', value=total_items)
    
    return f"Scanned {total_items} content items"


def toxicity_check(**context):
    """Evaluates toxicity and routes to appropriate task."""
    content_items = context['task_instance'].xcom_pull(task_ids='scan_csv', key='content_items')
    
    if not content_items:
        return 'audit_log'
    
    toxicity_scores = [random.uniform(0, 1) for _ in content_items]
    max_toxicity = max(toxicity_scores)
    
    context['task_instance'].xcom_push(key='toxicity_scores', value=toxicity_scores)
    context['task_instance'].xcom_push(key='max_toxicity', value=max_toxicity)
    
    return 'remove_and_flag_content' if max_toxicity > 0.7 else 'publish_content'


def remove_and_flag_content(**context):
    """Removes toxic content and flags user accounts."""
    content_items = context['task_instance'].xcom_pull(task_ids='scan_csv', key='content_items')
    toxicity_scores = context['task_instance'].xcom_pull(task_ids='toxicity_check', key='toxicity_scores')
    
    removed_count = 0
    flagged_users = set()
    
    for item, score in zip(content_items, toxicity_scores):
        if score > 0.7:
            removed_count += 1
            flagged_users.add(item.get('user_id', 'unknown_user'))
    
    context['task_instance'].xcom_push(key='action', value='remove_and_flag')
    context['task_instance'].xcom_push(key='removed_count', value=removed_count)
    context['task_instance'].xcom_push(key='flagged_users', value=list(flagged_users))
    
    return f"Removed {removed_count} items, flagged {len(flagged_users)} users"


def publish_content(**context):
    """Publishes safe content."""
    content_items = context['task_instance'].xcom_pull(task_ids='scan_csv', key='content_items')
    toxicity_scores = context['task_instance'].xcom_pull(task_ids='toxicity_check', key='toxicity_scores')
    
    published_count = sum(1 for score in toxicity_scores if score <= 0.7)
    
    context['task_instance'].xcom_push(key='action', value='publish')
    context['task_instance'].xcom_push(key='published_count', value=published_count)
    
    return f"Published {published_count} items"


def audit_log(**context):
    """Creates final audit log entry."""
    action = context['task_instance'].xcom_pull(key='action')
    total_items = context['task_instance'].xcom_pull(task_ids='scan_csv', key='total_items')
    
    if not action:
        raise ValueError("No action found in XCom")
    
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'total_items_scanned': total_items,
        'action_taken': action,
    }
    
    if action == 'remove_and_flag':
        log_entry['removed_count'] = context['task_instance'].xcom_pull(key='removed_count')
        log_entry['flagged_users'] = context['task_instance'].xcom_pull(key='flagged_users')
    elif action == 'publish':
        log_entry['published_count'] = context['task_instance'].xcom_pull(key='published_count')
    
    audit_path = '/tmp/audit_log.json'
    with open(audit_path, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    
    return f"Audit log created: {log_entry}"


with DAG(
    dag_id='content_moderation_pipeline',
    default_args=default_args,
    description='Content moderation pipeline with toxicity checking and conditional routing',
    schedule_interval='@daily',
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
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    scan_csv_task >> toxicity_check_task
    toxicity_check_task >> remove_and_flag_content_task >> audit_log_task
    toxicity_check_task >> publish_content_task >> audit_log_task