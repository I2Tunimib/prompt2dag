from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator


def choose_backup_strategy(**context):
    """
    Determine backup strategy based on execution date.
    
    Returns:
        str: Task ID to execute based on day of week
    """
    execution_date = context['execution_date']
    day_of_week = execution_date.weekday()
    
    # Saturday: full backup
    if day_of_week == 5:
        return 'full_backup_task'
    # Monday-Friday: incremental backup
    elif day_of_week in range(0, 5):
        return 'incremental_backup_task'
    # Sunday: default to incremental backup
    else:
        return 'incremental_backup_task'


default_args = {
    'owner': 'backup_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='database_backup_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['backup', 'database'],
    description='Branch-merge pattern for automated database backup operations',
) as dag:
    
    start_backup = EmptyOperator(
        task_id='start_backup_process'
    )
    
    date_check = BranchPythonOperator(
        task_id='date_check_task',
        python_callable=choose_backup_strategy
    )
    
    full_backup = BashOperator(
        task_id='full_backup_task',
        bash_command='echo "Executing full database backup"'
    )
    
    incremental_backup = BashOperator(
        task_id='incremental_backup_task',
        bash_command='echo "Executing incremental database backup"'
    )
    
    verify_backup = BashOperator(
        task_id='verify_backup_task',
        bash_command='echo "Verifying backup integrity and completeness"',
        trigger_rule='none_failed_min_one_success'
    )
    
    backup_complete = EmptyOperator(
        task_id='backup_complete'
    )
    
    # Define task dependencies
    start_backup >> date_check
    date_check >> [full_backup, incremental_backup]
    [full_backup, incremental_backup] >> verify_backup
    verify_backup >> backup_complete