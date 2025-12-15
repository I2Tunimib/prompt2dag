from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator


def choose_backup_strategy(**context):
    """
    Branching logic to select backup strategy based on execution date.
    Returns 'full_backup_task' for Saturday, 'incremental_backup_task' otherwise.
    """
    execution_date = context['execution_date']
    if execution_date.weekday() == 5:  # Saturday
        return 'full_backup_task'
    return 'incremental_backup_task'


default_args = {
    'owner': 'backup_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'database_backup_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['backup', 'database'],
    description='Automated database backup with branch-merge pattern',
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
        bash_command='echo "Verifying backup integrity"',
        trigger_rule='none_failed_min_one_success'
    )

    backup_complete = EmptyOperator(
        task_id='backup_complete'
    )

    # Define task dependencies
    start_backup >> date_check
    date_check >> full_backup
    date_check >> incremental_backup
    [full_backup, incremental_backup] >> verify_backup
    verify_backup >> backup_complete