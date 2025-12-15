from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator


def choose_backup_strategy(**context):
    """Determine backup type based on execution date."""
    execution_date = context['execution_date']
    if execution_date.weekday() == 5:  # Saturday
        return 'full_backup_task'
    return 'incremental_backup_task'


default_args = {
    'owner': 'backup_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='database_backup_branch_merge',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['backup', 'database'],
) as dag:

    start_backup_process = EmptyOperator(task_id='start_backup_process')

    date_check_task = BranchPythonOperator(
        task_id='date_check_task',
        python_callable=choose_backup_strategy,
    )

    full_backup_task = BashOperator(
        task_id='full_backup_task',
        bash_command="echo 'Executing full database backup on Saturday'",
    )

    incremental_backup_task = BashOperator(
        task_id='incremental_backup_task',
        bash_command="echo 'Executing incremental database backup on weekday'",
    )

    verify_backup_task = BashOperator(
        task_id='verify_backup_task',
        bash_command="echo 'Verifying backup integrity and completeness'",
        trigger_rule='none_failed_min_one_success',
    )

    backup_complete = EmptyOperator(task_id='backup_complete')

    # Define task dependencies
    start_backup_process >> date_check_task
    date_check_task >> [full_backup_task, incremental_backup_task]
    [full_backup_task, incremental_backup_task] >> verify_backup_task
    verify_backup_task >> backup_complete