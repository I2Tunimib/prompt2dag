from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'owner': 'Backup team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def decide_backup_strategy(**context):
    execution_date = context['execution_date']
    if execution_date.weekday() == 5:  # Saturday
        return 'full_backup_task'
    else:
        return 'incremental_backup_task'

with DAG(
    dag_id='database_backup_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    concurrency=2,
) as dag:

    start_backup_process = EmptyOperator(task_id='start_backup_process')

    date_check_task = BranchPythonOperator(
        task_id='date_check_task',
        python_callable=decide_backup_strategy,
        provide_context=True
    )

    full_backup_task = BashOperator(
        task_id='full_backup_task',
        bash_command='echo "Executing full database backup"',
    )

    incremental_backup_task = BashOperator(
        task_id='incremental_backup_task',
        bash_command='echo "Executing incremental database backup"',
    )

    verify_backup_task = BashOperator(
        task_id='verify_backup_task',
        bash_command='echo "Verifying backup integrity and completeness"',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    backup_complete = EmptyOperator(task_id='backup_complete')

    start_backup_process >> date_check_task
    date_check_task >> full_backup_task
    date_check_task >> incremental_backup_task
    full_backup_task >> verify_backup_task
    incremental_backup_task >> verify_backup_task
    verify_backup_task >> backup_complete