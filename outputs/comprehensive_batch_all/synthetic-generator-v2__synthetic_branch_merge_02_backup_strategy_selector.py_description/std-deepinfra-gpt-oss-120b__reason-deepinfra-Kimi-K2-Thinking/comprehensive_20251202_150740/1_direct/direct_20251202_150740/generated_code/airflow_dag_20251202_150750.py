"""Database backup DAG with branch-merge pattern.

This DAG runs daily and selects a backup strategy based on the day of the week:
- Saturday → full backup
- Weekdays → incremental backup

After the backup task, a verification step merges both branches.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def decide_backup_strategy(**context):
    """Return task_id for backup type based on execution day.

    Saturday (weekday == 5) → full backup,
    otherwise → incremental backup.
    """
    execution_date = context["execution_date"]
    if execution_date.weekday() == 5:  # Saturday
        return "full_backup_task"
    return "incremental_backup_task"


default_args = {
    "owner": "backup_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="database_backup",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=2,
    tags=["backup"],
) as dag:
    start_backup_process = EmptyOperator(task_id="start_backup_process")

    date_check_task = BranchPythonOperator(
        task_id="date_check_task",
        python_callable=decide_backup_strategy,
        provide_context=True,
    )

    full_backup_task = BashOperator(
        task_id="full_backup_task",
        bash_command="echo 'Running full database backup...'",
    )

    incremental_backup_task = BashOperator(
        task_id="incremental_backup_task",
        bash_command="echo 'Running incremental database backup...'",
    )

    verify_backup_task = BashOperator(
        task_id="verify_backup_task",
        bash_command="echo 'Verifying backup integrity...'",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    backup_complete = EmptyOperator(task_id="backup_complete")

    # Define dependencies
    start_backup_process >> date_check_task
    date_check_task >> [full_backup_task, incremental_backup_task]
    [full_backup_task, incremental_backup_task] >> verify_backup_task
    verify_backup_task >> backup_complete