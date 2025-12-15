"""Database backup DAG with branch-merge pattern.

This DAG runs daily and selects a backup strategy based on the day of the week:
- Saturday: full backup
- Weekdays (including Sunday): incremental backup

After the backup task, a verification step runs with a trigger rule that
allows it to proceed when at least one backup path succeeds.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator


def choose_backup_strategy(**kwargs):
    """Return the task_id of the backup to execute.

    Saturday (weekday == 5) -> full backup,
    otherwise -> incremental backup.
    """
    execution_date = kwargs["execution_date"]
    if execution_date.weekday() == 5:
        return "full_backup_task"
    return "incremental_backup_task"


default_args = {
    "owner": "backup team",
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
    start = EmptyOperator(task_id="start_backup_process")

    branch = BranchPythonOperator(
        task_id="date_check_task", python_callable=choose_backup_strategy
    )

    full_backup = BashOperator(
        task_id="full_backup_task",
        bash_command="echo 'Running full database backup'",
    )

    incremental_backup = BashOperator(
        task_id="incremental_backup_task",
        bash_command="echo 'Running incremental database backup'",
    )

    verify = BashOperator(
        task_id="verify_backup_task",
        bash_command="echo 'Verifying backup integrity and completeness'",
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="backup_complete")

    # Define dependencies
    start >> branch
    branch >> [full_backup, incremental_backup] >> verify >> end