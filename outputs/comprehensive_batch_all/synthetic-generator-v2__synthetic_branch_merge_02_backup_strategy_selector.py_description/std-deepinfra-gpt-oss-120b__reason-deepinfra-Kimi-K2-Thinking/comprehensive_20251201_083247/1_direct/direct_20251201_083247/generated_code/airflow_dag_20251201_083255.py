"""Automated database backup DAG with branch-merge logic.

This DAG runs daily, selects a backup strategy based on the day of the week,
executes the appropriate backup command, verifies the backup, and marks
completion.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


def choose_backup(**context):
    """Return task id based on execution day.

    - Saturday (weekday == 5): full backup
    - Weekdays (Monday‑Friday): incremental backup
    """
    execution_date = context["execution_date"]
    if execution_date.weekday() == 5:  # Saturday
        return "full_backup_task"
    return "incremental_backup_task"


default_args = {
    "owner": "Backup team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="database_backup_branch_merge",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["backup", "branch_merge"],
    concurrency=2,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start_backup_process")

    date_check = BranchPythonOperator(
        task_id="date_check_task",
        python_callable=choose_backup,
        provide_context=True,
    )

    full_backup = BashOperator(
        task_id="full_backup_task",
        bash_command="echo 'Running full database backup...'",
    )

    incremental_backup = BashOperator(
        task_id="incremental_backup_task",
        bash_command="echo 'Running incremental database backup...'",
    )

    verify_backup = BashOperator(
        task_id="verify_backup_task",
        bash_command="echo 'Verifying backup integrity...'",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(task_id="backup_complete")

    # Define task dependencies
    start >> date_check
    date_check >> [full_backup, incremental_backup] >> verify_backup >> end