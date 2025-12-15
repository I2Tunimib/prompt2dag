from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


def choose_backup_strategy(**context):
    """
    Determine backup type based on execution date.
    Returns task_id of the selected backup task.
    """
    exec_date = context["execution_date"]
    # Python weekday: Monday=0, Saturday=5, Sunday=6
    if exec_date.weekday() == 5:  # Saturday
        return "full_backup_task"
    return "incremental_backup_task"


default_args = {
    "owner": "backup_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="database_backup_dag",
    default_args=default_args,
    description="Daily DB backup with branch-merge logic",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backup"],
) as dag:
    start_backup_process = EmptyOperator(task_id="start_backup_process")

    date_check_task = BranchPythonOperator(
        task_id="date_check_task",
        python_callable=choose_backup_strategy,
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