from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["airflow@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="airflow_log_cleanup",
    description="Daily cleanup of old Airflow log files with parallel workers.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=False,
    tags=["maintenance", "cleanup"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Define worker names and staggered sleep times (seconds)
    workers = [
        ("cleanup_worker_1", 0),
        ("cleanup_worker_2", 10),
        ("cleanup_worker_3", 20),
    ]

    cleanup_tasks = []
    for worker_id, sleep_seconds in workers:
        cleanup = BashOperator(
            task_id=worker_id,
            bash_command="""
{% set max_age = dag_run.conf.get('maxLogAgeInDays') if dag_run and dag_run.conf.get('maxLogAgeInDays') else var.value('airflow_log_cleanup__max_log_age_in_days') %}
{% set enable_child = dag_run.conf.get('enableDeleteChildLog') if dag_run and dag_run.conf.get('enableDeleteChildLog') else var.value('airflow_log_cleanup__enable_delete_child_log') %}
{% set base_log_folder = var.value('base_log_folder') %}
#!/usr/bin/env bash
set -euo pipefail

LOCK_FILE="/tmp/airflow_log_cleanup_{{ task_instance.task_id }}.lock"

if [ -e "$LOCK_FILE" ]; then
  echo "Lock file $LOCK_FILE exists, another instance is running. Exiting."
  exit 0
fi

touch "$LOCK_FILE"

# Delete old log files
find "{{ base_log_folder }}" -type f -mtime +"{{ max_age }}" -delete

# Optionally delete child process logs
if [ "{{ enable_child | lower }}" = "true" ]; then
  find "{{ base_log_folder }}/scheduler" -type f -mtime +"{{ max_age }}" -delete
fi

# Remove empty subdirectories (including top‑level)
find "{{ base_log_folder }}" -type d -empty -delete

# Staggered sleep to distribute load
sleep "{{ env.SLEEP_SECONDS }}"

rm -f "$LOCK_FILE"
""",
            env={"SLEEP_SECONDS": str(sleep_seconds)},
            retries=1,
            retry_delay=timedelta(minutes=1),
        )
        cleanup_tasks.append(cleanup)

    end = EmptyOperator(task_id="end")

    start >> cleanup_tasks >> end