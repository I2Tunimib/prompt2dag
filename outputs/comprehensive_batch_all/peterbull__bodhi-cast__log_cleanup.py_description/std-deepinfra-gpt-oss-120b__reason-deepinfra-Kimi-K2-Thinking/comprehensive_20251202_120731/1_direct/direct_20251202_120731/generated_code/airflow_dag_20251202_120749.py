"""Airflow DAG for parallel cleanup of old Airflow log files.

The DAG runs daily and launches several parallel BashOperator workers that
remove log files older than a configurable number of days.  A file‑based
lock prevents concurrent execution of the same worker.  Configuration can be
provided via Airflow Variables or DAG run conf.
"""

from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ----------------------------------------------------------------------
# Default arguments applied to all tasks
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}

# ----------------------------------------------------------------------
# Helper to build the Bash command for a cleanup worker
# ----------------------------------------------------------------------
def _build_cleanup_command(worker_id: int, base_log_folder: str) -> str:
    """
    Return a Bash script that performs the three‑phase cleanup for a given worker.

    The script:
    * Acquires a lock file in /tmp to avoid concurrent runs of the same worker.
    * Sleeps a random short period to stagger execution across workers.
    * Determines the max age (in days) from DAG run conf or Airflow Variable.
    * Optionally includes child process logs based on a Variable.
    * Deletes old ``*.log`` files, removes empty sub‑directories and top‑level
      directories.
    """
    # Jinja templating is used for runtime values (dag_run.conf and Variables)
    return f"""\
LOCK_FILE="/tmp/airflow_log_cleanup_worker_{worker_id}.lock"

# Exit if another instance of this worker holds the lock
if [ -e "$LOCK_FILE" ]; then
    echo "Lock file $LOCK_FILE exists – another instance is running. Exiting."
    exit 0
fi

# Acquire lock
touch "$LOCK_FILE"
# Ensure lock removal on exit (success or failure)
trap 'rm -f "$LOCK_FILE"' EXIT

# Stagger workers to spread I/O load
sleep $((RANDOM % 30 + 1))

# Runtime configuration (overridable via DAG run conf)
MAX_AGE_DAYS="{{{{ dag_run.conf.get('maxLogAgeInDays', var.value.airflow_log_cleanup__max_log_age_in_days) }}}}"
ENABLE_CHILD="{{{{ var.value.airflow_log_cleanup__enable_delete_child_log | default('false') }}}}"

# ------------------------------------------------------------------
# Phase 1 – Delete old log files
# ------------------------------------------------------------------
find "{base_log_folder}" -type f -name "*.log" -mtime "+$MAX_AGE_DAYS" -delete

# Include child process logs if enabled
if [ "$ENABLE_CHILD" = "true" ]; then
    find "{base_log_folder}/scheduler" -type f -name "*.log" -mtime "+$MAX_AGE_DAYS" -delete
fi

# ------------------------------------------------------------------
# Phase 2 – Remove empty sub‑directories
# ------------------------------------------------------------------
find "{base_log_folder}" -type d -empty -delete

# ------------------------------------------------------------------
# Phase 3 – Remove empty top‑level log directories (covered above)
# ------------------------------------------------------------------
exit 0
"""

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="airflow_log_cleanup",
    description="Parallel cleanup of old Airflow log files",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance", "cleanup"],
) as dag:
    # ------------------------------------------------------------------
    # Entry point – does nothing but provides a clear start for the DAG
    # ------------------------------------------------------------------
    start = EmptyOperator(task_id="start")

    # ------------------------------------------------------------------
    # Determine the base log folder from Airflow configuration (static)
    # ------------------------------------------------------------------
    base_log_folder = conf.get("core", "BASE_LOG_FOLDER")

    # ------------------------------------------------------------------
    # Create parallel cleanup workers
    # ------------------------------------------------------------------
    worker_tasks = []
    for worker_id in range(1, 4):  # Adjust the range to change parallelism
        cleanup_task = BashOperator(
            task_id=f"cleanup_worker_{worker_id}",
            bash_command=_build_cleanup_command(worker_id, base_log_folder),
            # BashOperator automatically treats ``bash_command`` as a templated field
        )
        worker_tasks.append(cleanup_task)

    # ------------------------------------------------------------------
    # Set task dependencies
    # ------------------------------------------------------------------
    start >> worker_tasks