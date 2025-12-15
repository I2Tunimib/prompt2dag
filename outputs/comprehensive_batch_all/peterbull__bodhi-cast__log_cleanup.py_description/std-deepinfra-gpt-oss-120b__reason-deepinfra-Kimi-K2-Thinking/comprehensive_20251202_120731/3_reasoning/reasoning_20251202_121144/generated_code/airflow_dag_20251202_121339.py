from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.configuration import conf
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email': ['admin@example.com'],  # Replace with actual email
}

# Configuration
NUM_WORKERS = 3  # Number of parallel cleanup workers
LOCK_FILE = '/tmp/airflow_log_cleanup_worker.lock'
SLEEP_INTERVAL = 5  # Seconds between worker starts

# Get configuration from Airflow Variables
MAX_LOG_AGE_DAYS = Variable.get(
    'airflow_log_cleanup__max_log_age_in_days',
    default_var=30
)
ENABLE_CHILD_LOG = Variable.get(
    'airflow_log_cleanup__enable_delete_child_log',
    default_var=False,
    deserialize_json=True
)

# DAG definition
with DAG(
    dag_id='airflow_log_cleanup',
    default_args=default_args,
    description='Automated cleanup of old Airflow log files',
    schedule_interval='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['maintenance', 'cleanup'],
) as dag:

    start_task = EmptyOperator(
        task_id='start_log_cleanup'
    )

    # Create worker tasks
    cleanup_tasks = []
    for i in range(NUM_WORKERS):
        worker_id = i + 1
        
        # Bash command with file-based locking and three-phase cleanup
        bash_command = f"""
        #!/bin/bash
        set -e
        
        # Configuration
        LOCK_FILE="{LOCK_FILE}"
        WORKER_ID="{worker_id}"
        NUM_WORKERS="{NUM_WORKERS}"
        
        # Get configuration from DAG params or variables
        MAX_AGE="{{{{ params.maxLogAgeInDays or {MAX_LOG_AGE_DAYS} }}}}"
        ENABLE_CHILD="{{{{ params.enableChildLog or {str(ENABLE_CHILD_LOG).lower() } }}}}"
        
        # Base log folder from Airflow config
        BASE_LOG_FOLDER="{{{{ conf.get('logging', 'BASE_LOG_FOLDER') }}}}"
        
        # Sleep to stagger workers
        sleep $(( ({worker_id} - 1) * {SLEEP_INTERVAL} ))
        
        # File-based locking using flock
        exec 200>"$LOCK_FILE"
        if ! flock -n 200; then
            echo "Worker {worker_id}: Another instance is running. Exiting."
            exit 0
        fi
        
        # Trap to ensure lock is released on exit
        trap 'rm -f "$LOCK_FILE"' EXIT
        
        echo "Worker {worker_id}: Starting cleanup with max age $MAX_AGE days"
        
        # Phase 1: Delete old log files
        echo "Worker {worker_id}: Phase 1 - Deleting old log files"
        find "$BASE_LOG_FOLDER" -type f -name "*.log" -mtime +"$MAX_AGE" -delete 2>/dev/null || true
        
        # Phase 2: Remove empty subdirectories
        echo "Worker {worker_id}: Phase 2 - Removing empty subdirectories"
        find "$BASE_LOG_FOLDER" -type d -mindepth 1 -empty -delete 2>/dev/null || true
        
        # Phase 3: Remove empty top-level log directories
        echo "Worker {worker_id}: Phase 3 - Removing empty top-level directories"
        find "$BASE_LOG_FOLDER" -maxdepth 1 -type d -empty -delete 2>/dev/null || true
        
        # Cleanup child process logs if enabled
        if [ "$ENABLE_CHILD" = "true" ]; then
            CHILD_LOG_DIR="{{{{ conf.get('scheduler', 'CHILD_PROCESS_LOG_DIRECTORY', fallback='') }}}}"
            if [ -n "$CHILD_LOG_DIR" ] && [ -d "$CHILD_LOG_DIR" ]; then
                echo "Worker {worker_id}: Cleaning up child process logs"
                find "$CHILD_LOG_DIR" -type f -name "*.log" -mtime +"$MAX_AGE" -delete 2>/dev/null || true
                find "$CHILD_LOG_DIR" -type d -mindepth 1 -empty -delete 2>/dev/null || true
            fi
        fi
        
        echo "Worker {worker_id}: Cleanup completed successfully"
        """
        
        cleanup_task = BashOperator(
            task_id=f'cleanup_worker_{worker_id}',
            bash_command=bash_command,
            params={
                'maxLogAgeInDays': MAX_LOG_AGE_DAYS,
                'enableChildLog': ENABLE_CHILD_LOG,
            },
        )
        
        cleanup_tasks.append(cleanup_task)

    # Set dependencies: start_task triggers all cleanup tasks in parallel
    start_task >> cleanup_tasks