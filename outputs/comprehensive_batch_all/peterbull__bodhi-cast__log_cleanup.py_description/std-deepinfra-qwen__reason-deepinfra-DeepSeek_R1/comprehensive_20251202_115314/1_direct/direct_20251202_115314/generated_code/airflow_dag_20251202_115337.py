from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 60,
    'start_date': days_ago(1),
}

def create_lock_file(lock_file_path):
    """Create a lock file to prevent concurrent execution."""
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write('1')

def delete_lock_file(lock_file_path):
    """Delete the lock file after the task completes."""
    if os.path.exists(lock_file_path):
        os.remove(lock_file_path)

def cleanup_logs(log_dir, max_log_age_in_days, enable_delete_child_log):
    """Perform the three-phase cleanup process for log files and directories."""
    lock_file_path = '/tmp/airflow_log_cleanup_worker.lock'
    
    try:
        create_lock_file(lock_file_path)
        
        # Phase 1: Delete old log files
        os.system(f"find {log_dir} -type f -mtime +{max_log_age_in_days} -delete")
        
        # Phase 2: Remove empty subdirectories
        os.system(f"find {log_dir} -type d -empty -delete")
        
        # Phase 3: Remove empty top-level log directories
        os.system(f"find {log_dir} -type d -empty -delete")
        
        # Optional: Delete child process logs
        if enable_delete_child_log:
            child_log_dir = Variable.get('AIRFLOW__CORE__BASE_LOG_FOLDER', default_var='/opt/airflow/logs')
            os.system(f"find {child_log_dir} -type f -mtime +{max_log_age_in_days} -delete")
            os.system(f"find {child_log_dir} -type d -empty -delete")
    
    except Exception as e:
        print(f"Error during cleanup: {e}")
    finally:
        delete_lock_file(lock_file_path)

with DAG(
    dag_id='airflow_log_cleanup',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['maintenance', 'cleanup'],
    is_paused_upon_creation=False,
) as dag:

    start = EmptyOperator(task_id='start')

    max_log_age_in_days = Variable.get('airflow_log_cleanup__max_log_age_in_days', default_var=30)
    enable_delete_child_log = Variable.get('airflow_log_cleanup__enable_delete_child_log', default_var=False, deserialize_json=True)

    log_dirs = [
        '/opt/airflow/logs/dag_id_1',
        '/opt/airflow/logs/dag_id_2',
        '/opt/airflow/logs/dag_id_3',
    ]

    cleanup_tasks = []
    for log_dir in log_dirs:
        cleanup_task = PythonOperator(
            task_id=f'cleanup_{os.path.basename(log_dir)}',
            python_callable=cleanup_logs,
            op_kwargs={
                'log_dir': log_dir,
                'max_log_age_in_days': max_log_age_in_days,
                'enable_delete_child_log': enable_delete_child_log,
            },
        )
        cleanup_tasks.append(cleanup_task)

    start >> cleanup_tasks