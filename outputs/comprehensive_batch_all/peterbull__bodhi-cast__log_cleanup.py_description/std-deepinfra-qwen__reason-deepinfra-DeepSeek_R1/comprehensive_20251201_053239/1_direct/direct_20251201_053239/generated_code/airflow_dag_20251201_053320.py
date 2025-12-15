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
    'retries': 1,
    'retry_delay': 60,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'start_date': days_ago(1),
}

def create_lock_file(lock_file_path):
    try:
        with open(lock_file_path, 'x'):
            pass
    except FileExistsError:
        raise RuntimeError(f"Lock file {lock_file_path} already exists. Another instance is running.")

def delete_lock_file(lock_file_path):
    try:
        os.remove(lock_file_path)
    except FileNotFoundError:
        pass

def cleanup_logs(log_dir, max_log_age, enable_child_logs, lock_file_path):
    try:
        create_lock_file(lock_file_path)
        # Delete old log files
        os.system(f"find {log_dir} -type f -mtime +{max_log_age} -delete")
        # Remove empty subdirectories
        os.system(f"find {log_dir} -type d -empty -delete")
        # Remove empty top-level directories
        os.system(f"find {log_dir} -type d -empty -delete")
    except Exception as e:
        print(f"Error during log cleanup: {e}")
    finally:
        delete_lock_file(lock_file_path)

with DAG(
    dag_id='airflow_log_cleanup',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    start = EmptyOperator(task_id='start')

    log_dirs = [
        Variable.get('AIRFLOW__CORE__BASE_LOG_FOLDER', '/opt/airflow/logs'),
        Variable.get('airflow_log_cleanup__child_log_dir', '/opt/airflow/scheduler_logs')
    ]

    max_log_age = Variable.get('airflow_log_cleanup__max_log_age_in_days', default_var=30)
    enable_child_logs = Variable.get('airflow_log_cleanup__enable_delete_child_log', default_var=False)

    cleanup_tasks = []
    for log_dir in log_dirs:
        if enable_child_logs or log_dir == log_dirs[0]:
            task = PythonOperator(
                task_id=f'cleanup_{os.path.basename(log_dir)}',
                python_callable=cleanup_logs,
                op_kwargs={
                    'log_dir': log_dir,
                    'max_log_age': max_log_age,
                    'enable_child_logs': enable_child_logs,
                    'lock_file_path': '/tmp/airflow_log_cleanup_worker.lock'
                }
            )
            cleanup_tasks.append(task)

    start >> cleanup_tasks