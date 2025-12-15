from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hive_script',
    default_args=default_args,
    description='Linear DAG for basic Hive database operations',
    schedule_interval='0 1 * * *',
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
    tags=['hive', 'bash', 'example'],
) as dag:
    
    # Echo current system username
    run_after_loop = BashOperator(
        task_id='run_after_loop',
        bash_command='echo "Current username: $(whoami)"',
    )
    
    # Create Hive database/table and insert test record
    hive_script_task = HiveOperator(
        task_id='hive_script_task',
        hive_cli_conn_id='hive_local',
        hql="""
            CREATE DATABASE IF NOT EXISTS mydb;
            USE mydb;
            CREATE TABLE IF NOT EXISTS test_af (test INT);
            INSERT INTO TABLE test_af SELECT 2;
        """,
    )
    
    # Linear execution: bash task followed by hive task
    run_after_loop >> hive_script_task