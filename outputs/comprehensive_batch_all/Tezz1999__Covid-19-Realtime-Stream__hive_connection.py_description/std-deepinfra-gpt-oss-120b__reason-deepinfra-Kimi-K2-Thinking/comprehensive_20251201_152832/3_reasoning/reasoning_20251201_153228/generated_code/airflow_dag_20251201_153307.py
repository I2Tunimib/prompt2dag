from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago

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
    description='Simple linear DAG for Hive database operations',
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['hive', 'example'],
) as dag:
    
    run_after_loop = BashOperator(
        task_id='run_after_loop',
        bash_command='echo $USER',
    )
    
    hive_script_task = HiveOperator(
        task_id='hive_script_task',
        hiveql="""
            CREATE DATABASE IF NOT EXISTS mydb;
            USE mydb;
            CREATE TABLE IF NOT EXISTS test_af (test INT);
            INSERT INTO test_af VALUES (2);
        """,
        hive_cli_conn_id='hive_local',
    )
    
    run_after_loop >> hive_script_task