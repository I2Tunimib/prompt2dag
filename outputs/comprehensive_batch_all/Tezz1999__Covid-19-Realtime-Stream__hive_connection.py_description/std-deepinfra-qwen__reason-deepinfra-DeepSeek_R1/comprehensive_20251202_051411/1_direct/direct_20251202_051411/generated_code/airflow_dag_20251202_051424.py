from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta

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
    schedule_interval='0 1 * * *',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args=default_args,
    description='A simple linear DAG to perform basic Hive database operations',
) as dag:

    run_after_loop = BashOperator(
        task_id='run_after_loop',
        bash_command='echo $USER',
    )

    hive_script_task = HiveOperator(
        task_id='hive_script_task',
        hql="""
            CREATE DATABASE IF NOT EXISTS mydb;
            USE mydb;
            CREATE TABLE IF NOT EXISTS test_af (test INT);
            INSERT INTO test_af VALUES (2);
        """,
        hive_cli_conn_id='hive_local',
    )

    run_after_loop >> hive_script_task