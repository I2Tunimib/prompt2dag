from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils import timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

start_date = timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

with DAG(
    dag_id='hive_script',
    default_args=default_args,
    description='Simple linear DAG for basic Hive database operations',
    schedule_interval='0 1 * * *',
    start_date=start_date,
    catchup=False,
    tags=['hive', 'database'],
) as dag:

    run_after_loop = BashOperator(
        task_id='run_after_loop',
        bash_command='echo "Current username: $(whoami)"',
    )

    hive_script_task = HiveOperator(
        task_id='hive_script_task',
        hql="""
        CREATE DATABASE IF NOT EXISTS mydb;
        CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
        INSERT INTO mydb.test_af VALUES (2);
        """,
        hive_cli_conn_id='hive_local',
    )

    run_after_loop >> hive_script_task