"""Simple DAG to run a Bash command followed by a Hive script.

The DAG executes daily at 01:00 AM, first printing the current system user
and then creating a Hive database/table and inserting a test record.
"""

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hive_script",
    default_args=default_args,
    description="Run Bash then Hive script",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "hive"],
) as dag:
    run_after_loop = BashOperator(
        task_id="run_after_loop",
        bash_command="echo $USER",
    )

    hive_script = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """

    hive_script_task = HiveOperator(
        task_id="hive_script_task",
        hql=hive_script,
        hive_cli_conn_id="hive_local",
    )

    run_after_loop >> hive_script_task