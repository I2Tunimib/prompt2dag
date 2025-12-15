"""Airflow DAG for executing a Bash command followed by a Hive script.

The DAG runs daily at 1:00 AM and performs the following steps:
1. Prints the current system username using BashOperator.
2. Executes a Hive script that creates a database, a table, and inserts a test record.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="hive_script",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # Daily at 01:00 AM
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    tags=["example"],
) as dag:
    # Task: Print the current system username
    run_after_loop = BashOperator(
        task_id="run_after_loop",
        bash_command="echo $USER",
    )

    # Hive script to create database, table, and insert a record
    hive_sql = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """

    # Task: Execute the Hive script
    hive_script_task = HiveOperator(
        task_id="hive_script_task",
        hql=hive_sql,
        hive_cli_conn_id="hive_local",
    )

    # Define linear dependencies
    run_after_loop >> hive_script_task