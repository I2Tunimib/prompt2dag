from prefect import flow, task
import subprocess
from typing import Dict, Any


@task(name="run_after_loop", retries=2)
def run_bash_echo() -> str:
    """
    Execute bash command to echo the current system username.
    Replicates Airflow BashOperator task 'run_after_loop'.
    """
    result = subprocess.run(
        ["echo $USER"],
        shell=True,
        capture_output=True,
        text=True,
        check=True
    )
    username = result.stdout.strip()
    print(f"Bash task 'run_after_loop' completed. Username: {username}")
    return username


@task(name="hive_script_task", retries=1)
def execute_hive_ops(connection_block: str = "hive_local") -> str:
    """
    Execute Hive operations: create DB, table, and insert record.
    Replicates Airflow HiveOperator task 'hive_script_task'.
    """
    hive_script = """
    CREATE DATABASE IF NOT EXISTS mydb;
    USE mydb;
    CREATE TABLE IF NOT EXISTS test_af (test INT);
    INSERT INTO test_af VALUES (2);
    """
    
    # Production approach using prefect-hive (commented for reference):
    # from prefect_hive.credentials import HiveCredentials
    # from prefect_hive.tasks import execute_query
    # credentials = HiveCredentials.load(connection_block)
    # return execute_query(query=hive_script, credentials=credentials)
    
    # Direct subprocess approach for broader compatibility
    # Assumes hive CLI is available and configured with connection_block
    result = subprocess.run(
        ["hive", "-e", hive_script],
        capture_output=True,
        text=True,
        check=True
    )
    print("Hive task 'hive_script_task' completed successfully")
    return result.stdout


@flow(name="hive_script")
def hive_script_flow() -> Dict[str, Any]:
    """
    Prefect 2.x flow replicating the 'hive_script' Airflow DAG.
    
    Linear sequential execution:
    1. run_bash_echo: Echoes current username
    2. execute_hive_ops: Creates Hive DB/table and inserts record
    
    Schedule: Daily at 1:00 AM (configure via deployment)
    Start Date: One day prior to current date (configure via deployment)
    """
    # Sequential execution: bash task must complete before hive task
    username = run_bash_echo()
    
    # Hive task depends on bash task completion
    hive_output = execute_hive_ops()
    
    return {
        "bash_output": username,
        "hive_output": hive_output
    }


if __name__ == "__main__":
    # Local execution for testing and development
    # For production scheduling, deploy with:
    # prefect deployment build hive_script_flow.py:hive_script_flow \
    #   --name "daily-hive-script" \
    #   --schedule "0 1 * * *" \
    #   --apply
    hive_script_flow()