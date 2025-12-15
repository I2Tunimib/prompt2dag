from prefect import flow, task
import subprocess
from datetime import datetime, timedelta

@task
def run_after_loop():
    """Runs a Bash command to echo the current system username."""
    result = subprocess.run(["echo", "$USER"], capture_output=True, text=True, shell=True)
    return result.stdout.strip()

@task
def hive_script_task():
    """Executes a multi-statement Hive query to create a database, table, and insert a test record."""
    hive_query = """
    CREATE DATABASE IF NOT EXISTS mydb;
    USE mydb;
    CREATE TABLE IF NOT EXISTS test_af (test INT);
    INSERT INTO test_af VALUES (2);
    """
    result = subprocess.run(["hive", "-e", hive_query], capture_output=True, text=True)
    return result.stdout.strip()

@flow(name="hive_script")
def hive_script_flow():
    """Orchestrates the execution of the Bash and Hive tasks in a sequential manner."""
    bash_output = run_after_loop()
    hive_output = hive_script_task()
    return bash_output, hive_output

if __name__ == '__main__':
    # Schedule: Daily at 1:00 AM
    # Start Date: One day prior to current date
    # Note: For deployment, configure the schedule using Prefect's deployment and schedule features.
    hive_script_flow()