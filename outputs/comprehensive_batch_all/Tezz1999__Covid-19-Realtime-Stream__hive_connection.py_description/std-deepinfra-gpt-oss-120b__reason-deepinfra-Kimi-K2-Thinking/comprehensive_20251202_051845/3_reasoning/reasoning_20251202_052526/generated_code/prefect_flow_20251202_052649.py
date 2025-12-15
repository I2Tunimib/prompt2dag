from prefect import flow, task
import subprocess
import os
from typing import Dict, Any


@task
def run_after_loop() -> str:
    """Execute bash command to echo current username."""
    result = subprocess.run(
        "echo $USER",
        shell=True,
        capture_output=True,
        text=True,
        check=True
    )
    
    username = result.stdout.strip()
    print(f"Bash task output: {username}")
    return username


@task
def hive_script_task() -> str:
    """Execute Hive script to create database, table, and insert record."""
    # Connection parameters - in production, use Prefect Blocks or secrets
    HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
    HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
    HIVE_USER = os.getenv("HIVE_USER", "hive_user")
    
    queries = [
        "CREATE DATABASE IF NOT EXISTS mydb",
        "CREATE TABLE IF NOT EXISTS mydb.test_af (test INT)",
        "INSERT INTO mydb.test_af VALUES (2)"
    ]
    
    try:
        from pyhive import hive
        
        with hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER) as conn:
            with conn.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                    print(f"Executed Hive query: {query}")
        
        return "Hive operations completed successfully"
        
    except ImportError:
        print("Warning: pyhive not installed. Simulating Hive operations.")
        for query in queries:
            print(f"Would execute: {query}")
        return "Hive operations simulated"
    
    except Exception as e:
        error_msg = f"Hive operation failed: {str(e)}"
        print(error_msg)
        return error_msg


@flow(name="hive_script")
def hive_script_flow() -> Dict[str, Any]:
    """
    Flow that executes bash command followed by Hive script operations.
    
    Replicates Airflow DAG 'hive_script' with daily schedule at 1:00 AM.
    Schedule and start date are configured at deployment time in Prefect 2.x.
    """
    # Execute tasks sequentially: bash first, then hive
    username = run_after_loop()
    hive_result = hive_script_task()
    
    return {
        "bash_output": username,
        "hive_output": hive_result
    }


if __name__ == '__main__':
    # Local execution for testing. For scheduled deployment:
    # prefect deployment build hive_script.py:hive_script_flow \
    #   --name "daily-hive-script" \
    #   --cron "0 1 * * *" \
    #   --apply
    hive_script_flow()