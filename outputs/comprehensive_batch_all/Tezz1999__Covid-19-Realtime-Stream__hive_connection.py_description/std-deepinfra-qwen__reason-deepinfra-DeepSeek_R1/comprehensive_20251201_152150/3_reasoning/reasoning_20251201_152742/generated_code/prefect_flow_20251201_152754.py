from prefect import flow, task
import subprocess
from typing import Tuple

@task
def run_after_loop_task() -> Tuple[str, str]:
    """Execute bash command to echo current username"""
    result = subprocess.run(
        ["echo", "$USER"],
        capture_output=True,
        text=True,
        shell=True
    )
    return (result.stdout.strip(), result.stderr.strip())

@task
def hive_script_task() -> None:
    """Execute Hive operations to create database/table and insert record"""
    # In production, replace with actual Hive connection using:
    # - PyHive library with hive_local connection details
    # - Prefect Hive connector when available
    print("Creating database 'mydb' if not exists")
    print("Creating table 'mydb.test_af' with column 'test' INT")
    print("Inserting record with value 2 into test_af")

@flow(name="hive_script")
def hive_script_flow() -> None:
    """Orchestrate Hive database operations pipeline"""
    # To configure daily 1:00 AM schedule:
    # prefect deployment build --name prod --schedule "0 1 * * *" ...
    bash_result = run_after_loop_task()
    hive_script_task()

if __name__ == "__main__":
    hive_script_flow()