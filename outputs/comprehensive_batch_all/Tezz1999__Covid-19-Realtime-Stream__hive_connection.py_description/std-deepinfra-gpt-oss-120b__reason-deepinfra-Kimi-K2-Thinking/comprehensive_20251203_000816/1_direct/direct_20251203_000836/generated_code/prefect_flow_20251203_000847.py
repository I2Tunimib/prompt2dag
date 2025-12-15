import subprocess
from prefect import flow, task


@task
def run_after_loop() -> str:
    """Execute a Bash command that prints the current system username."""
    result = subprocess.run(
        ["bash", "-c", "echo $USER"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Bash command failed: {result.stderr.strip()}")
    username = result.stdout.strip()
    return username


@task
def hive_script_task() -> str:
    """Run a Hive script that creates a database, a table, and inserts a test record."""
    hive_query = """
        CREATE DATABASE IF NOT EXISTS mydb;
        CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
        INSERT INTO mydb.test_af VALUES (2);
    """
    # The Hive client must be available in the execution environment.
    result = subprocess.run(
        ["hive", "-e", hive_query],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Hive command failed: {result.stderr.strip()}")
    return result.stdout.strip()


@flow
def hive_script_flow() -> None:
    """Orchestrate the Bash and Hive tasks sequentially."""
    username = run_after_loop()
    # The Hive task runs after the Bash task completes.
    hive_output = hive_script_task()
    # Optionally log the outputs (Prefect automatically captures task returns).
    print(f"Username from Bash task: {username}")
    print(f"Hive task output: {hive_output}")


# Note: In a real deployment, configure a Prefect deployment with a daily schedule at 1:00 AM.
if __name__ == "__main__":
    hive_script_flow()