import os
import subprocess
import tempfile

from prefect import flow, task, get_run_logger


@task
def run_after_loop() -> str:
    """
    Execute a Bash command that prints the current system username.

    Returns:
        The username string output from the Bash command.
    """
    logger = get_run_logger()
    try:
        result = subprocess.run(
            ["bash", "-c", "echo $USER"],
            capture_output=True,
            text=True,
            check=True,
        )
        username = result.stdout.strip()
        logger.info("Bash command output: %s", username)
        return username
    except subprocess.CalledProcessError as exc:
        logger.error("Bash command failed: %s", exc.stderr)
        raise


@task
def hive_script_task() -> str:
    """
    Run a Hive script that creates a database, a table, and inserts a test record.

    The script is written to a temporary file and executed with the `hive` CLI.
    Returns:
        The standard output from the Hive command.
    """
    logger = get_run_logger()
    hive_query = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """
    # Write the Hive query to a temporary file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".hql") as tmp_file:
        tmp_file.write(hive_query)
        script_path = tmp_file.name

    try:
        result = subprocess.run(
            ["hive", "-f", script_path],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("Hive command stdout: %s", result.stdout.strip())
        if result.stderr:
            logger.warning("Hive command stderr: %s", result.stderr.strip())
        return result.stdout.strip()
    except subprocess.CalledProcessError as exc:
        logger.error("Hive command failed: %s", exc.stderr)
        raise
    finally:
        # Clean up the temporary script file
        if os.path.exists(script_path):
            os.remove(script_path)


@flow
def hive_script_flow() -> None:
    """
    Orchestrates the pipeline:
    1. Run a Bash command to echo the current username.
    2. Execute a Hive script to create a database/table and insert a record.
    """
    # Step 1: Bash task
    _ = run_after_loop()
    # Step 2: Hive task (runs after Bash task completes)
    _ = hive_script_task()


if __name__ == "__main__":
    # Local execution entry point.
    # Note: In production, schedule this flow to run daily at 01:00 AM via a Prefect deployment.
    hive_script_flow()