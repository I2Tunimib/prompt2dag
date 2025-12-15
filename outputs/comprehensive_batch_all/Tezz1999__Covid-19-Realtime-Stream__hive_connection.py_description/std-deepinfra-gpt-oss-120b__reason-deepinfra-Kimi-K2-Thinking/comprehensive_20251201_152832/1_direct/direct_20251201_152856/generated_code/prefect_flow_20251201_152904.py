from __future__ import annotations

import getpass
import subprocess
from pathlib import Path

from prefect import flow, task


@task
def run_bash_echo_username() -> str:
    """
    Execute a Bash command that echoes the current system username.

    Returns
    -------
    str
        The username captured from the system.
    """
    username = getpass.getuser()
    # In a real Bash execution you might use subprocess:
    # result = subprocess.run(
    #     ["bash", "-c", "echo $USER"],
    #     capture_output=True,
    #     text=True,
    #     check=True,
    # )
    # username = result.stdout.strip()
    return username


@task
def run_hive_script(username: str) -> dict[str, str]:
    """
    Execute a Hive script that creates a database, a table, and inserts a test record.

    Parameters
    ----------
    username : str
        The username from the previous task; included here to demonstrate
        dependency handling.

    Returns
    -------
    dict[str, str]
        A dictionary containing the Hive command's stdout and stderr.
    """
    hive_query = """
        CREATE DATABASE IF NOT EXISTS mydb;
        CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
        INSERT INTO mydb.test_af VALUES (2);
    """
    # Trim whitespace for cleaner CLI execution
    hive_query = " ".join(line.strip() for line in hive_query.splitlines() if line.strip())

    # Assume the `hive` CLI is available and uses the 'hive_local' connection implicitly.
    # Adjust the command or environment variables as needed for your environment.
    command = ["hive", "-e", hive_query]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
    except subprocess.CalledProcessError as exc:
        stdout = exc.stdout.strip() if exc.stdout else ""
        stderr = exc.stderr.strip() if exc.stderr else str(exc)

    return {"stdout": stdout, "stderr": stderr, "triggered_by": username}


@flow
def hive_script_flow() -> None:
    """
    Prefect flow that runs a Bash command to echo the current username,
    then executes a Hive script to set up a database, table, and test record.
    """
    username = run_bash_echo_username()
    hive_result = run_hive_script(username)

    # Simple logging of results; in a real flow you might store these in a data store.
    print(f"Bash task output (username): {username}")
    print("Hive task stdout:")
    print(hive_result["stdout"])
    if hive_result["stderr"]:
        print("Hive task stderr:")
        print(hive_result["stderr"])


# Deployment note:
# This flow could be scheduled to run daily at 1:00 AM using a Prefect deployment,
# e.g., `prefect deployment create hive_script_flow.py:hive_script_flow --cron "0 1 * * *"`.

if __name__ == "__main__":
    hive_script_flow()