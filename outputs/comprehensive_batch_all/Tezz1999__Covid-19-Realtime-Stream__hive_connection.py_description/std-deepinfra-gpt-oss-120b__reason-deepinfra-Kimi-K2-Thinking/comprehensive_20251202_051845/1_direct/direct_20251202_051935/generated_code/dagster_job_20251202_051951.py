from __future__ import annotations

import getpass
import subprocess
from typing import Any, Dict

from dagster import ConfigurableResource, OpExecutionContext, job, op, resource, ScheduleDefinition, DefaultScheduleStatus


class HiveResource(ConfigurableResource):
    """A minimal stub for a Hive connection resource.

    In a real deployment this would manage a connection to a Hive server,
    e.g. using PyHive or a custom client.
    """

    connection_name: str = "hive_local"

    def execute_query(self, query: str) -> None:
        """Execute a Hive query.

        This stub simply logs the query; replace with actual execution logic.
        """
        print(f"[HiveResource] Executing query on connection '{self.connection_name}':")
        print(query)


@op(required_resource_keys={"hive"})
def run_after_loop(context: OpExecutionContext) -> str:
    """Echo the current system username using a Bash command."""
    try:
        result = subprocess.run(
            ["bash", "-c", "echo $USER"], capture_output=True, text=True, check=True
        )
        username = result.stdout.strip()
        context.log.info(f"Current username (from Bash): {username}")
    except subprocess.CalledProcessError as exc:
        # Fallback to Python's getpass if Bash fails
        username = getpass.getuser()
        context.log.warning(
            f"Bash command failed (returncode={exc.returncode}); using getpass: {username}"
        )
    return username


@op(required_resource_keys={"hive"})
def hive_script_task(context: OpExecutionContext, _: str) -> None:
    """Run a multi‑statement Hive script that creates a database, table, and inserts a record."""
    hive_query = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """
    hive_resource: HiveResource = context.resources.hive
    hive_resource.execute_query(hive_query)
    context.log.info("Hive script executed successfully.")


@job(resource_defs={"hive": resource(HiveResource)})
def hive_script_job():
    """Dagster job that mirrors the linear Airflow DAG described."""
    username = run_after_loop()
    hive_script_task(username)


# Optional daily schedule at 01:00 AM
daily_hive_schedule = ScheduleDefinition(
    job=hive_script_job,
    cron_schedule="0 1 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs the Hive script job daily at 01:00 AM.",
)


if __name__ == "__main__":
    # Execute the job in-process for quick testing.
    result = hive_script_job.execute_in_process()
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")