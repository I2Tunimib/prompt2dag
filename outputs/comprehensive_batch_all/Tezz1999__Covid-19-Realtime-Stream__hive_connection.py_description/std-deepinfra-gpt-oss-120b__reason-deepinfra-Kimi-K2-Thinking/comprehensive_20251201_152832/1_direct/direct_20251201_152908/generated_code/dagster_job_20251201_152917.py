from datetime import datetime, timedelta
import getpass
import logging

from dagster import (
    ConfigurableResource,
    In,
    Out,
    OpExecutionContext,
    Output,
    ResourceDefinition,
    ScheduleDefinition,
    DefaultScheduleStatus,
    JobDefinition,
    job,
    op,
    schedule,
)


class HiveResource(ConfigurableResource):
    """Simple stub for a Hive connection resource."""

    connection_id: str = "hive_local"

    def execute_query(self, query: str) -> None:
        """Placeholder method to simulate executing a Hive query."""
        logger = logging.getLogger(__name__)
        logger.info("Executing Hive query on connection %s:\n%s", self.connection_id, query)
        # In a real implementation, this would use a Hive client library.
        # For this example we simply log the query.


@op(out=Out(str))
def run_after_loop(context: OpExecutionContext) -> str:
    """Echo the current system username (simulated via Python)."""
    username = getpass.getuser()
    context.log.info("Current system username: %s", username)
    return username


@op(
    required_resource_keys={"hive"},
    out=Out(str),
)
def hive_script_task(context: OpExecutionContext, username: str) -> str:
    """
    Execute a multi‑statement Hive script that creates a database, a table,
    and inserts a test record.
    """
    hive: HiveResource = context.resources.hive
    hive_query = """
        CREATE DATABASE IF NOT EXISTS mydb;
        CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
        INSERT INTO mydb.test_af VALUES (2);
    """
    context.log.info(
        "Running Hive script after username %s was retrieved.", username
    )
    hive.execute_query(hive_query)
    return "Hive script completed"


@job(
    resource_defs={"hive": ResourceDefinition.resource(HiveResource)},
)
def hive_script_job():
    """Linear job: Bash‑like step followed by Hive step."""
    username = run_after_loop()
    hive_script_task(username)


daily_hive_script_schedule = ScheduleDefinition(
    job=hive_script_job,
    cron_schedule="0 1 * * *",  # Daily at 01:00 UTC
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="UTC",
)


if __name__ == "__main__":
    result = hive_script_job.execute_in_process()
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")