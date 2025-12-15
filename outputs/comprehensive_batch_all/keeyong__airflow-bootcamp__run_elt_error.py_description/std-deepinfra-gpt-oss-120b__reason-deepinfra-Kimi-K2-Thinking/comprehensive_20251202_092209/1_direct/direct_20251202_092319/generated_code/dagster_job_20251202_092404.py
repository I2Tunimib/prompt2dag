from __future__ import annotations

import time
from typing import Any, Dict, List

from dagster import (
    Config,
    OpExecutionContext,
    ResourceDefinition,
    ScheduleDefinition,
    ScheduleStatus,
    graph,
    job,
    op,
    schedule,
)


# ----------------------------------------------------------------------
# Configuration for tables to process
# ----------------------------------------------------------------------
TABLES: List[Dict[str, str]] = [
    {
        "schema": "PUBLIC",
        "table_name": "orders",
        "sql": "SELECT * FROM raw.orders",
    },
    {
        "schema": "PUBLIC",
        "table_name": "customers",
        "sql": "SELECT * FROM raw.customers",
    },
    # Add additional table definitions as needed
]


# ----------------------------------------------------------------------
# Mock Snowflake resource (replace with real implementation)
# ----------------------------------------------------------------------
class MockSnowflakeConnection:
    """A very small mock of a Snowflake connection for demonstration purposes."""

    def cursor(self) -> "MockSnowflakeConnection":
        return self

    def execute(self, sql: str) -> None:
        print(f"[Snowflake] Executing SQL: {sql}")

    def fetchone(self) -> tuple[int]:
        # Always return a count of 1 for validation queries
        return (1,)

    def commit(self) -> None:
        print("[Snowflake] Commit transaction")

    def close(self) -> None:
        print("[Snowflake] Close connection")


def mock_snowflake_resource(_init_context) -> MockSnowflakeConnection:
    """Return a mock Snowflake connection."""
    return MockSnowflakeConnection()


# ----------------------------------------------------------------------
# Mock Slack resource (replace with real implementation)
# ----------------------------------------------------------------------
class MockSlackClient:
    """A very small mock of a Slack client for demonstration purposes."""

    def send_message(self, text: str) -> None:
        print(f"[Slack] {text}")


def mock_slack_resource(_init_context) -> MockSlackClient:
    """Return a mock Slack client."""
    return MockSlackClient()


# ----------------------------------------------------------------------
# Op configuration schema
# ----------------------------------------------------------------------
class TableConfig(Config):
    schema: str
    table_name: str
    sql: str


# ----------------------------------------------------------------------
# Core op that processes a single table
# ----------------------------------------------------------------------
@op(
    required_resource_keys={"snowflake", "slack"},
    config_schema=TableConfig,
)
def process_table(context: OpExecutionContext) -> None:
    """Create a temporary table, validate, ensure target exists, and swap."""
    cfg: TableConfig = context.op_config
    schema = cfg.schema
    table_name = cfg.table_name
    source_sql = cfg.sql

    temp_table = f"{table_name}_temp_{int(time.time())}"
    ctas_sql = f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} AS {source_sql}"
    validate_sql = f"SELECT COUNT(*) FROM {temp_table}"
    ensure_target_sql = (
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} LIKE {temp_table}"
    )
    swap_sql = f"ALTER TABLE {schema}.{table_name} SWAP WITH {temp_table}"

    conn = context.resources.snowflake
    cursor = conn.cursor()
    try:
        # Step 1: Create temporary table
        cursor.execute(ctas_sql)

        # Step 2: Validate that at least one row exists
        cursor.execute(validate_sql)
        row_count = cursor.fetchone()[0]
        if row_count == 0:
            raise Exception(
                f"Validation failed for temporary table {temp_table}: zero rows"
            )

        # Step 3: Ensure target table exists
        cursor.execute(ensure_target_sql)

        # Step 4: Swap temporary and target tables
        cursor.execute(swap_sql)

        # Commit the transaction
        conn.commit()
        context.log.info(
            f"Successfully processed table {schema}.{table_name} (temp: {temp_table})"
        )
    except Exception as exc:
        # Send Slack notification on failure
        slack = context.resources.slack
        slack.send_message(
            f"ELT job failed for table {schema}.{table_name}: {exc}"
        )
        raise
    finally:
        cursor.close()
        conn.close()


# ----------------------------------------------------------------------
# Graph that wires the per‑table ops sequentially
# ----------------------------------------------------------------------
@graph
def daily_elt_graph() -> None:
    """Create a linear chain of `process_table` ops based on TABLES."""
    previous_op = None
    for table_cfg in TABLES:
        op_name = f"process_{table_cfg['table_name']}"
        configured_op = process_table.alias(op_name).configured(table_cfg)()
        if previous_op is not None:
            previous_op >> configured_op
        previous_op = configured_op


# ----------------------------------------------------------------------
# Job definition with resources
# ----------------------------------------------------------------------
daily_elt_job = daily_elt_graph.to_job(
    resource_defs={
        "snowflake": ResourceDefinition.mock_resource(mock_snowflake_resource),
        "slack": ResourceDefinition.mock_resource(mock_slack_resource),
    }
)


# ----------------------------------------------------------------------
# Daily schedule (UTC midnight)
# ----------------------------------------------------------------------
@schedule(
    cron_schedule="0 0 * * *",
    job=daily_elt_job,
    execution_timezone="UTC",
    default_status=ScheduleStatus.RUNNING,
)
def daily_elt_schedule():
    """Run the ELT job once per day."""
    return {}


# ----------------------------------------------------------------------
# Entry point for local execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    result = daily_elt_job.execute_in_process()
    if result.success:
        print("ELT pipeline completed successfully.")
    else:
        print("ELT pipeline failed.")