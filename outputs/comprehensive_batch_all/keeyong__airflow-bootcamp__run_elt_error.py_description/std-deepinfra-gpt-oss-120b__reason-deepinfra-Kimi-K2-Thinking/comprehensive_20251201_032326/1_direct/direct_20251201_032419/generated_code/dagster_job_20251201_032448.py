from dataclasses import dataclass
from typing import List

from dagster import (
    OpInvocation,
    ResourceDefinition,
    job,
    op,
    resource,
)


@dataclass(frozen=True)
class TableConfig:
    """Configuration for a single table processing step."""

    schema: str
    name: str
    sql: str


# Example table configurations; replace with real definitions as needed.
TABLES: List[TableConfig] = [
    TableConfig(
        schema="analytics",
        name="sales",
        sql="SELECT * FROM raw.sales_data",
    ),
    TableConfig(
        schema="analytics",
        name="customers",
        sql="SELECT * FROM raw.customer_data",
    ),
    TableConfig(
        schema="analytics",
        name="products",
        sql="SELECT * FROM raw.product_data",
    ),
]


class SnowflakeConnection:
    """Minimal stub for Snowflake interactions."""

    def execute(self, sql: str) -> None:
        """Execute a SQL statement."""
        print(f"[Snowflake] Executing SQL: {sql}")

    def query_scalar(self, sql: str) -> int:
        """Execute a scalar query and return an integer result."""
        print(f"[Snowflake] Querying scalar: {sql}")
        # Stub returns 1 to indicate at least one row.
        return 1


class SlackClient:
    """Minimal stub for Slack messaging."""

    def send_message(self, text: str) -> None:
        """Send a message to Slack."""
        print(f"[Slack] {text}")


@resource
def snowflake_resource(_):
    """Provides a SnowflakeConnection instance."""
    return SnowflakeConnection()


@resource
def slack_resource(_):
    """Provides a SlackClient instance."""
    return SlackClient()


def make_table_op(table_cfg: TableConfig):
    """Factory that creates a Dagster op for a specific table."""

    @op(
        name=f"process_{table_cfg.schema}_{table_cfg.name}",
        required_resource_keys={"snowflake", "slack"},
    )
    def _process_table(context):
        """Creates, validates, ensures, and swaps a Snowflake table."""
        sf: SnowflakeConnection = context.resources.snowflake
        slack: SlackClient = context.resources.slack

        temp_table = f"{table_cfg.schema}.{table_cfg.name}_temp"
        target_table = f"{table_cfg.schema}.{table_cfg.name}"

        try:
            # 1. Create temporary table via CTAS.
            sf.execute(f"CREATE OR REPLACE TABLE {temp_table} AS {table_cfg.sql}")

            # 2. Validate that the temporary table contains at least one record.
            row_count = sf.query_scalar(f"SELECT COUNT(*) FROM {temp_table}")
            if row_count < 1:
                raise ValueError(f"Temporary table {temp_table} is empty.")

            # 3. Ensure the target table exists (create empty version if missing).
            sf.execute(
                f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}"
            )

            # 4. Swap the temporary table with the target table.
            sf.execute(
                f"ALTER TABLE {target_table} SWAP WITH {temp_table}"
            )

            context.log.info(
                f"Successfully processed table {target_table}."
            )
        except Exception as exc:
            # Notify Slack on failure and re‑raise to mark the op as failed.
            slack.send_message(
                f"❗️ Failure processing table {target_table}: {exc}"
            )
            raise

    return _process_table


# Dynamically create an op for each table configuration.
TABLE_OPS: List[op] = [make_table_op(cfg) for cfg in TABLES]


@job(
    resource_defs={
        "snowflake": snowflake_resource,
        "slack": slack_resource,
    }
)
def daily_elt_job():
    """Orchestrates sequential processing of all configured tables."""
    previous_invocation: OpInvocation | None = None

    for table_op in TABLE_OPS:
        current_invocation = table_op()
        if previous_invocation is not None:
            current_invocation.set_upstream(previous_invocation)
        previous_invocation = current_invocation


if __name__ == "__main__":
    # Execute the job in-process for quick testing.
    result = daily_elt_job.execute_in_process()
    if result.success:
        print("Dagster job completed successfully.")
    else:
        print("Dagster job failed.")