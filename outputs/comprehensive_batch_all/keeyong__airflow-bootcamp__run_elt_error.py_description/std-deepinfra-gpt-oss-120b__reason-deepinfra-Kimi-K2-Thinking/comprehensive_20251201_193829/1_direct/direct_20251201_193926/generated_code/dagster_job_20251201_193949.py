from typing import Any, Dict, List

from dagster import (
    InitResourceContext,
    Nothing,
    Out,
    ResourceDefinition,
    In,
    JobDefinition,
    op,
    job,
    schedule,
)


# Example table configuration; replace with real definitions as needed.
TABLES: List[Dict[str, str]] = [
    {
        "schema": "analytics",
        "table_name": "sales",
        "sql": "SELECT * FROM raw.sales_data",
    },
    {
        "schema": "analytics",
        "table_name": "customers",
        "sql": "SELECT * FROM raw.customer_data",
    },
    {
        "schema": "analytics",
        "table_name": "products",
        "sql": "SELECT * FROM raw.product_data",
    },
]


def snowflake_resource(init_context: InitResourceContext) -> Any:
    """A minimal Snowflake client stub."""

    class SnowflakeClient:
        def execute(self, sql: str) -> None:
            init_context.log.info(f"Snowflake EXECUTE: {sql}")

        def query_one(self, sql: str) -> Any:
            init_context.log.info(f"Snowflake QUERY ONE: {sql}")
            # Stub returns a positive count for validation.
            return 1

    return SnowflakeClient()


def slack_resource(init_context: InitResourceContext) -> Any:
    """A minimal Slack client stub."""

    class SlackClient:
        def send_message(self, text: str) -> None:
            init_context.log.info(f"Slack MESSAGE: {text}")

    return SlackClient()


resource_defs = {
    "snowflake": ResourceDefinition.resource_fn(snowflake_resource),
    "slack": ResourceDefinition.resource_fn(slack_resource),
}


@op(
    required_resource_keys={"snowflake", "slack"},
    out=Out(Nothing),
    config_schema={"schema": str, "table_name": str, "sql": str},
)
def process_table(context: Any, _: Nothing) -> Nothing:
    """
    Executes the ELT steps for a single table:
    1. Create a temporary table via CTAS.
    2. Validate the temporary table contains at least one row.
    3. Ensure the target table exists.
    4. Swap the temporary table with the target table.
    """
    cfg: Dict[str, str] = context.op_config
    schema = cfg["schema"]
    table_name = cfg["table_name"]
    sql = cfg["sql"]
    run_id = context.run_id
    temp_table = f"{schema}.{table_name}_temp_{run_id.replace('-', '_')}"

    snowflake = context.resources.snowflake
    slack = context.resources.slack

    try:
        # 1. Create temporary table.
        ctas_sql = f"CREATE OR REPLACE TABLE {temp_table} AS {sql}"
        snowflake.execute(ctas_sql)
        context.log.info(f"Created temporary table {temp_table}")

        # 2. Validate temporary table has rows.
        count_sql = f"SELECT COUNT(*) FROM {temp_table}"
        row_count = snowflake.query_one(count_sql)
        if not row_count or int(row_count) < 1:
            raise ValueError(f"Validation failed: {temp_table} contains no rows.")
        context.log.info(f"Validation passed: {temp_table} has {row_count} rows")

        # 3. Ensure target table exists (create empty if missing).
        target_table = f"{schema}.{table_name}"
        ensure_sql = f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}"
        snowflake.execute(ensure_sql)
        context.log.info(f"Ensured target table {target_table} exists")

        # 4. Swap tables.
        swap_sql = f"ALTER TABLE {target_table} SWAP WITH {temp_table}"
        snowflake.execute(swap_sql)
        context.log.info(f"Swapped {temp_table} with {target_table}")

    except Exception as exc:  # pylint: disable=broad-except
        error_msg = f"ELT failed for {schema}.{table_name}: {exc}"
        slack.send_message(error_msg)
        raise

    return None


@job(resource_defs=resource_defs)
def daily_elt_job() -> None:
    """
    Orchestrates processing of all tables defined in TABLES.
    Each table's processing depends on the previous one to enforce sequential execution.
    """
    previous: Nothing = None
    for table_cfg in TABLES:
        op_instance = process_table.alias(f"process_{table_cfg['table_name']}")
        if previous is None:
            previous = op_instance(config=table_cfg)
        else:
            previous = op_instance(previous, config=table_cfg)


@schedule(
    cron_schedule="0 2 * * *",  # Daily at 02:00 UTC
    job=daily_elt_job,
    execution_timezone="UTC",
)
def daily_elt_schedule() -> Dict[str, Any]:
    """Schedule that triggers the daily ELT job."""
    return {}


if __name__ == "__main__":
    result = daily_elt_job.execute_in_process()
    assert result.success, "Job execution failed"