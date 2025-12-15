import os
import logging
from typing import List, Dict

import snowflake.connector
from slack_sdk import WebClient
from prefect import flow, task

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example table configuration; replace with real definitions as needed
TABLES: List[Dict[str, str]] = [
    {
        "schema": "PUBLIC",
        "table_name": "orders",
        "sql": "SELECT * FROM raw.orders_raw",
    },
    {
        "schema": "PUBLIC",
        "table_name": "customers",
        "sql": "SELECT * FROM raw.customers_raw",
    },
]


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def send_slack_message(message: str) -> None:
    """Send a message to Slack using the bot token and channel from env vars."""
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL")
    if not token or not channel:
        logger.warning("Slack credentials not set; skipping Slack notification.")
        return
    client = WebClient(token=token)
    try:
        client.chat_postMessage(channel=channel, text=message)
        logger.info("Sent Slack notification.")
    except Exception as exc:
        logger.error("Failed to send Slack message: %s", exc)


def create_temp_table(
    conn: snowflake.connector.SnowflakeConnection,
    schema: str,
    table_name: str,
    sql: str,
) -> None:
    """Create or replace a temporary table using CTAS."""
    temp_table = f"{schema}.{table_name}_tmp"
    query = f"CREATE OR REPLACE TEMP TABLE {temp_table} AS {sql}"
    logger.info("Creating temporary table %s.", temp_table)
    conn.cursor().execute(query)


def validate_temp_table(
    conn: snowflake.connector.SnowflakeConnection,
    schema: str,
    table_name: str,
) -> None:
    """Validate that the temporary table contains at least one row."""
    temp_table = f"{schema}.{table_name}_tmp"
    query = f"SELECT COUNT(*) FROM {temp_table}"
    logger.info("Validating temporary table %s.", temp_table)
    cursor = conn.cursor()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError(f"Temporary table {temp_table} is empty.")
    logger.info("Temporary table %s contains %d rows.", temp_table, count)


def ensure_target_table(
    conn: snowflake.connector.SnowflakeConnection,
    schema: str,
    table_name: str,
) -> None:
    """Create the target table if it does not exist, using the temp table as a template."""
    target_table = f"{schema}.{table_name}"
    temp_table = f"{schema}.{table_name}_tmp"
    query = f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}"
    logger.info("Ensuring target table %s exists.", target_table)
    conn.cursor().execute(query)


def swap_tables(
    conn: snowflake.connector.SnowflakeConnection,
    schema: str,
    table_name: str,
) -> None:
    """Swap the temporary table with the target table."""
    temp_table = f"{schema}.{table_name}_tmp"
    target_table = f"{schema}.{table_name}"
    query = f"ALTER TABLE {temp_table} SWAP WITH {target_table}"
    logger.info("Swapping tables %s and %s.", temp_table, target_table)
    conn.cursor().execute(query)


@task
def process_table(table_config: Dict[str, str]) -> None:
    """
    Process a single table: create temp, validate, ensure target, and swap.

    Args:
        table_config: Dictionary with keys 'schema', 'table_name', and 'sql'.
    """
    schema = table_config["schema"]
    table_name = table_config["table_name"]
    sql = table_config["sql"]
    logger.info("Starting processing for %s.%s.", schema, table_name)

    conn = get_snowflake_connection()
    try:
        create_temp_table(conn, schema, table_name, sql)
        validate_temp_table(conn, schema, table_name)
        ensure_target_table(conn, schema, table_name)
        swap_tables(conn, schema, table_name)
        logger.info("Completed processing for %s.%s.", schema, table_name)
    except Exception as exc:
        error_msg = f"Failed processing table {schema}.{table_name}: {exc}"
        logger.error(error_msg)
        send_slack_message(error_msg)
        raise
    finally:
        conn.close()


@flow
def daily_elt_flow() -> None:
    """
    Orchestrates the ELT process for all configured tables sequentially.

    This flow is intended to be scheduled for daily execution.
    """
    for table_cfg in TABLES:
        process_table(table_cfg)


if __name__ == "__main__":
    # Local execution entry point
    daily_elt_flow()
    # Note: In production, configure a Prefect deployment with a daily schedule
    # and disable catch-up for missed runs.