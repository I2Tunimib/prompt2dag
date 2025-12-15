import os
import logging
import uuid
import requests
import snowflake.connector
from prefect import flow, task

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for tables to process
TABLES = [
    {
        "schema": "PUBLIC",
        "target_table": "CUSTOMER_DIM",
        "sql": "SELECT * FROM RAW_DB.RAW_CUSTOMER",
    },
    {
        "schema": "PUBLIC",
        "target_table": "ORDER_FACT",
        "sql": "SELECT * FROM RAW_DB.RAW_ORDER",
    },
    # Add additional table definitions as needed
]


def get_snowflake_connection():
    """
    Create a Snowflake connection using environment variables.
    Expected variables:
        SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_ROLE (optional)
    """
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE", None),
    )


@task
def slack_notify(message: str):
    """
    Send a message to Slack via Incoming Webhook.
    The webhook URL must be set in the SLACK_WEBHOOK_URL environment variable.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("Slack webhook URL not configured; skipping notification.")
        return

    payload = {"text": message}
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Slack notification sent.")
    except Exception as exc:
        logger.error("Failed to send Slack notification: %s", exc)


@task
def process_table(table_config: dict):
    """
    Process a single table:
    1. Create a temporary table using CTAS.
    2. Validate the temporary table contains at least one row.
    3. Ensure the target table exists (create empty version if missing).
    4. Swap the temporary table with the target table.
    """
    schema = table_config["schema"]
    target_table = table_config["target_table"]
    sql = table_config["sql"]
    temp_table = f"{target_table}_temp_{uuid.uuid4().hex[:8]}"

    logger.info("Processing table %s.%s using temporary table %s", schema, target_table, temp_table)

    try:
        conn = get_snowflake_connection()
        try:
            cur = conn.cursor()

            # 1. Create temporary table
            create_temp_stmt = f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} AS {sql}"
            logger.debug("Executing: %s", create_temp_stmt)
            cur.execute(create_temp_stmt)

            # 2. Validate temporary table has rows
            cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
            row_count = cur.fetchone()[0]
            logger.debug("Temporary table %s row count: %d", temp_table, row_count)
            if row_count == 0:
                raise ValueError(f"Temporary table {temp_table} is empty after CTAS.")

            # 3. Ensure target table exists (create empty version if missing)
            ensure_target_stmt = (
                f"CREATE TABLE IF NOT EXISTS {schema}.{target_table} "
                f"LIKE {temp_table}"
            )
            logger.debug("Executing: %s", ensure_target_stmt)
            cur.execute(ensure_target_stmt)

            # 4. Swap tables
            swap_stmt = f"ALTER TABLE {schema}.{target_table} SWAP WITH {temp_table}"
            logger.debug("Executing: %s", swap_stmt)
            cur.execute(swap_stmt)

            logger.info("Successfully processed table %s.%s", schema, target_table)

        finally:
            cur.close()
            conn.close()
    except Exception as exc:
        error_msg = (
            f"Failed processing table {schema}.{target_table}: {exc}"
        )
        logger.error(error_msg)
        slack_notify.submit(error_msg)  # Notify via Slack
        raise  # Re‑raise to let Prefect record the failure


@flow
def daily_elt_flow():
    """
    Orchestrates the ELT process for all configured tables.
    Executes each table processing task sequentially.
    """
    for table_cfg in TABLES:
        process_table(table_cfg)


if __name__ == "__main__":
    # This guard allows local execution of the flow.
    # In production, configure a Prefect deployment with a daily schedule.
    daily_elt_flow()