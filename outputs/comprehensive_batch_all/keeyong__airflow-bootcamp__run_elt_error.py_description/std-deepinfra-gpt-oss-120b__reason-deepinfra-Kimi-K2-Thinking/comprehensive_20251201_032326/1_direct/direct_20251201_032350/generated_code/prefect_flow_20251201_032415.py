import os
import requests
import snowflake.connector
from prefect import flow, task


# Configuration for tables to process
TABLES = [
    {
        "schema": "PUBLIC",
        "table_name": "sales",
        "sql": "SELECT * FROM raw.sales_raw",
    },
    {
        "schema": "PUBLIC",
        "table_name": "customers",
        "sql": "SELECT * FROM raw.customers_raw",
    },
    # Add additional table definitions as needed
]


def get_snowflake_connection():
    """
    Create a Snowflake connection using environment variables.

    Expected environment variables:
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


def slack_notify(message: str):
    """
    Send a message to Slack using an incoming webhook URL.

    The webhook URL should be stored in the SLACK_WEBHOOK_URL environment variable.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return
    payload = {"text": message}
    try:
        requests.post(webhook_url, json=payload, timeout=10)
    except requests.RequestException:
        # In a real implementation you might log this failure
        pass


@task
def process_table(table_config: dict):
    """
    Process a single table:
    1. Create a temporary table using CTAS.
    2. Validate the temporary table contains at least one row.
    3. Ensure the target table exists.
    4. Swap the temporary table with the target table.
    """
    schema = table_config["schema"]
    table_name = table_config["table_name"]
    sql = table_config["sql"]
    temp_table = f"{schema}.tmp_{table_name}"
    target_table = f"{schema}.{table_name}"

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # 1. Create temporary table
        cur.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} AS {sql}"
        )

        # 2. Validate temporary table has rows
        cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError(f"Temporary table {temp_table} is empty.")

        # 3. Ensure target table exists (create empty copy if missing)
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}"
        )

        # 4. Swap tables
        cur.execute(
            f"ALTER TABLE {target_table} SWAP WITH {temp_table}"
        )

        conn.commit()
    except Exception as exc:
        slack_notify(
            f"❗️ Failed processing table `{schema}.{table_name}`: {exc}"
        )
        raise
    finally:
        cur.close()
        conn.close()


@flow
def daily_elt_flow():
    """
    Orchestrates the ELT process for all configured tables sequentially.
    """
    for table_cfg in TABLES:
        process_table(table_cfg)


if __name__ == "__main__":
    # This flow is intended to run daily via a Prefect deployment.
    # Deployment schedule (e.g., cron: "0 2 * * *") should be configured separately.
    daily_elt_flow()