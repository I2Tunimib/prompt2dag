import os
from typing import Dict, List

from prefect import flow, task
from prefect.context import get_run_context

import snowflake.connector
from slack_sdk.webhook import WebhookClient


# Configuration for tables to process
TABLES: List[Dict[str, str]] = [
    {
        "schema": "ANALYTICS",
        "table_name": "customer_summary",
        "sql": """
            SELECT 
                customer_id, 
                COUNT(*) as order_count, 
                SUM(amount) as total_spent 
            FROM raw.orders 
            GROUP BY customer_id
        """
    },
    {
        "schema": "ANALYTICS",
        "table_name": "product_performance",
        "sql": """
            SELECT 
                product_id, 
                SUM(quantity) as units_sold, 
                SUM(revenue) as total_revenue 
            FROM raw.order_items 
            GROUP BY product_id
        """
    },
    {
        "schema": "ANALYTICS",
        "table_name": "daily_sales",
        "sql": """
            SELECT 
                DATE(order_date) as sale_date, 
                COUNT(*) as order_count, 
                SUM(amount) as daily_revenue 
            FROM raw.orders 
            GROUP BY sale_date
        """
    }
]


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Establish and return a Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "PROD_DB"),
        role=os.getenv("SNOWFLAKE_ROLE", "ANALYTICS_ROLE")
    )


def send_slack_notification(message: str) -> None:
    """Send a notification to Slack via webhook."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return
    
    client = WebhookClient(webhook_url)
    client.send(text=message)


@task(retries=2, retry_delay_seconds=30)
def create_temp_table(table_config: Dict[str, str]) -> None:
    """Create a temporary table using CTAS."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        temp_table = f"{table_config['schema']}.temp_{table_config['table_name']}"
        sql = f"CREATE OR REPLACE TABLE {temp_table} AS {table_config['sql']}"
        cursor.execute(sql)
    finally:
        conn.close()


@task(retries=2, retry_delay_seconds=30)
def validate_temp_table(table_config: Dict[str, str]) -> None:
    """Validate the temporary table contains at least one record."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        temp_table = f"{table_config['schema']}.temp_{table_config['table_name']}"
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
        count = cursor.fetchone()[0]
        
        if count == 0:
            raise ValueError(f"Temp table {temp_table} has no records")
    finally:
        conn.close()


@task(retries=2, retry_delay_seconds=30)
def ensure_target_table_exists(table_config: Dict[str, str]) -> None:
    """Ensure target table exists by creating empty version if missing."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        schema = table_config['schema']
        table_name = table_config['table_name']
        target_table = f"{schema}.{table_name}"
        
        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'"
        )
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            temp_table = f"{schema}.temp_{table_name}"
            cursor.execute(f"CREATE TABLE {target_table} LIKE {temp_table}")
    finally:
        conn.close()


@task(retries=2, retry_delay_seconds=30)
def swap_tables(table_config: Dict[str, str]) -> None:
    """Swap temporary table with target table."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        schema = table_config['schema']
        table_name = table_config['table_name']
        temp_table = f"{schema}.temp_{table_name}"
        target_table = f"{schema}.{table_name}"
        
        cursor.execute(f"ALTER TABLE {temp_table} SWAP WITH {target_table}")
    finally:
        conn.close()


@task
def send_failure_notification(error: Exception) -> None:
    """Send Slack notification for flow failure."""
    try:
        context = get_run_context()
        flow_run_name = context.flow_run.name
        message = f"❌ Daily ELT Flow failed: {flow_run_name}\nError: {str(error)}"
        send_slack_notification(message)
    except Exception:
        # Don't let notification failures break the flow
        pass


@flow(name="daily-elt-flow")
def daily_elt_flow() -> None:
    """
    Daily ELT process building analytics tables from raw Snowflake data.
    Executes tasks sequentially with linear dependencies between tables.
    """
    try:
        for table_config in TABLES:
            # Process each table sequentially
            create_temp_table(table_config)
            validate_temp_table(table_config)
            ensure_target_table_exists(table_config)
            swap_tables(table_config)
    except Exception as exc:
        send_failure_notification(exc)
        raise


if __name__ == "__main__":
    # Local execution entry point
    # For scheduled deployment, run:
    # prefect deployment build daily_elt_flow:daily_elt_flow \
    #   --name "daily-elt-deployment" \
    #   --cron "0 2 * * *" \
    #   --no-catchup
    daily_elt_flow()