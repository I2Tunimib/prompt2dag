import os
from typing import Dict, List, Any, Optional
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration for tables to process
TABLES = [
    {
        "schema": "ANALYTICS",
        "table": "DAILY_USER_METRICS",
        "sql": """
            SELECT 
                user_id,
                COUNT(*) as event_count,
                MAX(event_timestamp) as last_event_date
            FROM RAW.EVENTS
            WHERE event_timestamp >= CURRENT_DATE - 1
            GROUP BY user_id
        """
    },
    {
        "schema": "ANALYTICS",
        "table": "DAILY_REVENUE_SUMMARY",
        "sql": """
            SELECT 
                DATE(order_date) as order_day,
                SUM(order_amount) as total_revenue,
                COUNT(*) as order_count
            FROM RAW.ORDERS
            WHERE order_date >= CURRENT_DATE - 1
            GROUP BY DATE(order_date)
        """
    }
]

def get_snowflake_connection() -> SnowflakeConnection:
    """Establish and return a Snowflake connection."""
    try:
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER", "your_user"),
            password=os.getenv("SNOWFLAKE_PASSWORD", "your_password"),
            account=os.getenv("SNOWFLAKE_ACCOUNT", "your_account"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "PROD_DB"),
            role=os.getenv("SNOWFLAKE_ROLE", "ANALYST_ROLE")
        )
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Snowflake: {e}")

def send_slack_notification(message: str, channel: str = "#data-pipeline-alerts") -> None:
    """Send a Slack notification."""
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        return
    
    try:
        client = WebClient(token=token)
        client.chat_postMessage(channel=channel, text=message)
    except SlackApiError as e:
        print(f"Failed to send Slack notification: {e.response['error']}")

@task(
    retries=2,
    retry_delay_seconds=60,
    name="process-table"
)
def process_table(table_config: Dict[str, Any]) -> str:
    """
    Process a single table through the ELT pipeline:
    1. Create temp table via CTAS
    2. Validate data integrity (at least one record)
    3. Ensure target table exists
    4. Swap temp table with target table
    """
    logger = get_run_logger()
    schema = table_config["schema"]
    table_name = table_config["table"]
    sql = table_config["sql"]
    
    temp_table = f"{schema}.{table_name}_TEMP"
    target_table = f"{schema}.{table_name}"
    
    conn: Optional[SnowflakeConnection] = None
    
    try:
        logger.info(f"Starting processing for table: {target_table}")
        
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        logger.info(f"Creating temporary table: {temp_table}")
        create_temp_sql = f"CREATE OR REPLACE TABLE {temp_table} AS {sql}"
        cur.execute(create_temp_sql)
        
        logger.info(f"Validating data in temporary table: {temp_table}")
        cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
        row_count = cur.fetchone()[0]
        
        if row_count == 0:
            raise ValueError(f"Temporary table {temp_table} contains no records")
        
        logger.info(f"Validation passed: {row_count} records found in {temp_table}")
        
        logger.info(f"Ensuring target table exists: {target_table}")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}")
        
        logger.info(f"Swapping {temp_table} with {target_table}")
        cur.execute(f"ALTER TABLE {temp_table} SWAP WITH {target_table}")
        
        conn.commit()
        
        logger.info(f"Successfully processed table: {target_table}")
        return f"SUCCESS: {target_table}"
        
    except Exception as e:
        logger.error(f"Failed to process table {target_table}: {str(e)}")
        
        try:
            context = get_run_context()
            flow_run_name = context.flow_run.name if context and context.flow_run else "unknown"
            error_message = (
                f"❌ ELT Pipeline Failure\n"
                f"Table: `{target_table}`\n"
                f"Flow: `{flow_run_name}`\n"
                f"Error: {str(e)}"
            )
            send_slack_notification(error_message)
        except Exception as slack_error:
            logger.warning(f"Failed to send Slack notification: {slack_error}")
        
        raise
        
    finally:
        if conn:
            conn.close()

@flow(
    name="daily-snowflake-elt-pipeline",
    description="Daily ELT pipeline that processes analytics tables sequentially in Snowflake"
)
def daily_snowflake_elt_pipeline(tables: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    """
    Daily ELT pipeline that processes analytics tables sequentially.
    Each table is processed in a linear chain with no parallel execution.
    """
    logger = get_run_logger()
    
    if tables is None:
        tables = TABLES
    
    logger.info(f"Starting daily ELT pipeline for {len(tables)} tables")
    
    results: List[str] = []
    for table_config in tables:
        result = process_table(table_config)
        results.append(result)
    
    logger.info("Daily ELT pipeline completed successfully")
    return results

if __name__ == "__main__":
    daily_snowflake_elt_pipeline()