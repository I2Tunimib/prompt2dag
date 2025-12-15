from prefect import flow, task
from prefect.context import get_run_context
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for tables to process
# In production, load from YAML/JSON file or environment variables
TABLES: List[Dict[str, str]] = [
    {
        "schema": "ANALYTICS",
        "table_name": "DAILY_SALES_SUMMARY",
        "sql": """
            SELECT 
                date, 
                SUM(amount) as total_sales,
                COUNT(*) as transaction_count
            FROM RAW.SALES 
            WHERE date = CURRENT_DATE() - 1
            GROUP BY date
        """
    },
    {
        "schema": "ANALYTICS",
        "table_name": "DAILY_USER_ACTIVITY",
        "sql": """
            SELECT 
                date, 
                COUNT(DISTINCT user_id) as active_users,
                COUNT(*) as event_count
            FROM RAW.ACTIVITY 
            WHERE date = CURRENT_DATE() - 1
            GROUP BY date
        """
    }
]

def get_snowflake_connection():
    """
    Helper to get Snowflake connection.
    REPLACE THIS with actual implementation using snowflake-connector-python.
    """
    # Production implementation:
    # import snowflake.connector
    # return snowflake.connector.connect(
    #     user=SNOWFLAKE_USER,
    #     password=SNOWFLAKE_PASSWORD,
    #     account=SNOWFLAKE_ACCOUNT,
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE
    # )
    
    class MockConnection:
        def cursor(self):
            return MockCursor()
        def close(self):
            pass
    
    class MockCursor:
        def execute(self, query):
            logger.info(f"Executing: {query}")
        def fetchone(self):
            return [100]  # Simulate non-empty table
        def close(self):
            pass
    
    return MockConnection()

def send_slack_notification(message: str):
    """
    Helper to send Slack notification.
    REPLACE THIS with actual implementation using slack_sdk.
    """
    # Production implementation:
    # from slack_sdk import WebClient
    # client = WebClient(token=SLACK_BOT_TOKEN)
    # client.chat_postMessage(channel="#data-pipeline-alerts", text=message)
    
    logger.warning(f"SLACK NOTIFICATION: {message}")

def on_task_failure(task, state):
    """Callback for task failures to send Slack notifications."""
    try:
        run_context = get_run_context()
        flow_run_name = run_context.flow_run.name if run_context.flow_run else "unknown"
        task_run_name = run_context.task_run.name if run_context.task_run else "unknown"
        
        error_msg = (
            f"❌ ELT Pipeline Task Failed\n"
            f"Flow: {flow_run_name}\n"
            f"Task: {task_run_name}\n"
            f"State: {state}\n"
            f"Please check logs for details."
        )
        send_slack_notification(error_msg)
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

@task(on_failure=on_task_failure)
def create_temp_table(schema: str, table_name: str, sql: str) -> str:
    """
    Create a temporary table using CTAS statement.
    
    Args:
        schema: The schema name
        table_name: The target table name
        sql: The SQL query for CTAS
    
    Returns:
        The temporary table name
    """
    temp_table_name = f"{table_name}_TEMP"
    ctas_sql = f"CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS {sql}"
    
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(ctas_sql)
        logger.info(f"✅ Created temp table {schema}.{temp_table_name}")
        return temp_table_name
    except Exception as e:
        logger.error(f"Failed to create temp table {schema}.{temp_table_name}: {e}")
        raise
    finally:
        conn.close()

@task(on_failure=on_task_failure)
def validate_table_has_records(schema: str, table_name: str) -> bool:
    """
    Validate that the table contains at least one record.
    
    Args:
        schema: The schema name
        table_name: The table name
    
    Returns:
        True if table has records
    
    Raises:
        ValueError: If table has no records
    """
    count_sql = f"SELECT COUNT(*) FROM {schema}.{table_name}"
    
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(count_sql)
            result = cursor.fetchone()
            count = result[0] if result else 0
        
        if count == 0:
            raise ValueError(f"Table {schema}.{table_name} has no records")
        
        logger.info(f"✅ Validated {schema}.{table_name} has {count} records")
        return True
    except Exception as e:
        logger.error(f"Validation failed for {schema}.{table_name}: {e}")
        raise
    finally:
        conn.close()

@task(on_failure=on_task_failure)
def ensure_target_table_exists(schema: str, table_name: str):
    """
    Ensure the target table exists by creating an empty version if missing.
    
    Args:
        schema: The schema name
        table_name: The target table name
    """
    # Minimal schema - in production, derive from source table structure
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id INT,
        data VARIANT
    )
    """
    
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        logger.info(f"✅ Ensured target table {schema}.{table_name} exists")
    except Exception as e:
        logger.error(f"Failed to ensure target table {schema}.{table_name}: {e}")
        raise
    finally:
        conn.close()

@task(on_failure=on_task_failure)
def swap_tables(schema: str, temp_table_name: str, target_table_name: str):
    """
    Swap temporary table with target table using ALTER TABLE SWAP.
    
    Args:
        schema: The schema name
        temp_table_name: The temporary table name
        target_table_name: The target table name
    """
    swap_sql = f"ALTER TABLE {schema}.{temp_table_name} SWAP WITH {schema}.{target_table_name}"
    
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(swap_sql)
        logger.info(f"✅ Swapped {schema}.{temp_table_name} with {schema}.{target_table_name}")
    except Exception as e:
        logger.error(f"Failed to swap tables {schema}.{temp_table_name} <-> {schema}.{target_table_name}: {e}")
        raise
    finally:
        conn.close()

@flow
def elt_pipeline():
    """
    Daily ELT pipeline to build analytics tables from raw data in Snowflake.
    Processes tables sequentially with validation and Slack notifications on failure.
    """
    # Deployment configuration:
    # Run daily at 2 AM UTC with no catchup for missed runs
    # prefect deployment build elt_pipeline:elt_pipeline --name daily-elt --cron "0 2 * * *" --no-catchup
    
    for idx, table_config in enumerate(TABLES):
        schema = table_config["schema"]
        table_name = table_config["table_name"]
        sql = table_config["sql"]
        
        logger.info(f"Processing table {idx + 1}/{len(TABLES)}: {schema}.{table_name}")
        
        # Execute tasks sequentially for each table
        temp_table = create_temp_table(schema, table_name, sql)
        validate_table_has_records(schema, temp