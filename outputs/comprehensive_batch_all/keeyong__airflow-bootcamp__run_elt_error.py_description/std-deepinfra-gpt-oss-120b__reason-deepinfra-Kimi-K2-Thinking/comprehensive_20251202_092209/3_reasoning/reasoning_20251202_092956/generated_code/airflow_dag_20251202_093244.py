from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.notifications.slack import send_slack_notification
from datetime import datetime, timedelta
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

TABLES = [
    {
        'schema': 'ANALYTICS',
        'table': 'USER_METRICS',
        'sql': """
            SELECT 
                user_id, 
                COUNT(*) as event_count,
                CURRENT_DATE() as processed_date
            FROM RAW.EVENTS 
            WHERE event_date = CURRENT_DATE() - 1
            GROUP BY user_id
        """
    },
    {
        'schema': 'ANALYTICS',
        'table': 'REVENUE_DAILY',
        'sql': """
            SELECT 
                DATE(created_at) as dt, 
                SUM(amount) as revenue,
                COUNT(DISTINCT transaction_id) as transaction_count
            FROM RAW.TRANSACTIONS 
            WHERE created_at >= CURRENT_DATE() - 1
            GROUP BY dt
        """
    },
]

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_notification(
        slack_conn_id='slack_default',
        text=":red_circle: Task failed in {{ dag.dag_id }}: {{ task.task_id }}",
        channel='#data-alerts'
    )
}

def get_snowflake_hook() -> SnowflakeHook:
    return SnowflakeHook(snowflake_conn_id='snowflake_default')

def execute_query(query: str, autocommit: bool = True) -> Any:
    hook = get_snowflake_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if autocommit:
            conn.commit()
        return cursor
    except Exception as e:
        conn.rollback()
        logger.error(f"Query execution failed: {query[:200]}...")
        raise e
    finally:
        cursor.close()
        conn.close()

def create_temp_table(schema: str, table: str, sql: str) -> str:
    temp_table = f"{schema}.{table}_TEMP"
    query = f"CREATE OR REPLACE TABLE {temp_table} AS {sql}"
    logger.info(f"Creating temp table: {temp_table}")
    execute_query(query)
    return temp_table

def validate_temp_table(temp_table: str) -> None:
    query = f"SELECT COUNT(*) as cnt FROM {temp_table}"
    cursor = execute_query(query)
    result = cursor.fetchone()
    record_count = result[0]
    
    if record_count == 0:
        raise ValueError(f"Validation failed: {temp_table} contains no records")
    
    logger.info(f"Validation passed: {temp_table} contains {record_count} records")

def ensure_target_table_exists(schema: str, table: str) -> None:
    target_table = f"{schema}.{table}"
    temp_table = f"{schema}.{table}_TEMP"
    query = f"CREATE TABLE IF NOT EXISTS {target_table} (LIKE {temp_table})"
    logger.info(f"Ensuring target table exists: {target_table}")
    execute_query(query)

def swap_tables(schema: str, table: str) -> None:
    temp_table = f"{schema}.{table}_TEMP"
    target_table = f"{schema}.{table}"
    query = f"ALTER TABLE {temp_table} SWAP WITH {target_table}"
    logger.info(f"Swapping {temp_table} with {target_table}")
    execute_query(query)

def process_table(table_config: Dict[str, str], **context) -> None:
    schema = table_config['schema']
    table = table_config['table']
    sql = table_config['sql']
    
    logger.info(f"Starting processing for {schema}.{table}")
    
    try:
        temp_table = create_temp_table(schema, table, sql)
        validate_temp_table(temp_table)
        ensure_target_table_exists(schema, table)
        swap_tables(schema, table)
        logger.info(f"Successfully completed processing for {schema}.{table}")
    except Exception as e:
        logger.error(f"Failed to process {schema}.{table}: {str(e)}")
        raise

with DAG(
    'daily_snowflake_elt',
    default_args=default_args,
    description='Daily ELT process for analytics tables in Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['snowflake', 'elt', 'analytics'],
) as dag:
    
    previous_task = None
    
    for table_config in TABLES:
        schema = table_config['schema']
        table = table_config['table']
        task_id = f"process_table_{schema.lower()}_{table.lower()}"
        
        current_task = PythonOperator(
            task_id=task_id,
            python_callable=process_table,
            op_kwargs={'table_config': table_config},
        )
        
        if previous_task:
            previous_task >> current_task
        
        previous_task = current_task