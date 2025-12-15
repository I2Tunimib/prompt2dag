from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import os
import logging

logger = logging.getLogger(__name__)

TABLES = [
    {
        'schema': 'ANALYTICS',
        'table': 'user_activity_summary',
        'sql': """
            SELECT 
                user_id, 
                COUNT(*) as activity_count,
                MAX(event_time) as last_activity
            FROM raw.events 
            WHERE event_date = CURRENT_DATE - 1
            GROUP BY user_id
        """
    },
    {
        'schema': 'ANALYTICS',
        'table': 'revenue_daily',
        'sql': """
            SELECT 
                DATE(event_time) as date, 
                SUM(revenue) as total_revenue,
                COUNT(DISTINCT transaction_id) as transaction_count
            FROM raw.transactions 
            WHERE date = CURRENT_DATE - 1
            GROUP BY date
        """
    },
]

def notify_slack_failure(context: dict) -> None:
    """Send Slack notification on task failure."""
    slack_webhook_token = os.getenv('SLACK_WEBHOOK_TOKEN')
    if not slack_webhook_token:
        logger.warning("SLACK_WEBHOOK_TOKEN not set, skipping Slack notification")
        return
    
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    log_url = task_instance.log_url
    
    message = (
        f":red_circle: Task Failed\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Log:* {log_url}"
    )
    
    try:
        SlackWebhookOperator(
            task_id='slack_failure_notification',
            webhook_token=slack_webhook_token,
            message=message,
            username='Airflow',
        ).execute(context=context)
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

def get_snowflake_hook() -> SnowflakeHook:
    """Get Snowflake hook using default connection."""
    return SnowflakeHook(snowflake_conn_id='snowflake_default')

def create_temp_table(schema: str, table: str, sql: str, **context) -> str:
    """Create temporary table using CTAS."""
    hook = get_snowflake_hook()
    temp_table = f"{table}_TEMP"
    
    ctas_sql = f"CREATE OR REPLACE TABLE {schema}.{temp_table} AS {sql}"
    logger.info(f"Creating temp table: {schema}.{temp_table}")
    hook.run(ctas_sql)
    
    return temp_table

def validate_temp_table(schema: str, temp_table: str, **context) -> int:
    """Validate temporary table contains at least one record."""
    hook = get_snowflake_hook()
    
    count_sql = f"SELECT COUNT(*) FROM {schema}.{temp_table}"
    logger.info(f"Validating temp table: {schema}.{temp_table}")
    result = hook.get_first(count_sql)
    count = result[0] if result else 0
    
    if count == 0:
        raise ValueError(f"Temporary table {schema}.{temp_table} contains no records")
    
    logger.info(f"Temp table validation passed: {count} records")
    return count

def ensure_target_table_exists(schema: str, table: str, temp_table: str, **context) -> bool:
    """Ensure target table exists by creating empty version if missing."""
    hook = get_snowflake_hook()
    
    check_sql = f"""
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '{schema}' 
    AND TABLE_NAME = '{table}'
    """
    logger.info(f"Checking if target table exists: {schema}.{table}")
    result = hook.get_first(check_sql)
    exists = result[0] > 0 if result else False
    
    if not exists:
        logger.info(f"Target table does not exist, creating empty table: {schema}.{table}")
        create_sql = f"CREATE TABLE {schema}.{table} LIKE {schema}.{temp_table}"
        hook.run(create_sql)
    
    return exists

def swap_tables(schema: str, temp_table: str, target_table: str, **context) -> None:
    """Swap temporary table with target table using Snowflake's SWAP command."""
    hook = get_snowflake_hook()
    
    swap_sql = f"ALTER TABLE {schema}.{temp_table} SWAP WITH {schema}.{target_table}"
    logger.info(f"Swapping tables: {schema}.{temp_table} <-> {schema}.{target_table}")
    hook.run(swap_sql)

def process_table(schema: str, table: str, sql: str, **context) -> None:
    """Process a single table through the complete ELT pipeline."""
    logger.info(f"Starting ELT process for {schema}.{table}")
    
    temp_table = create_temp_table(schema, table, sql, **context)
    validate_temp_table(schema, temp_table, **context)
    ensure_target_table_exists(schema, table, temp_table, **context)
    swap_tables(schema, temp_table, table, **context)
    
    logger.info(f"Successfully completed ELT process for {schema}.{table}")

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_slack_failure,
}

with DAG(
    dag_id='daily_snowflake_elt',
    default_args=default_args,
    description='Daily ELT process to build analytics tables in Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'elt', 'analytics'],
    max_active_runs=1,
) as dag:
    
    previous_task = None
    
    for config in TABLES:
        schema = config['schema']
        table = config['table']
        sql = config['sql']
        
        task_id = f"process_{schema.lower()}_{table.lower()}"
        
        current_task = PythonOperator(
            task_id=task_id,
            python_callable=process_table,
            op_kwargs={
                'schema': schema,
                'table': table,
                'sql': sql,
            },
        )
        
        if previous_task:
            previous_task >> current_task
        
        previous_task = current_task