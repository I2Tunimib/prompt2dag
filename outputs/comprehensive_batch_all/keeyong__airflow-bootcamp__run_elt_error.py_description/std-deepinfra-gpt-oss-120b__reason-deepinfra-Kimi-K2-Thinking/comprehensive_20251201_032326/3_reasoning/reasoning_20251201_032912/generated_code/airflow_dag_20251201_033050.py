import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# Note: Requires 'snowflake_default' and 'slack_default' connections configured in Airflow

# Configuration for tables to process
TABLES = [
    {
        'schema': 'ANALYTICS',
        'table': 'DAILY_USER_METRICS',
        'sql': """
            SELECT 
                user_id, 
                COUNT(*) as event_count,
                CURRENT_DATE as processed_date
            FROM RAW.EVENTS 
            WHERE DATE(created_at) = CURRENT_DATE - 1 
            GROUP BY user_id
        """
    },
    {
        'schema': 'ANALYTICS',
        'table': 'DAILY_REVENUE',
        'sql': """
            SELECT 
                DATE(created_at) as dt, 
                SUM(amount) as revenue,
                COUNT(DISTINCT transaction_id) as transaction_count
            FROM RAW.TRANSACTIONS 
            WHERE DATE(created_at) = CURRENT_DATE - 1 
            GROUP BY DATE(created_at)
        """
    },
]


def send_slack_failure_notification(context):
    """
    Send failure notification to Slack via webhook.
    Requires 'slack_default' connection to be configured.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    
    message = (
        f":red_circle: *Task Failed*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Log URL:* {log_url}"
    )
    
    try:
        slack_op = SlackWebhookOperator(
            task_id='slack_failure',
            slack_webhook_conn_id='slack_default',
            message=message,
            channel='#data-alerts'
        )
        slack_op.execute(context=context)
    except Exception as e:
        logging.error(f"Failed to send Slack notification: {e}")


# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_failure_notification,
}


def process_table(schema, table, sql, **context):
    """
    Process a single table through the ELT pipeline:
    1. Create temp table via CTAS
    2. Validate temp table has records
    3. Ensure target table exists
    4. Swap temp table with target table
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    temp_table = f"{table}_TEMP"
    full_temp_table = f"{schema}.{temp_table}"
    full_target_table = f"{schema}.{table}"
    
    try:
        # Step 1: Create temp table with CTAS
        logging.info(f"Creating temp table: {full_temp_table}")
        create_sql = f"CREATE OR REPLACE TABLE {full_temp_table} AS {sql}"
        cursor.execute(create_sql)
        
        # Step 2: Validate temp table has at least one record
        logging.info(f"Validating temp table: {full_temp_table}")
        cursor.execute(f"SELECT COUNT(*) FROM {full_temp_table}")
        count = cursor.fetchone()[0]
        if count == 0:
            raise ValueError(f"Temp table {full_temp_table} contains no records")
        
        # Step 3: Ensure target table exists
        logging.info(f"Ensuring target table exists: {full_target_table}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_target_table} LIKE {full_temp_table}")
        
        # Step 4: Swap temp table with target table
        logging.info(f"Swapping temp table with target: {full_temp_table} <-> {full_target_table}")
        cursor.execute(f"ALTER TABLE {full_temp_table} SWAP WITH {full_target_table}")
        
        conn.commit()
        logging.info(f"Successfully processed table: {full_target_table}")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error processing table {full_target_table}: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


# DAG definition
with DAG(
    'daily_snowflake_elt_pipeline',
    default_args=default_args,
    description='Daily ELT pipeline for Snowflake analytics tables',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'elt', 'daily'],
) as dag:
    
    # Create tasks for each table in sequence
    previous_task = None
    
    for table_config in TABLES:
        # Generate task ID from schema and table names
        task_id = f"process_{table_config['schema'].lower()}_{table_config['table'].lower()}"
        
        current_task = PythonOperator(
            task_id=task_id,
            python_callable=process_table,
            op_kwargs={
                'schema': table_config['schema'],
                'table': table_config['table'],
                'sql': table_config['sql']
            },
        )
        
        # Set linear dependency: previous_task >> current_task
        if previous_task:
            previous_task >> current_task
        
        previous_task = current_task