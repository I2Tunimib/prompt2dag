from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import snowflake.connector

# Configuration
TABLES = [
    {"schema": "public", "table": "orders", "sql": "SELECT * FROM raw_orders"},
    {"schema": "public", "table": "customers", "sql": "SELECT * FROM raw_customers"},
    {"schema": "public", "table": "products", "sql": "SELECT * FROM raw_products"}
]

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SLACK_WEBHOOK_CONN_ID = 'slack_webhook_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_temp_table(schema, table, sql):
    return f"""
    CREATE OR REPLACE TEMPORARY TABLE {schema}.{table}_temp AS
    {sql};
    """

def validate_temp_table(schema, table):
    return f"""
    SELECT COUNT(*) FROM {schema}.{table}_temp;
    """

def ensure_target_table_exists(schema, table):
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (LIKE {schema}.{table}_temp);
    """

def swap_tables(schema, table):
    return f"""
    ALTER TABLE {schema}.{table} SWAP WITH {schema}.{table}_temp;
    """

def send_slack_notification(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    log_url = context['task_instance'].log_url
    message = f"Task {task_id} failed. Check the logs: {log_url}"
    return SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=message,
        on_failure_callback=True
    ).execute(context)

with DAG(
    'daily_elt_pipeline',
    default_args=default_args,
    description='Daily ELT process for building analytics tables in Snowflake',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    previous_task = None

    for table_config in TABLES:
        schema = table_config['schema']
        table = table_config['table']
        sql = table_config['sql']

        create_temp_table_task = SnowflakeOperator(
            task_id=f'create_temp_table_{table}',
            sql=create_temp_table(schema, table, sql),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        validate_temp_table_task = SnowflakeOperator(
            task_id=f'validate_temp_table_{table}',
            sql=validate_temp_table(schema, table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        ensure_target_table_exists_task = SnowflakeOperator(
            task_id=f'ensure_target_table_exists_{table}',
            sql=ensure_target_table_exists(schema, table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        swap_tables_task = SnowflakeOperator(
            task_id=f'swap_tables_{table}',
            sql=swap_tables(schema, table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        create_temp_table_task >> validate_temp_table_task >> ensure_target_table_exists_task >> swap_tables_task

        if previous_task:
            previous_task >> create_temp_table_task

        previous_task = swap_tables_task

    # Slack notification on failure
    for task in dag.tasks:
        task.on_failure_callback = send_slack_notification