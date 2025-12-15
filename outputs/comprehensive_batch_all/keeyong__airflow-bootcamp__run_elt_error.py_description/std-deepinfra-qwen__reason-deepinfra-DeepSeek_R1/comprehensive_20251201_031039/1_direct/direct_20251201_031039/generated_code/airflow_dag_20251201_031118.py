from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import snowflake.connector

# Configuration
TABLES = [
    {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
    {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
    {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"}
]

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SLACK_WEBHOOK_CONN_ID = 'slack_webhook_conn'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_failure_notification
}

def send_slack_failure_notification(context):
    task_instance = context['task_instance']
    log_url = task_instance.log_url
    message = f"Task {task_instance.task_id} failed. Logs: {log_url}"
    SlackWebhookOperator(
        task_id='send_slack_failure_notification',
        http_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=message
    ).execute(context)

def create_temp_table(table_config):
    sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE {table_config['schema']}.{table_config['table']}_temp
    AS {table_config['sql']}
    """
    return sql

def validate_temp_table(table_config):
    sql = f"""
    SELECT COUNT(*) FROM {table_config['schema']}.{table_config['table']}_temp
    """
    return sql

def ensure_target_table_exists(table_config):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_config['schema']}.{table_config['table']} (LIKE {table_config['schema']}.{table_config['table']}_temp)
    """
    return sql

def swap_tables(table_config):
    sql = f"""
    ALTER TABLE {table_config['schema']}.{table_config['table']} SWAP WITH {table_config['schema']}.{table_config['table']}_temp
    """
    return sql

with DAG(
    dag_id='daily_elt_pipeline',
    schedule_interval='0 0 * * *',
    catchup=False,
    default_args=default_args
) as dag:

    previous_task = None

    for table in TABLES:
        create_temp_table_task = SnowflakeOperator(
            task_id=f'create_temp_table_{table["table"]}',
            sql=create_temp_table(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        validate_temp_table_task = SnowflakeOperator(
            task_id=f'validate_temp_table_{table["table"]}',
            sql=validate_temp_table(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        ensure_target_table_exists_task = SnowflakeOperator(
            task_id=f'ensure_target_table_exists_{table["table"]}',
            sql=ensure_target_table_exists(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        swap_tables_task = SnowflakeOperator(
            task_id=f'swap_tables_{table["table"]}',
            sql=swap_tables(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        if previous_task:
            previous_task >> create_temp_table_task

        create_temp_table_task >> validate_temp_table_task >> ensure_target_table_exists_task >> swap_tables_task
        previous_task = swap_tables_task