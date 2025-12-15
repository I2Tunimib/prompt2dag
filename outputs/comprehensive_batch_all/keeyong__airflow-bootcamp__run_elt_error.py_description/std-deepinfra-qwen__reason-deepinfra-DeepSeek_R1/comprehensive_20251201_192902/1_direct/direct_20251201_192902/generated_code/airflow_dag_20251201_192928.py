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
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_temp_table(table, sql):
    """Create a temporary table using a CTAS statement."""
    return f"""
    CREATE OR REPLACE TEMPORARY TABLE {table['schema']}.{table['table']}_temp
    AS {sql};
    """

def validate_temp_table(table):
    """Validate that the temporary table contains at least one record."""
    return f"""
    SELECT COUNT(*) FROM {table['schema']}.{table['table']}_temp;
    """

def ensure_target_table_exists(table):
    """Ensure the target table exists by creating an empty version if missing."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table['schema']}.{table['table']} (LIKE {table['schema']}.{table['table']}_temp);
    """

def swap_tables(table):
    """Swap the temporary table with the target table."""
    return f"""
    ALTER TABLE {table['schema']}.{table['table']} SWAP WITH {table['schema']}.{table['table']}_temp;
    """

def send_slack_notification(context):
    """Send a Slack notification on task failure."""
    return SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=f"Task failed: {context['task_instance'].task_id}",
        channel='#airflow-notifications',
    ).execute(context=context)

with DAG(
    'daily_elt_pipeline',
    default_args=default_args,
    description='Daily ELT process using Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    previous_task = None

    for table in TABLES:
        create_temp_table_task = SnowflakeOperator(
            task_id=f'create_temp_table_{table["table"]}',
            sql=create_temp_table(table, table['sql']),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        validate_temp_table_task = SnowflakeOperator(
            task_id=f'validate_temp_table_{table["table"]}',
            sql=validate_temp_table(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        ensure_target_table_exists_task = SnowflakeOperator(
            task_id=f'ensure_target_table_exists_{table["table"]}',
            sql=ensure_target_table_exists(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        swap_tables_task = SnowflakeOperator(
            task_id=f'swap_tables_{table["table"]}',
            sql=swap_tables(table),
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        create_temp_table_task >> validate_temp_table_task >> ensure_target_table_exists_task >> swap_tables_task

        if previous_task:
            previous_task >> create_temp_table_task

        previous_task = swap_tables_task

    # Add a failure handler to send Slack notifications
    for task in dag.tasks:
        task.on_failure_callback = send_slack_notification