from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.blocks.system import Secret
from prefect_snowflake.database import SnowflakeConnector
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_slack_message
import hashlib

# Configuration
TABLES = [
    {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
    {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
    {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"}
]

# Snowflake and Slack configurations
SNOWFLAKE_CONNECTOR = SnowflakeConnector.load("snowflake-connector")
SLACK_WEBHOOK = SlackWebhook.load("slack-webhook")

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def create_temp_table(table_config):
    logger = get_run_logger()
    logger.info(f"Creating temporary table for {table_config['table']}")
    query = f"CREATE OR REPLACE TEMPORARY TABLE {table_config['schema']}.{table_config['table']}_temp AS ({table_config['sql']})"
    SNOWFLAKE_CONNECTOR.run(query)
    logger.info(f"Temporary table created for {table_config['table']}")

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def validate_temp_table(table_config):
    logger = get_run_logger()
    logger.info(f"Validating temporary table for {table_config['table']}")
    query = f"SELECT COUNT(*) FROM {table_config['schema']}.{table_config['table']}_temp"
    result = SNOWFLAKE_CONNECTOR.run(query)
    if result[0][0] == 0:
        raise ValueError(f"Temporary table {table_config['table']}_temp is empty")
    logger.info(f"Temporary table validated for {table_config['table']}")

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def ensure_target_table_exists(table_config):
    logger = get_run_logger()
    logger.info(f"Ensuring target table exists for {table_config['table']}")
    query = f"CREATE TABLE IF NOT EXISTS {table_config['schema']}.{table_config['table']} AS SELECT * FROM {table_config['schema']}.{table_config['table']}_temp LIMIT 0"
    SNOWFLAKE_CONNECTOR.run(query)
    logger.info(f"Target table ensured for {table_config['table']}")

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def swap_tables(table_config):
    logger = get_run_logger()
    logger.info(f"Swapping tables for {table_config['table']}")
    query = f"ALTER TABLE {table_config['schema']}.{table_config['table']} SWAP WITH {table_config['schema']}.{table_config['table']}_temp"
    SNOWFLAKE_CONNECTOR.run(query)
    logger.info(f"Tables swapped for {table_config['table']}")

@task
def send_slack_notification(message):
    logger = get_run_logger()
    logger.info(f"Sending Slack notification: {message}")
    send_slack_message(webhook=SLACK_WEBHOOK, text=message)

@flow(name="Daily ELT Pipeline")
def daily_elt_pipeline():
    logger = get_run_logger()
    logger.info("Starting daily ELT pipeline")

    for i, table_config in enumerate(TABLES):
        try:
            create_temp_table(table_config)
            validate_temp_table(table_config)
            ensure_target_table_exists(table_config)
            swap_tables(table_config)
            if i < len(TABLES) - 1:
                # Ensure sequential execution
                pass
        except Exception as e:
            error_message = f"Error processing table {table_config['table']}: {str(e)}"
            logger.error(error_message)
            send_slack_notification(error_message)
            raise

if __name__ == "__main__":
    daily_elt_pipeline()