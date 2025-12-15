from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake.database import SnowflakeConnector
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_slack_message

TABLES = [
    {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
    {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
    {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"}
]

@task
def create_temp_table(snowflake_connector: SnowflakeConnector, schema: str, table: str, sql: str):
    logger = get_run_logger()
    temp_table = f"{schema}.temp_{table}"
    query = f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} AS ({sql})"
    logger.info(f"Creating temporary table: {temp_table}")
    snowflake_connector.execute(query)
    return temp_table

@task
def validate_temp_table(snowflake_connector: SnowflakeConnector, temp_table: str):
    logger = get_run_logger()
    query = f"SELECT COUNT(*) FROM {temp_table}"
    result = snowflake_connector.fetch_one(query)
    if result[0] == 0:
        raise ValueError(f"Temporary table {temp_table} is empty")
    logger.info(f"Temporary table {temp_table} contains {result[0]} records")

@task
def ensure_target_table_exists(snowflake_connector: SnowflakeConnector, schema: str, table: str):
    logger = get_run_logger()
    target_table = f"{schema}.{table}"
    query = f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM {schema}.raw_table LIMIT 0"
    logger.info(f"Ensuring target table exists: {target_table}")
    snowflake_connector.execute(query)

@task
def swap_tables(snowflake_connector: SnowflakeConnector, schema: str, table: str, temp_table: str):
    logger = get_run_logger()
    target_table = f"{schema}.{table}"
    query = f"ALTER TABLE {temp_table} SWAP WITH {target_table}"
    logger.info(f"Swapping tables: {temp_table} with {target_table}")
    snowflake_connector.execute(query)

@task
def send_slack_notification(slack_webhook: SlackWebhook, message: str):
    logger = get_run_logger()
    logger.info(f"Sending Slack notification: {message}")
    send_slack_message(slack_webhook, message)

@flow
def elt_pipeline():
    logger = get_run_logger()
    snowflake_connector = SnowflakeConnector.load("snowflake-connector")
    slack_webhook = SlackWebhook.load("slack-webhook")

    for i, table_config in enumerate(TABLES):
        try:
            temp_table = create_temp_table(snowflake_connector, table_config["schema"], table_config["table"], table_config["sql"])
            validate_temp_table(snowflake_connector, temp_table)
            ensure_target_table_exists(snowflake_connector, table_config["schema"], table_config["table"])
            swap_tables(snowflake_connector, table_config["schema"], table_config["table"], temp_table)
        except Exception as e:
            error_message = f"Error processing table {table_config['table']}: {e}"
            logger.error(error_message)
            send_slack_notification(slack_webhook, error_message)
            break

if __name__ == "__main__":
    elt_pipeline()