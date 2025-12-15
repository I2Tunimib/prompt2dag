from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.schedules import IntervalSchedule
from datetime import timedelta
import snowflake.connector
import os

# Configuration
TABLES = [
    {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
    {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
    {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"}
]

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def create_temp_table(table_config):
    logger = get_run_logger()
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=table_config["schema"]
    )
    cursor = conn.cursor()
    temp_table_name = f"temp_{table_config['table']}"
    sql = f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS ({table_config['sql']})"
    logger.info(f"Creating temporary table: {temp_table_name}")
    cursor.execute(sql)
    cursor.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def validate_temp_table(table_config):
    logger = get_run_logger()
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=table_config["schema"]
    )
    cursor = conn.cursor()
    temp_table_name = f"temp_{table_config['table']}"
    sql = f"SELECT COUNT(*) FROM {temp_table_name}"
    logger.info(f"Validating temporary table: {temp_table_name}")
    cursor.execute(sql)
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    if count == 0:
        raise ValueError(f"Temporary table {temp_table_name} is empty")

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def ensure_target_table_exists(table_config):
    logger = get_run_logger()
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=table_config["schema"]
    )
    cursor = conn.cursor()
    target_table_name = table_config["table"]
    sql = f"CREATE TABLE IF NOT EXISTS {target_table_name} (LIKE {target_table_name})"
    logger.info(f"Ensuring target table exists: {target_table_name}")
    cursor.execute(sql)
    cursor.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def swap_tables(table_config):
    logger = get_run_logger()
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=table_config["schema"]
    )
    cursor = conn.cursor()
    temp_table_name = f"temp_{table_config['table']}"
    target_table_name = table_config["table"]
    sql = f"ALTER TABLE {temp_table_name} SWAP WITH {target_table_name}"
    logger.info(f"Swapping tables: {temp_table_name} with {target_table_name}")
    cursor.execute(sql)
    cursor.close()
    conn.close()

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def send_slack_notification(message):
    logger = get_run_logger()
    import requests
    logger.info(f"Sending Slack notification: {message}")
    response = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    response.raise_for_status()

@flow(name="Daily ELT Pipeline")
def daily_elt_pipeline():
    logger = get_run_logger()
    for i, table_config in enumerate(TABLES):
        try:
            create_temp_table(table_config)
            validate_temp_table(table_config)
            ensure_target_table_exists(table_config)
            swap_tables(table_config)
        except Exception as e:
            logger.error(f"Task failed for table {table_config['table']}: {e}")
            send_slack_notification(f"Task failed for table {table_config['table']}: {e}")
            break

if __name__ == "__main__":
    # Schedule configuration (optional)
    # schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime.now())
    daily_elt_pipeline()