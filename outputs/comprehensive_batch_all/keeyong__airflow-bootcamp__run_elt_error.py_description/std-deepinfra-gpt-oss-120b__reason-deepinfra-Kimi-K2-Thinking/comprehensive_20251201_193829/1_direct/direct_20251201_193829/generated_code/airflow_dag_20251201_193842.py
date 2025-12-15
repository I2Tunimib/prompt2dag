"""Daily ELT pipeline for Snowflake tables with Slack failure notifications."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

# Configuration for tables to process
TABLES = [
    {
        "schema": "PUBLIC",
        "table_name": "sales",
        "sql": "SELECT * FROM raw.sales_raw",
    },
    {
        "schema": "PUBLIC",
        "table_name": "customers",
        "sql": "SELECT * FROM raw.customers_raw",
    },
    # Add additional table definitions as needed
]

# Slack connection id for failure alerts
SLACK_CONN_ID = "slack_webhook"


def slack_failure_alert(context: dict) -> None:
    """Send a Slack message when a task fails."""
    message = (
        f":red_circle: *Task Failed*\n"
        f"*DAG*: {context['dag'].dag_id}\n"
        f"*Task*: {context['task_instance'].task_id}\n"
        f"*Execution*: {context['execution_date']}"
    )
    hook = SlackWebhookHook(http_conn_id=SLACK_CONN_ID)
    hook.send(text=message)


def process_table(table_config: dict, **kwargs) -> None:
    """
    Process a single table:
    1. Create a temporary table using CTAS.
    2. Validate the temporary table contains at least one row.
    3. Ensure the target table exists.
    4. Swap the temporary table with the target table.
    """
    schema = table_config["schema"]
    table_name = table_config["table_name"]
    sql = table_config["sql"]
    execution_date = kwargs["execution_date"]

    temp_table = f"{schema}.{table_name}_temp_{execution_date.strftime('%Y%m%d%H%M%S')}"

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # 1. Create temporary table
    ctas_stmt = f"CREATE OR REPLACE TABLE {temp_table} AS {sql}"
    hook.run(ctas_stmt)

    # 2. Validate row count
    count_stmt = f"SELECT COUNT(*) FROM {temp_table}"
    records = hook.get_records(count_stmt)
    row_count = records[0][0] if records else 0
    if row_count == 0:
        raise AirflowException(f"Temporary table {temp_table} is empty.")

    # 3. Ensure target table exists (create empty version if missing)
    ensure_target_stmt = (
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} "
        f"LIKE {temp_table} "
        f"COMMENT = 'Created by ELT pipeline if missing.'"
    )
    hook.run(ensure_target_stmt)

    # 4. Swap temporary table with target table
    swap_stmt = f"ALTER TABLE {schema}.{table_name} SWAP WITH {temp_table}"
    hook.run(swap_stmt)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_alert,
}

with DAG(
    dag_id="snowflake_daily_elt",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["elt", "snowflake"],
) as dag:
    previous_task = None

    for table in TABLES:
        task = PythonOperator(
            task_id=f"process_{table['table_name']}",
            python_callable=process_table,
            op_kwargs={"table_config": table},
        )
        if previous_task:
            previous_task >> task
        previous_task = task