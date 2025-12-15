<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements.

First, the pipeline is a daily ELT process using Snowflake. The tasks are sequential for each table in the configuration. Each table task does several operations: create temp table with CTAS, validate it has records, ensure target exists, swap tables. Dependencies are linear, each task depends on the previous. Also, Slack notifications on failure, no catch-up, and daily schedule.

So, the DAG should have a daily schedule, catchup=False. Default args should include email on failure, maybe retries, and the Slack callback. But the user mentioned using Slack for failure notifications via callback, so I need to set the on_failure_callback in default_args.

Next, the tasks are sequential for each table. Each table's task is a single operator, but each step within the task (CTAS, validate, etc.) might be separate. Wait, the description says each task performs those operations in order. So each table's processing is a single task that does all four steps. But in Airflow, each step would typically be a separate task. However, the user says "each task performs these operations in order" and the tasks are linear. So maybe each table's processing is a single PythonOperator that calls a function which does all four steps. That makes sense because the steps are sequential and part of the same logical unit.

The configuration has TABLES, which is a list of dicts with schema, table_name, and sql. So I'll define that as a variable in the DAG file.

For Snowflake operations, I'll need a hook. The user mentioned helper utilities for Snowflake connection, so I'll assume that the Snowflake connection is set up in Airflow with a connection ID, say 'snowflake_conn'. The functions will use SnowflakeHook from the provider package. Wait, Airflow's SnowflakeHook is part of the apache-airflow-providers-snowflake package. So I need to import SnowflakeHook from airflow.providers.snowflake.hooks.snowflake.

Each task (for a table) will execute the four steps. Let's create a Python function that takes the table config and does all the steps. So for each table in TABLES, create a PythonOperator task that calls this function.

The function needs to:

1. Create temp table using CTAS.
2. Validate temp table has at least one record.
3. Ensure target exists (create empty if missing).
4. Swap temp with target.

I'll write a helper function, maybe process_table, which takes the table config and does these steps using SnowflakeHook.

For the Slack integration, the on_failure_callback should be a function that sends a message. The user mentioned helper utilities for Slack, so I'll assume there's a function send_slack_alert in a module that's accessible. But since the code needs to be self-contained, maybe I should include a placeholder for that. But the user said to avoid placeholders, so perhaps use the SlackWebhookOperator in the on_failure_callback. Wait, but the callback is a function that takes context. So maybe define a function that uses the Slack webhook. Alternatively, the user might have a utility function, but since we can't assume that, perhaps use the SlackWebhookOperator in the callback. However, the code needs to be executable. Let me check: the failure callback can be a function that sends a Slack message using the webhook. So I'll include a send_slack_notification function that uses the SlackWebhookHook. The default_args will have 'on_failure_callback': send_slack_notification.

But the user's pipeline description says "Slack integration for failure notifications via callback", so the callback is set in the DAG. So I need to define that function. Let's create a send_slack_notification function that uses the SlackWebhookHook. The connection ID for Slack would be 'slack_conn', and the function uses the webhook token from that connection.

Now, putting it all together:

- Import necessary modules: DAG, PythonOperator, SnowflakeHook, SlackWebhookHook, etc.
- Define default_args with owner, start_date, email, etc., and on_failure_callback.
- Define the DAG with schedule_interval='@daily', catchup=False.
- Define the TABLES configuration as a list of dicts.
- Create a function process_table that takes the table config and performs the four steps using SnowflakeHook.
- Create a function send_slack_notification that sends a message on failure.
- Generate a series of PythonOperator tasks, each for a table in TABLES, linked in sequence.

Wait, the tasks need to be created in a loop over TABLES, each task named like 'process_table_{table_name}', and each task depends on the previous. So for example, if TABLES has three entries, the tasks would be t1 >> t2 >> t3.

So in code:

for i, table_config in enumerate(TABLES):
    task = PythonOperator(
        task_id=f'process_{table_config["table_name"]}',
        python_callable=process_table,
        op_kwargs={'table_config': table_config},
        dag=dag,
    )
    if i > 0:
        previous_task >> task
    previous_task = task

But how to handle the loop? Maybe collect all tasks in a list and then set the dependencies.

Alternatively, create a list of tasks and then chain them with >> between each consecutive pair.

Yes, that's better. So:

tasks = []
for table_config in TABLES:
    task = PythonOperator(...)
    tasks.append(task)

for i in range(1, len(tasks)):
    tasks[i-1] >> tasks[i]

That way, each task depends on the previous.

Now, the process_table function:

def process_table(table_config, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    schema = table_config['schema']
    table_name = table_config['table_name']
    sql = table_config['sql']
    temp_table = f"{table_name}_temp"
    target_table = table_config['table_name']

    # Create temp table with CTAS
    create_temp_sql = f"CREATE TEMPORARY TABLE {temp_table} AS {sql}"
    hook.run(create_temp_sql)

    # Validate temp table has at least one record
    validate_sql = f"SELECT COUNT(*) FROM {temp_table}"
    count = hook.get_first(validate_sql)[0]
    if count < 1:
        raise ValueError(f"No records in temporary table {temp_table}")

    # Ensure target exists (create empty if missing)
    create_target_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (LIKE {temp_table})
    """
    hook.run(create_target_sql)

    # Swap temp and target
    swap_sql = f"ALTER TABLE {target_table} SWAP WITH {temp_table}"
    hook.run(swap_sql)

But wait, Snowflake's CREATE TABLE ... LIKE requires the source table to exist. Since temp_table was created via CTAS, it exists. So creating the target table as LIKE temp_table if it doesn't exist.

But maybe the target table's schema should be predefined. However, according to the pipeline description, the step is to create an empty version if missing. So using CREATE TABLE IF NOT EXISTS with the same structure as the temp table.

Now, the Slack notification on failure. The send_slack_notification function:

def send_slack_notification(context):
    slack_conn_id = 'slack_conn'
    hook = SlackWebhookHook(slack_conn_id)
    message = f":red_circle: Task failed: {context['task_instance'].task_id}"
    hook.send(text=message)

But SlackWebhookHook is from airflow.providers.slack.operators.slack_webhook import SlackWebhookHook. Wait, no: the correct import is from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook.

So, include that import.

In default_args, set 'on_failure_callback': send_slack_notification.

Putting it all together:

Imports:

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

Default args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_notification
}

Wait, but the send_slack_notification function is defined after default_args in the code. So need to define the function before default_args. So reorder the code.

Also, the DAG's start_date should be a static datetime, not dynamic. So start_date=datetime(2023, 1, 1).

Now, the TABLES configuration:

TABLES = [
    {
        'schema': 'analytics',
        'table_name': 'user_activity',
        'sql': 'SELECT * FROM raw.user_activity WHERE date = CURRENT_DATE()'
    },
    {
        'schema': 'analytics',
        'table_name': 'sales_summary',
        'sql': 'SELECT product_id, SUM(amount) FROM raw.sales GROUP BY product_id'
    }
]

But the user said to include minimal, safe defaults. So maybe two example tables.

Putting all together:

The code would look like:

import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def send_slack_notification(context):
    hook = SlackWebhookHook(slack_conn_id='slack_webhook_default')
    message = f"Task {context['task_instance'].task_id} failed."
    hook.send(text=message)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['admin@example.com'],
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'on_failure_callback': send_slack_notification,
}

TABLES = [
    {
        'schema': 'analytics',
        'table_name': 'user_activity',
        'sql': 'SELECT * FROM raw.user_activity'
    },
    {
        'schema': 'analytics',
        'table_name': 'sales_summary',
        'sql': 'SELECT product_id, SUM(amount) FROM raw.sales GROUP BY product_id'
    }
]


def process_table(table_config, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    schema = table_config['schema']
    table_name = table_config['table_name']
    sql = table_config['sql']
    temp_table = f"{table_name}_temp"
    qualified_temp = f"{schema}.{temp_table}"
    qualified_target = f"{schema}.{table_name}"

    # Create temporary table
    create_sql = f"CREATE TEMP TABLE {qualified_temp} AS {sql}"
    hook.run(create_sql)

    # Validate temporary table
    count_sql = f"SELECT COUNT(*) FROM {qualified_temp}"
    result = hook.get_first(count_sql)
    if not result or result[0] < 1:
        raise ValueError(f"No records in {qualified_temp}")

    # Create target if not exists
    create_target_sql = f"""
    CREATE TABLE IF NOT EXISTS {qualified_target} (LIKE {qualified_temp})
    """
    hook.run(create_target_sql)

    # Swap tables
    swap_sql = f"ALTER TABLE {qualified_target} SWAP WITH {qualified_temp}"
    hook.run(swap_sql)


with DAG(
    dag_id='snowflake_elt_pipeline',
    default_args=default_args,
    description='Daily ELT pipeline for Snowflake analytics tables',
    schedule_interval='@daily',
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=['elt', 'snowflake'],
) as dag:

    previous_task = None
    for table in TABLES:
        task_id = f"process_{table['table_name']}"
        task = PythonOperator(
            task_id=task_id,
            python_callable=process_table,
            op_kwargs={'table_config': table},
        )
        if previous_task:
            previous_task >> task
        previous_task = task

Wait, but in the code above, the TABLES are processed in a loop, creating tasks and setting dependencies. Each task is a PythonOperator that runs process_table with the table config.

Yes, that should work. Also, the Snowflake connection is assumed to be 'snowflake_default', and Slack connection 'slack_webhook_default', which are default connection IDs.

Testing this code: The functions are properly defined, the DAG is within the context manager, tasks are created sequentially.

I need to make sure that all imports are correct. The SlackWebhookHook is from airflow.providers.slack.hooks.slack_webhook, and SnowflakeHook from airflow.providers.snowflake.hooks.snowflake.

Also, the process_table function uses qualified table names with schema.

The send_slack_notification uses the SlackWebhookHook with the connection ID. The user may need to set up the Slack webhook connection in Airflow.

This code should be executable assuming the providers are installed (apache-airflow-providers-snowflake and apache-airflow-providers-slack), and the connections are configured.

So, the final code should look like this.
</think>

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def send_slack_notification(context):
    hook = SlackWebhookHook(slack_webhook_conn_id="slack_default")
    message = f"Task {context['task_instance'].task_id} failed."
    hook.send(text=message)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": ["data-team@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_notification,
}

TABLES = [
    {
        "schema": "analytics",
        "table_name": "user_activity",
        "sql": "SELECT * FROM raw.user_activity WHERE date = CURRENT_DATE()",
    },
    {
        "schema": "analytics",
        "table_name": "sales_summary",
        "sql": "SELECT product_id, SUM(amount) FROM raw.sales GROUP BY product_id",
    },
]


def process_table(table_config, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    schema = table_config["schema"]
    table_name = table_config["table_name"]
    temp_table = f"{table_name}_temp"
    qualified_temp = f"{schema}.{temp_table}"
    qualified_target = f"{schema}.{table_name}"

    # Create temporary table
    create_sql = f"CREATE TEMPORARY TABLE {qualified_temp} AS {table_config['sql']}"
    hook.run(create_sql)

    # Validate record count
    count = hook.get_first(f"SELECT COUNT(*) FROM {qualified_temp}")[0]
    if count < 1:
        raise ValueError(f"Empty temporary table {qualified_temp}")

    # Ensure target exists
    hook.run(f"CREATE TABLE IF NOT EXISTS {qualified_target} (LIKE {qualified_temp})")

    # Swap tables
    hook.run(f"ALTER TABLE {qualified_target} SWAP WITH {qualified_temp}")


with DAG(
    dag_id="snowflake_elt_pipeline",
    default_args=default_args,
    description="Daily Snowflake ELT pipeline with sequential table processing",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["snowflake", "analytics"],
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