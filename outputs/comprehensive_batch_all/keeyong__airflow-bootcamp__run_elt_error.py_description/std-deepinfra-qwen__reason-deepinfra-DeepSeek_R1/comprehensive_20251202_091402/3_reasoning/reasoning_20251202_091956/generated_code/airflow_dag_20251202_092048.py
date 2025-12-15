<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements.

First, the pipeline is a daily ELT process using Snowflake. The main steps involve creating temporary tables with CTAS, validating data, ensuring target tables exist, swapping them, and sending Slack notifications on failures. The tasks are sequential for each table in the configuration.

The constraints mention using Airflow 2.x style imports, so I'll make sure to import DAG and necessary operators correctly. The DAG should have a schedule_interval set to daily, catchup turned off, and default_args that include necessary parameters like retries and email settings. Also, Slack notifications on failure are required, so I'll need to set up a failure callback using SlackOperator or similar.

The pipeline steps are linear for each table in TABLES config. Each table task has several operations. Since each task is identical but for different tables, I can loop over the TABLES list and generate PythonOperators or SnowflakeOperators for each. But since the user mentioned using PythonOperator, maybe each task will call a function that handles all the steps for a table.

Wait, the user's example uses PythonOperator, so each task will execute a Python function that performs the Snowflake operations. But Snowflake operations typically use SnowflakeOperator, which is more efficient. However, the problem states to use PythonOperator, perhaps because the helper utilities for Snowflake are in Python functions. Hmm, maybe the user expects SnowflakeHook inside PythonOperator tasks.

The steps for each table are: create temp table with CTAS, validate it has at least one record, ensure target exists, swap. So each of these could be separate tasks, but the description says each task performs these operations in order. So each table's processing is a single task that does all four steps sequentially. Therefore, each table has one task, and these tasks depend on each other in a linear chain.

The TABLES configuration is a list of dictionaries with schema, table_name, and sql. So I'll define that as a variable in the DAG file.

For Slack integration on failure, the default_args should have an 'on_failure_callback' that points to a function sending a Slack message. I'll need to import SlackWebhookOperator or use the SlackNotifier. Alternatively, use the task's parameters to handle failures.

Wait, the failure callback is a DAG-level parameter. So in default_args, set 'on_failure_callback' to a function that sends a Slack message. The helper utilities for Slack are mentioned, so perhaps there's a function like send_slack_alert that needs to be defined.

Now, structuring the DAG:

1. Import necessary modules: DAG, PythonOperator, default_args, etc.
2. Define the TABLES configuration.
3. Define helper functions: execute_table_operations (the main function for each task), send_slack_alert (for failure notifications), and maybe Snowflake utility functions.
4. Set up default_args with owner, start_date, email settings, retries, and on_failure_callback.
5. Instantiate the DAG with schedule_interval='@daily', catchup=False, etc.
6. Create a list of tasks, each being a PythonOperator that calls execute_table_operations with the respective table config.
7. Set dependencies between tasks in a linear sequence (task1 >> task2 >> task3 ...).

Wait, but how to loop through the TABLES and create a task for each, then set dependencies between them sequentially. For example, if TABLES has three entries, create three tasks where each depends on the previous.

So, in code:

tasks = []
for i, table_config in enumerate(TABLES):
    task = PythonOperator(
        task_id=f'process_table_{table_config["table_name"]}',
        python_callable=execute_table_operations,
        op_kwargs={'table_config': table_config},
        dag=dag
    )
    tasks.append(task)

Then, set the dependencies: for each task in tasks, after the first, each depends on the previous. So, for tasks t1, t2, t3: t1 >> t2 >> t3.

In code, that can be done with a loop:

for i in range(1, len(tasks)):
    tasks[i-1] >> tasks[i]

Now, the execute_table_operations function needs to handle all Snowflake operations. So inside that function, using SnowflakeHook to get a connection, execute SQL statements.

The steps are:

1. Create temp table with CTAS using the provided SQL.
2. Validate temp table has at least one row.
3. Ensure target table exists (create empty if missing).
4. Swap temp and target tables.

So, the function would look something like:

def execute_table_operations(table_config, **context):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Create temp table
    temp_table_name = f"{table_config['table_name']}_temp"
    create_temp_sql = f"CREATE TEMPORARY TABLE {temp_table_name} AS {table_config['sql']}"
    cursor.execute(create_temp_sql)

    # Validate temp table has records
    validate_sql = f"SELECT COUNT(*) FROM {temp_table_name}"
    cursor.execute(validate_sql)
    count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError("Temp table has no records")

    # Ensure target exists (create if not exists)
    target_table = f"{table_config['schema']}.{table_config['table_name']}"
    create_target_sql = f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table_name}"
    cursor.execute(create_target_sql)

    # Swap tables
    swap_sql = f"ALTER TABLE {target_table} SWAP WITH {temp_table_name}"
    cursor.execute(swap_sql)

    conn.commit()
    cursor.close()
    conn.close()

But need to handle exceptions, maybe. Also, Snowflake's SWAP command requires that the target table exists, but the step before ensures it does. Also, creating the target table as a copy of the temp's structure.

Wait, the step says "Ensures the target table exists by creating an empty version if missing". So perhaps the target is created empty if not exists. The code above uses CREATE TABLE IF NOT EXISTS ... LIKE temp_table, which would create it with the same structure. But maybe the user wants an empty table. Alternatively, maybe the SQL for creating the target is different.

But given the description, the code seems plausible.

Now, the Slack notification on failure: the on_failure_callback in default_args should trigger a function that sends a message. So define a function send_slack_alert, which uses the SlackWebhookOperator or the Slack hook.

But using the SlackWebhookOperator in a callback might not be straightforward. Alternatively, use the Slack API directly or use the airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator's execute method.

Alternatively, the callback function can send a message using the Slack hook. For example:

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def send_slack_alert(context):
    slack_webhook_token = Variable.get("SLACK_WEBHOOK_TOKEN")
    alert_message = f"Task {context['task_instance'].task_id} failed."
    slack_operator = SlackWebhookOperator(
        task_id='slack_alert',
        webhook_token=slack_webhook_token,
        message=alert_message,
        channel='#alerts'
    )
    slack_operator.execute(context=context)

But this is a bit involved. Alternatively, use requests to post directly to the Slack webhook URL.

But perhaps the user expects a simple callback that uses the Slack connection. Let's proceed with the SlackWebhookOperator approach.

But in the code, the imports would need to include SlackWebhookOperator. However, the problem statement says to use minimal imports. Wait, the user's constraints mention "Integrations & Dependencies: Slack integration for failure notifications via callback". So the code must include the Slack callback.

Putting it all together:

Imports:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable

But wait, the user's example might not need all these. Also, the SlackWebhookOperator is used in the callback function.

The default_args would include 'on_failure_callback': send_slack_alert.

But the SlackWebhookOperator requires a connection ID or a webhook token. So perhaps the webhook token is stored in an Airflow Variable or a Connection.

Assuming the Slack webhook is stored in a Variable named 'SLACK_WEBHOOK_TOKEN', the send_slack_alert function can retrieve it and send a message.

Now, putting all together:

The code would look like:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

TABLES = [
    {
        'schema': 'analytics',
        'table_name': 'user_activity',
        'sql': 'SELECT * FROM raw.user_activity_stream'
    },
    {
        'schema': 'analytics',
        'table_name': 'sales_summary',
        'sql': 'SELECT product_id, SUM(amount) FROM raw.sales GROUP BY product_id'
    }
]

def send_slack_alert(context):
    slack_token = Variable.get('SLACK_WEBHOOK_TOKEN')
    message = f':red_circle: Task Failed: {context.get("task_instance").task_id}'
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        webhook_token=slack_token,
        message=message,
        channel='#alerts'
    )
    alert.execute(context=context)

def execute_table_operations(table_config, **context):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    temp_table = f"{table_config['table_name']}_temp"
    full_temp_name = f"{table_config['schema']}.{temp_table}"
    create_temp_sql = f"CREATE TEMPORARY TABLE {full_temp_name} AS {table_config['sql']}"
    cursor.execute(create_temp_sql)

    count_sql = f"SELECT COUNT(*) FROM {full_temp_name}"
    cursor.execute(count_sql)
    if cursor.fetchone()[0] == 0:
        raise ValueError(f"No data in temporary table {full_temp_name}")

    target_table = f"{table_config['schema']}.{table_config['table_name']}"
    create_target_sql = f"CREATE TABLE IF NOT EXISTS {target_table} (dummy INT)"
    cursor.execute(create_target_sql)

    swap_sql = f"ALTER TABLE {target_table} SWAP WITH {full_temp_name}"
    cursor.execute(swap_sql)

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'analytics',
    'start_date': datetime(2023, 1, 1),
    'email': ['analytics@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_alert
}

with DAG(
    dag_id='snowflake_elt_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['analytics']
) as dag:

    previous_task = None
    for table in TABLES:
        task = PythonOperator(
            task_id=f'process_{table["table_name"]}',
            python_callable=execute_table_operations,
            op_kwargs={'table_config': table}
        )
        if previous_task:
            previous_task >> task
        previous_task = task

Wait, but the code above creates tasks in a loop and sets dependencies between them. That's correct.

But in the execute_table_operations function, when creating the target table, the example uses "(dummy INT)" as a placeholder. However, the pipeline description says "create an empty version if missing". The original step says "Ensures the target table exists by creating an empty version if missing". So perhaps the target table should be created with the same structure as the temp table but empty. However, in Snowflake, CREATE TABLE ... LIKE copies the structure. So maybe the code should be:

create_target_sql = f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {full_temp_name}"

But if the temp table is temporary, then after the session ends, it's gone. However, during the task execution, the temp table exists. So using LIKE temp_table would work. But perhaps the user intended to create an empty table with the same structure as the target. Alternatively, maybe the target table's structure is already known, and creating it as a dummy is sufficient. But according to the pipeline steps, the code should create an empty version of the target if missing. So using LIKE temp_table would create the same structure, which is better.

But in the code above, the example uses "(dummy INT)", which is incorrect. So I need to adjust that.

So changing the create_target_sql to:

create_target_sql = f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {full_temp_name}"

But wait, the temp table is temporary and exists in the same schema. So when creating the target table, using LIKE would reference the temp table's structure. That makes sense.

But in Snowflake, temporary tables are session-specific. So if the connection is closed, the temp table may not exist. However, within the same task's execution, the connection is maintained, so the temp table exists. Therefore, using LIKE temp_table is feasible.

So adjusting the code:

create_target_sql = f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {full_temp_name}"

This way, the target table has the same structure as the temp table. Then, swapping them would work correctly.

Another point: when creating the temp table, the code uses CREATE TEMPORARY TABLE ... AS SELECT. Then, validating the count. Then creating the target table if not exists. Then swapping.

But Snowflake's ALTER TABLE SWAP requires that both tables exist. So creating the target table if not exists is correct.

Now, testing for possible issues:

- The Snowflake connection ID is hard-coded as 'snowflake_conn'. The user should have this connection set up in Airflow.

- The Slack webhook token is retrieved from a Variable named 'SLACK_WEBHOOK_TOKEN', which should be set.

- The execute_table_operations function uses the SnowflakeHook correctly.

- Tasks are named process_{table_name}, which is clear.

- Dependencies are set correctly in a linear chain.

- The DAG is scheduled daily, no catchup.

- PEP 8 compliance: lines under 79 chars? Maybe some lines are longer, but the user can adjust.

Now, putting all together, the code should be as follows:
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLES = [
    {
        "schema": "analytics",
        "table_name": "user_activity",
        "sql": "SELECT * FROM raw.user_activity_stream"
    },
    {
        "schema": "analytics",
        "table_name": "sales_summary",
        "sql": "SELECT product_id, SUM(amount) FROM raw.sales GROUP BY product_id"
    }
]

def send_slack_alert(context):
    webhook_token = Variable.get("SLACK_WEBHOOK_TOKEN")
    alert = SlackWebhookOperator(
        task_id="slack_alert",
        webhook_token=webhook_token,
        message=f"Task failed: {context['task_instance'].task_id}",
        channel="#data-alerts"
    )
    alert.execute(context=context)

def execute_table_operations(table_config, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    temp_table = f"{table_config['table_name']}_temp"
    full_temp_name = f"{table_config['schema']}.{temp_table}"
    
    try:
        cursor.execute(f"CREATE TEMPORARY TABLE {full_temp_name} AS {table_config['sql']}")
        cursor.execute(f"SELECT COUNT(*) FROM {full_temp_name}")
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"Empty data in {full_temp_name}")
        
        target_table = f"{table_config['schema']}.{table_config['table_name']}"
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {full_temp_name}")
        cursor.execute(f"ALTER TABLE {target_table} SWAP WITH {full_temp_name}")
        conn.commit()
    finally:
        cursor.close()
        conn.close()

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_alert,
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG(
    dag_id="snowflake_elt_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["analytics"]
) as dag:
    
    previous_task = None
    for table in TABLES:
        current_task = PythonOperator(
            task_id=f"process_{table['table_name']}",
            python_callable=execute_table_operations,
            op_kwargs={"table_config": table}
        )
        
        if previous_task:
            previous_task >> current_task
        
        previous_task = current_task