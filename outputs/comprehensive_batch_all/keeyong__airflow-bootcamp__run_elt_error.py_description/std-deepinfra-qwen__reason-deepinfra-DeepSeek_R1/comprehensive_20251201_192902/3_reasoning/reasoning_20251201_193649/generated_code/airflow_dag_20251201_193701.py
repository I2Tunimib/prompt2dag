<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements.

First, the pipeline is a daily ELT process using Snowflake. The tasks are sequential for each table in the configuration. Each task does several Snowflake operations: CTAS to create a temp table, validate it has records, ensure the target exists, swap tables. Also, Slack notifications on failure, and tasks run one after another.

Constraints mention using Airflow 2.x imports, so I'll make sure to import DAG and necessary operators correctly. The DAG needs schedule_interval, catchup set to False, default_args with necessary parameters. Tasks should have explicit dependencies using >>.

Since there's no branching or sensors, I don't need BranchPythonOperator or sensors. Each table task is a PythonOperator that calls a function to handle the Snowflake steps. The functions will use Snowflake hooks. For Slack, use on_failure_callback in default_args.

The configuration has TABLES, so I'll define that as a list of dicts. Each task in the DAG will loop over TABLES, creating a sequence where each task depends on the previous.

I need to write the Python functions for the tasks. Each function will take the table config, execute SQL statements. For Snowflake, use SnowflakeHook. The steps are: create temp table with CTAS, validate count > 0, create target if not exists, swap tables.

Error handling: if any step fails, the task fails, triggering the Slack notification. The on_failure_callback should be a function that sends a Slack message using SlackWebhookHook.

Now, structuring the code:

1. Import necessary modules: DAG, PythonOperator, SlackWebhookHook, SnowflakeHook, etc.
2. Define default_args with owner, start_date, on_failure_callback.
3. Create the DAG with daily schedule, catchup=False.
4. Define the Slack notification function.
5. Define the function for processing a table, using SnowflakeHook to run SQL.
6. Loop over TABLES config to create tasks, each depending on the previous.

Wait, the TABLES configuration is part of the DAG, so I'll define it as a variable. Each task is generated dynamically. Since the tasks are sequential, each task after the first depends on the prior one.

Potential issues: Ensuring that the Snowflake connection is set up correctly in Airflow. The Slack webhook connection must also be configured. But the code should reference these via connection IDs.

Testing if the functions handle all SQL steps. For example, create_temp_table, validate_temp_table, ensure_target_exists, swap_tables. Each of these would execute specific SQL commands.

Also, need to make sure that the Python functions are properly structured, with parameters passed via op_kwargs in PythonOperator.

Wait, the task functions will need to access the table configuration (schema, table_name, sql). So when creating each PythonOperator, pass the specific table config as op_kwargs.

So, for each table in TABLES, create a PythonOperator task with the function process_table, and pass the table's parameters. Then set dependencies so each task runs after the previous.

Putting it all together:

- Define the DAG.
- Define the on_failure function for Slack.
- Define process_table function with steps.
- Generate tasks in a loop, linking them sequentially.

Let me check for PEP8 compliance, proper imports, and correct use of Airflow 2.x style. Also, make sure that the Snowflake and Slack hooks are used correctly.

I think that's the plan. Now, code time.
</think>

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.notifications.slack import send_slack_notification

TABLES = [
    {
        "schema": "analytics",
        "table_name": "user_activity",
        "sql": "SELECT * FROM raw.user_activity WHERE date = '{{ ds }}'"
    },
    {
        "schema": "analytics",
        "table_name": "sales_summary",
        "sql": "SELECT product_id, SUM(amount) FROM raw.sales GROUP BY product_id"
    }
]

default_args = {
    "owner": "analytics",
    "start_date": datetime(2023, 1, 1),
    "on_failure_callback": lambda context: send_slack_notification(
        text=f"Task failed: {context.get('task_instance').task_id}",
        channel="#alerts",
        slack_conn_id="slack_default"
    )
}

def process_table(schema, table_name, sql, **context):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    temp_table = f"{table_name}_temp"
    full_temp_name = f"{schema}.{temp_table}"
    full_target_name = f"{schema}.{table_name}"

    try:
        # Create temporary table
        cursor.execute(f"CREATE TEMPORARY TABLE {full_temp_name} AS {sql}")
        
        # Validate temporary table
        cursor.execute(f"SELECT COUNT(*) FROM {full_temp_name}")
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"No records in temporary table {full_temp_name}")
        
        # Ensure target exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_target_name} 
            LIKE {full_temp_name}
        """)
        
        # Swap tables
        cursor.execute(f"ALTER TABLE {full_target_name} SWAP WITH {full_temp_name}")
        conn.commit()
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id="snowflake_elt_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["analytics"],
) as dag:
    
    previous_task = None
    for idx, table_config in enumerate(TABLES):
        task = PythonOperator(
            task_id=f"process_table_{table_config['table_name']}_{idx}",
            python_callable=process_table,
            op_kwargs={
                "schema": table_config["schema"],
                "table_name": table_config["table_name"],
                "sql": table_config["sql"]
            }
        )
        
        if previous_task:
            previous_task >> task
            
        previous_task = task