<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x Python module. Let's start by understanding the requirements.

First, the pipeline is a daily ELT process using Snowflake. It processes tables sequentially in a linear chain. Each table task has several steps: create temp table with CTAS, validate data, ensure target exists, swap tables. Tasks must run one after another, no parallelism.

Constraints for Prefect: Use @task and @flow. Each step in the task should be a task? Wait, no. The description says each table processing is a task, and each of those tasks has operations. So maybe each table processing is a task, and within that task, the operations are handled. But Prefect tasks can't have subtasks, so perhaps each table task is a single @task that does all the steps, or maybe break down into smaller tasks. But the user said "Define @task functions for each step". Wait, the pipeline steps mention that each task performs operations in order. So maybe each of those operations (create temp, validate, etc.) are separate tasks, but the user's answer might structure each table processing as a task that calls other tasks. Hmm, but the original problem says "each task performs these operations in order", which suggests that each table's processing is a single task with multiple steps. But Prefect tasks are supposed to be units of work. So perhaps each table's processing is a task, and within that task, the four operations are performed. Alternatively, each operation could be a separate task, but the user's example might combine them into a single task per table.

Wait, the pipeline steps say: "The pipeline executes a linear sequence of identical tasks, one for each table". So each table is handled by a task. Each of these tasks does four operations. So each table's processing is a single task. Therefore, the code should have a @task for processing a single table, which includes the four steps. But how to handle that? Since the steps are sequential within the task, perhaps the task function will execute them in order.

But the user's constraints say: "Define @task functions for each step". Wait, maybe each of the four operations (create temp, validate, etc.) are separate tasks, and the processing of a table is a flow of these tasks. But the problem says that the pipeline is a linear sequence of tasks (one per table), each depending on the previous. So maybe each table's processing is a single task that internally does the four steps. But Prefect tasks are supposed to be the smallest unit, so perhaps each step should be a task. But the problem says "each task performs these operations in order", which suggests that each table's task is a single task that does all four steps. Hmm, conflicting interpretations.

Wait, the user's constraints say: "Define @task functions for each step, and a single @flow that orchestrates them in the described order." So each step in the pipeline's description should be a @task. But the pipeline steps mention that each table processing task has four operations. So perhaps each of those four operations are @tasks, and the processing of a table is a subflow? Or maybe the four operations are part of a single task. The user's example might clarify.

Looking at the sample answer, let's see. The user's answer has a process_table task, which calls other helper functions (maybe those are regular functions, not tasks). So perhaps the four steps are within a single task. Because if each step were a task, then the process_table task would have to call them, but in Prefect, tasks can't call other tasks directly; only flows can orchestrate tasks. So the process_table is a task that does all four steps, using regular functions (not Prefect tasks) for each step. That makes sense because the problem states that the table processing tasks are sequential, and each depends on the previous. So each table's processing is a single task, which internally does the four steps. The helper functions (like create_temp_table, validate_table, etc.) are regular Python functions, not Prefect tasks.

So the code structure would be:

- A flow function that iterates over the TABLES config, and for each table, calls the process_table task, ensuring each depends on the previous.

Each process_table task is a @task-decorated function that takes a table config and processes it by performing the four steps. The steps themselves are regular functions, not tasks, because they are part of the same unit of work.

Additionally, the flow needs to handle Slack notifications on task failures. So perhaps using Prefect's built-in mechanisms for task failure notifications, like state hooks or using the task's on_failure handler.

The code should include a configuration for TABLES, which is a list of dictionaries with schema, name, and sql.

For Snowflake connections, the helper functions (get_snowflake_conn) would handle that, perhaps using snowflake-connector.

The Slack integration is mentioned, so there should be a function to send Slack messages, and this is triggered when a task fails. In Prefect, this can be done by adding an on_failure hook to the task, or using a signal handler. Alternatively, the task can have a try-except block and send a Slack message on failure. The example answer uses a @task with a on_failure callback.

Looking at the sample answer provided earlier, the process_table task has a on_failure parameter set to handle_slack_notification. The handle_slack_notification is a @task that sends a message. So when a process_table task fails, it triggers the handle_slack_notification task.

But in Prefect, the on_failure handler is a function that is called when the task fails. However, to send a Slack notification, perhaps the handle_slack_notification is a task that needs to be submitted. But in Prefect 2, you can't directly call a task from within another task's on_failure handler because tasks are run by the flow. So maybe the on_failure is a flow-level hook, or the process_table task uses a try-except block and calls the Slack task upon exception.

Alternatively, using Prefect's notifications: but perhaps the example uses a try-except within the process_table task, and upon failure, calls the slack_notification_task.

But the sample answer provided earlier shows that the process_table task is decorated with @task(on_failure=handle_slack_notification), and handle_slack_notification is a @task. Wait, but in Prefect, the on_failure handler is a list of state handlers, which are functions that take the task, old_state, new_state. So perhaps the handle_slack_notification is a function, not a task. But the sample answer uses a task, which might not be correct. Alternatively, the handle_slack_notification is a regular function that calls the Slack API, not a Prefect task.

Hmm, perhaps the correct approach is to have the handle_slack_notification be a @task, and in the on_failure parameter of the process_table task, reference that task. But Prefect's on_failure expects a list of state handlers (functions), not tasks. So maybe the handle_slack_notification is a regular function that sends the Slack message, using the Slack SDK, and that function is called as the on_failure handler.

Alternatively, the sample answer may have the handle_slack_notification as a @task, but then how is it triggered? Because tasks can't be called directly from another task's on_failure handler. So perhaps the correct approach is to have the on_failure be a function that submits the Slack task. But that might not be straightforward.

Alternatively, the code may use a try-except block within the process_table task. If any exception occurs during the processing steps, the task catches it, sends a Slack notification via a helper function (not a task), and re-raises the exception. But then the Slack notification is part of the task's code, not a separate task.

But the problem states that Slack integration is for failure notifications via callback. So perhaps the on_failure is a callback that triggers the Slack notification. The sample answer uses @task(on_failure=handle_slack_notification), where handle_slack_notification is a @task. But Prefect's on_failure expects a function, not a task. So that might be incorrect. Therefore, perhaps the handle_slack_notification is a regular function that sends the message, and is called as the on_failure handler.

In any case, the code needs to include the necessary imports, the Snowflake connection handling, the Slack notification on failure, and the sequential processing of tables.

Putting it all together:

- The flow function loops through each table in TABLES, and for each, calls process_table task, ensuring each depends on the previous. Since tasks are called sequentially, each process_table task is called normally (not with .submit()), so they run in order.

- The process_table task is a @task that takes the table config, performs the four steps using helper functions (create_temp_table, validate_table, etc.), which interact with Snowflake.

- The helper functions for Snowflake (get_snowflake_conn) use snowflake-connector to get a connection.

- The Slack notification is handled via an on_failure callback that sends a message.

Now, considering all that, here's the code structure:

Imports: from prefect import flow, task; import snowflake.connector; from slack_sdk import WebClient; etc.

Config: TABLES = [...] with schema, name, sql.

Helper functions: get_snowflake_conn(), send_slack_alert().

Tasks: process_table decorated with @task(on_failure=send_slack_alert), which does the four steps.

Flow: elt_flow() that iterates over TABLES, calls process_table for each, ensuring sequential execution.

The code should also have if __name__ == '__main__': elt_flow().

But wait, the process_table task's on_failure is set to send_slack_alert, which is a regular function (not a task). Because Prefect's on_failure expects a function that is called when the task fails. So send_slack_alert would be a regular function that uses the Slack SDK to send a message.

So the code would look like:

from prefect import flow, task
import snowflake.connector
from slack_sdk import WebClient
from snowflake.connector.errors import ProgrammingError
import os
from typing import Dict, Any

TABLES = [
    {"schema": "analytics", "name": "user_metrics", "sql": "SELECT * FROM raw.users"},
    # ... other tables
]

def get_snowflake_conn():
    # returns a connection
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )

def send_slack_alert(task, old_state, new_state):
    if new_state.is_failed():
        client = WebClient(token=os.getenv("SLACK_TOKEN"))
        client.chat_postMessage(
            channel="#alerts",
            text=f"Task {task.name} failed!",
        )

@task(on_failure=[send_slack_alert])
def process_table(table_config: Dict[str, Any]):
    temp_table_name = f"{table_config['name']}_temp"
    
    # Step 1: Create temporary table
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"CREATE TEMPORARY TABLE {temp_table_name} AS {table_config['sql']}"
        )
        
        # Step 2: Validate temporary table has records
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
        count = cursor.fetchone()[0]
        if count == 0:
            raise ValueError("Temporary table has no records")
        
        # Step 3: Ensure target exists (create empty if missing)
        target_table = f"{table_config['schema']}.{table_config['name']}"
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM {temp_table_name} WHERE 1=0"
        )
        
        # Step 4: Swap tables
        cursor.execute(f"ALTER TABLE {target_table} SWAP WITH {temp_table_name}")
        conn.commit()
    except ProgrammingError as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

@flow
def elt_flow():
    previous = None
    for table in TABLES:
        current = process_table.submit(table, wait_for=[previous] if previous else [])
        previous = current

if __name__ == "__main__":
    elt_flow()

Wait, but in the flow, the code uses process_table.submit() with wait_for. But since the tasks need to run sequentially, each subsequent task should wait for the previous. However, using .submit() would create a future, and using wait_for=[previous] ensures that each task runs after the previous. But in Prefect, when you call .submit(), it's for asynchronous execution, but in this case, since we want sequential execution, perhaps we should call the task normally (without .submit()), which would run them in sequence. However, the problem says that for parallel steps, use .submit(), and for sequential, call normally. Since the tasks are sequential, the code should call them normally, not using .submit(). But the sample answer uses .submit() with wait_for. That might be incorrect.

Wait, the problem says: "For parallel steps, use .submit() to run tasks concurrently; for sequential steps, call tasks normally." So in this case, since the tasks are sequential, they should be called normally. So the flow would be:

previous_result = None
for table in TABLES:
    current_result = process_table(table, previous_result)
    previous_result = current_result

But since each process_table task doesn't take a dependency on the previous, except that they need to run in order. So in Prefect, tasks are executed in the order they are called unless there are dependencies. So in the flow, if you call process_table(table1), then process_table(table2), they will run sequentially. So the code can be:

@flow
def elt_flow():
    for table in TABLES:
        process_table(table)

But according to the problem description, each task depends on the completion of the previous. So even if the tasks don't pass data between them, the flow should enforce that each task runs after the previous. In Prefect, when you call a task in a flow, it runs in the order of the function calls. So the code above would run them sequentially. However, if the tasks are called in a loop, Prefect will schedule them sequentially by default. So the code can be:

@flow
def elt_flow():
    for table_config in TABLES:
        process_table(table_config)

But the problem says "each table task depends on completion of the previous table task". So perhaps each task is submitted, and each subsequent task waits for the previous. But using normal task calls in a loop would achieve that. Alternatively, using .submit() and wait_for. But according to Prefect's documentation, when you call a task normally in a flow, it blocks until the task is complete, so the next task call will start after. Therefore, the code can simply loop through the tables and call process_table normally, which ensures sequential execution.

But the sample answer provided earlier uses .submit() with wait_for. That might be unnecessary. However, the user's problem says that for sequential steps, call tasks normally. So the correct approach is to call them without .submit().

So the flow would be:

@flow
def elt_flow():
    for table_config in TABLES:
        process_table(table_config)

This ensures each process_table task runs sequentially.

But wait, the problem says "Tasks execute sequentially where each table processing task depends on completion of the previous table task". So each task must have a dependency on the previous. In Prefect, dependencies are created by passing the output of one task as the input to another. If the tasks don't have inputs, then Prefect may run them in parallel unless ordered by the flow's code. However, when called sequentially in a loop, Prefect will schedule them in order. So the code above is correct.

Putting it all together, the code would have:

- The process_table task decorated with @task, handling all four steps.

- The flow loops through TABLES and calls process_table for each.

- The Slack notification is handled via the on_failure callback.

- Snowflake helper functions handle the connection.

Now, considering all that, here's the code:
</think>

from prefect import flow, task
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from slack_sdk import WebClient
import os
from typing import Dict, List

TABLES = [
    {"schema": "analytics", "name": "user_metrics", "sql": "SELECT * FROM raw.users"},
    {"schema": "analytics", "name": "order_stats", "sql": "SELECT * FROM raw.orders"}
]

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE")
    )

def send_slack_notification(message: str):
    client = WebClient(token=os.getenv("SLACK_TOKEN"))
    client.chat_postMessage(
        channel="#data-alerts",
        text=message
    )

@task(on_failure=lambda task, state: send_slack_notification(f"Task {task.name} failed!"))
def process_table(table_config: Dict[str, str]):
    temp_table = f"{table_config['name']}_temp"
    target_table = f"{table_config['schema']}.{table_config['name']}"
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # Create temporary table
        cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} AS {table_config['sql']}")
        
        # Validate data exists
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
        if cursor.fetchone()[0] == 0:
            raise ValueError("No records in temporary table")
        
        # Ensure target exists
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}")
        
        # Swap tables
        cursor.execute(f"ALTER TABLE {target_table} SWAP WITH {temp_table}")
        conn.commit()
    except ProgrammingError as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

@flow
def snowflake_elt_pipeline():
    for table_config in TABLES:
        process_table(table_config)

if __name__ == "__main__":
    # Deployment schedule comment: Configure with daily schedule using cron
    snowflake_elt_pipeline()