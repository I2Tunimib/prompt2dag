<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements.

First, the pipeline is a daily ELT process using Snowflake. It processes tables sequentially in a linear chain. Each table task has several steps: create temp table with CTAS, validate data, ensure target exists, swap tables. Tasks must run one after another.

Constraints mention using Prefect 2.x with @flow and @task decorators. For sequential steps, call tasks normally. Since the tasks for each table are sequential and each depends on the previous, I'll need to structure the flow so that each table's processing task runs after the previous completes.

The user provided a sample answer, but I should generate my own. Let me outline the steps.

First, import necessary modules: prefect's flow and task, maybe snowflake connector, and slack_sdk for notifications. Also, need a config for tables.

Each table processing step is a task. But since each table is processed sequentially, maybe each table's processing is a task that calls subtasks. Wait, the pipeline steps say each task (per table) performs operations in order. So perhaps each table has a task that orchestrates the steps for that table, and these tasks are sequential.

Wait, the pipeline steps mention that each task (for a table) does several operations. So maybe each table's processing is a single task that does all the steps, but that's not ideal because tasks should be units of work. Alternatively, each step (create temp, validate, ensure target, swap) could be subtasks, but the problem states that each table task depends on the previous. So perhaps the main task for each table calls these subtasks in order, and each main task is sequential.

But the user's example answer has a process_table task that calls other tasks. So maybe that's the way to go.

The configuration has a list of tables, each with schema, name, and sql. So I'll define a TABLES config as a list of dictionaries.

For Snowflake, need a helper function to get a connection. The example uses a context manager, so I'll include a get_snowflake_connection function.

Slack notifications on failure: use a task failure callback. So I'll create a send_slack_notification task and use it as the on_failure callback for each task.

Now, structuring the flow:

- The main flow is a @flow that iterates over each table in TABLES, and for each, calls process_table task sequentially.

Wait, but in Prefect, tasks are called within a flow. So the flow function would loop through the tables, and for each, call the process_table task, which itself calls the subtasks. But since each process_table must wait for the previous, they need to be called in sequence.

So in the flow, for each table in TABLES, we call process_table, and each call is awaited before the next. Since Prefect 2 is async-aware but by default runs synchronously, just calling the tasks in order in the flow should work.

Each process_table task is a @task which groups the steps for a single table. But wait, tasks can call other tasks. So process_table could be a task that calls create_temp_table, validate_table, etc., which are also tasks. However, in Prefect, if a task calls another task, those are considered subflows, but maybe it's better to have the main tasks as tasks, and their subtasks as functions or other tasks.

Alternatively, perhaps the process_table is a flow, but the problem states to have a single @flow. So the main flow is the @flow, and within it, for each table, it calls process_table, which is a @task that orchestrates the steps for that table.

But the steps for each table (create temp, validate, etc.) need to be sequential. So within the process_table task, those steps are called in order. However, tasks can't call other tasks directly; only flows can call tasks. Wait, no: tasks can call other tasks, but in Prefect 2, when you call a task from another task, it's treated as a sub-task. But the parent task would block until the subtask completes. So perhaps the process_table task is a task that calls other tasks in sequence.

But according to Prefect's documentation, tasks can call other tasks, but the called tasks are not tracked as separate in the Prefect UI. So maybe better to structure each step as a task, and the process_table task calls them in order.

Wait, but the user's example answer has process_table as a task that calls other functions (not tasks). That might not be correct. Because if the steps (create_temp_table, etc.) are not @task-decorated, then their execution isn't tracked by Prefect. So perhaps each of those steps should be @task as well.

But the pipeline description says each table processing task (the main one) performs those operations in order. So maybe each of those operations are functions within the process_table task, which is a single task. But that would mean that Prefect sees it as a single task, which might not be ideal for observability. Alternatively, each step is a @task, and the process_table task (which is a @task) calls them in order.

But in Prefect, a task cannot call another task directly. Only a flow can call tasks. So if process_table is a @task, it can't call other @task functions. Therefore, the steps (create_temp, validate, etc.) must be part of the same task, or the process_table must be a subflow.

Wait, no. Wait, in Prefect 2, you can have a task that calls another task, but the called task is not a Prefect task in that context. So to have each step as a Precept task, they need to be called from a flow, not from another task.

Therefore, the correct structure is:

- The main flow is a @flow function.

- The main flow iterates over each table in TABLES, and for each table, it calls a sequence of tasks (create_temp, validate, ensure_target, swap), in order.

But since each table's processing must happen sequentially after the previous table's processing, the main flow must process each table in sequence. So for each table in the list, the flow calls the four tasks in order, and each table's set of tasks depends on the previous table's tasks completing.

But that would require the flow to have nested loops, which might complicate things. Alternatively, group the four steps for a table into a single task, but that reduces visibility.

Alternatively, the process_table is a @task that does all four steps for a table, and the main flow calls this task for each table in sequence.

But if the steps are not Prefect tasks, their execution isn't tracked. So perhaps better to split each step into a @task, and the main flow orchestrates them.

Wait, but the pipeline says that each table's processing is a task that depends on the previous. So perhaps each table's processing is a single task, and these tasks are called sequentially.

But then, within that task, the four steps are executed as part of the same task. That would mean Prefect only tracks the process_table task, not the individual steps. If that's acceptable, then proceed.

But the user's example answer shows that process_table is a @task, which calls other helper functions (not tasks) like create_temp_table, which are regular functions. So Prefect won't track those as tasks.

Alternatively, perhaps each of the four steps should be @task, and the main flow calls them in order for each table.

But that would require the main flow to loop through each table, and for each, call the four tasks in order, each depending on the previous. But how to structure that.

Wait, let's think of the main flow as:

@flow
def elt_flow():
    for table_config in TABLES:
        create_temp_table_result = create_temp_table(table_config)
        validate_result = validate_table(create_temp_table_result)
        ensure_target_result = ensure_target_table(validate_result)
        swap_tables_result = swap_tables(ensure_target_result)

But each of these functions would be @task decorated. However, since each step for a table depends on the previous step, they are called in sequence. And each table's steps are processed after the previous table's steps.

But in this setup, all steps for all tables are part of the same flow, and each step is a task. However, the problem states that each table's processing task depends on the previous. So if each table's processing is a single task, then the flow would call process_table for table1, then process_table for table2, etc., each in sequence.

But if each process_table is a task that internally does the four steps (as regular function calls, not tasks), then Prefect only tracks the process_table tasks, not the individual steps. That's less detailed but simpler.

The user's example answer uses the latter approach: a process_table task that does all four steps, which are regular functions. So Prefect tracks the process_table tasks as individual tasks, each for a table, running sequentially.

So perhaps that's the way to go.

So, code structure:

- Import flow and task from prefect.

- Define a send_slack_notification task (on failure).

- Define a get_snowflake_connection helper function (not a task).

- Define the four steps as helper functions (create_temp_table, validate_table, ensure_target_table, swap_tables), which are called within the process_table task.

- The process_table task is a @task with on_failure callback set to send_slack_notification.

- The main flow elt_flow loops through each table in TABLES and calls process_table for each, in sequence.

Wait, but in Prefect, tasks called in a loop are executed sequentially by default. So in the flow:

for table in TABLES:
    process_table(table)

Each process_table task will run after the previous one completes.

So the code would look like:

from prefect import flow, task
from snowflake.connector import connect
from slack_sdk import WebClient
from typing import Dict, List

TABLES = [
    {"schema": "analytics", "name": "user_metrics", "sql": "SELECT ..."},
    # other tables...
]

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
SNOWFLAKE_CONFIG = {
    "account": "my-account",
    "user": "user",
    "password": "password",
    "warehouse": "compute_wh",
    "database": "analytics",
}

@task
def send_slack_notification(message: str):
    client = WebClient(token=SLACK_WEBHOOK_URL)
    client.chat_postMessage(channel="#data-ops", text=message)

def get_snowflake_connection():
    return connect(**SNOWFLAKE_CONFIG)

@task(on_failure=send_slack_notification)
def process_table(table_config: Dict):
    # Steps for processing a single table
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        # Create temp table
        temp_table_sql = f"CREATE TEMP TABLE {table_config['name']}_temp AS {table_config['sql']}"
        cursor.execute(temp_table_sql)
        
        # Validate temp table has records
        count_sql = f"SELECT COUNT(*) FROM {table_config['name']}_temp"
        cursor.execute(count_sql)
        count = cursor.fetchone()[0]
        if count == 0:
            raise ValueError("No records in temporary table")
        
        # Ensure target exists
        create_target_sql = f"CREATE TABLE IF NOT EXISTS {table_config['schema']}.{table_config['name']} (dummy INT)"
        cursor.execute(create_target_sql)
        
        # Swap tables
        swap_sql = f"ALTER TABLE {table_config['schema']}.{table_config['name']} SWAP WITH {table_config['name']}_temp"
        cursor.execute(swap_sql)
        
        cursor.close()

@flow
def elt_flow():
    for table_config in TABLES:
        process_table(table_config)

if __name__ == "__main__":
    elt_flow()

But wait, the process_table task is a @task, which when called in the loop, will be submitted as a task run. Each call to process_table in the loop will be executed sequentially.

But in the code above, the process_table task does all the steps as part of a single task. So Prefect will track each process_table task run for each table, but not the individual steps within. That's acceptable if the user doesn't need visibility into each step.

However, the problem statement says each task (per table) performs these operations in order. So perhaps the example is correct.

But the helper functions (create_temp_table, etc.) are not tasks, so their execution isn't tracked. If the user wants each step to be a task, then each step should be a @task, and the process_table task would call them. But since tasks can't call other tasks, that's not possible. Therefore, the steps must be part of the same task.

Alternatively, the main flow could orchestrate each step for each table. For example:

@flow
def elt_flow():
    for table_config in TABLES:
        temp_table = create_temp_table(table_config)
        validated = validate_table(temp_table)
        target_ensured = ensure_target_table(validated)
        swapped = swap_tables(target_ensured)

But each of these (create_temp_table, etc.) would be @task functions. This way, each step is a Prefect task, and they execute sequentially for each table. However, this would require the flow to have four tasks per table, and each table's tasks depend on the previous table's tasks.

But the problem states that each table task depends on the previous. So if each table's processing is a single task, then the flow is simpler. But if each step is a task, then for each table, four tasks are created, and each must run in sequence, and the next table's tasks must wait for the previous table's tasks.

This approach would provide more granularity in the Prefect UI but requires more tasks.

The user's example answer uses the first approach: a single task per table, handling all steps internally. So I'll follow that structure.

Now, checking the example answer's code:

They have a process_table task that uses a Snowflake connection and executes the steps. The send_slack_notification is a task used as on_failure callback. The main flow loops through TABLES and calls process_table for each, sequentially.

The code seems correct. Let me adjust for PEP8 and ensure all steps are included.

Wait, in the example answer, the process_table task is decorated with @task(on_failure=send_slack_notification). The send_slack_notification is a @task, but in Prefect, the on_failure callback should be a function that takes a task instance and an exception. Wait, no. The on_failure parameter expects a list of task state handlers. Wait, perhaps the example is incorrect here.

Wait, according to Prefect documentation, the on_failure parameter can be a list of functions that are called when the task fails. These functions should accept two arguments: the task instance, and the exception. However, the example answer's send_slack_notification task is a @task, which may not be appropriate here. Because the on_failure callback is not a Prefect task but a function that is called when the task fails.

So perhaps the send_slack_notification should be a regular function, not a @task. Because the on_failure expects a callback function, not a Prefect task.

Wait, no. The on_failure parameter can take a list of state handlers. Each state handler is a function that takes two arguments: the task instance and the old_state. But in the example, the send_slack_notification is a @task, which is not appropriate here. Because when you pass a task as a callback, it's not called as a Prefect task but as a regular function. So the @task decorator is unnecessary and incorrect here.

So the correct approach is to define send_slack_notification as a regular function, not a task. Because it's a callback function that sends a message when the task fails.

So the example answer's code may have an error here. Let me correct that.

So, the send_slack_notification should be a regular function, not a task. Because it's used as a callback, not as a Prefect task.

But the example answer's code defines it as a @task. That's incorrect. So in the code, the send_slack_notification should be a regular function.

Wait, but how to send a Slack notification as part of the on_failure callback. The on_failure is triggered when the task fails, and the callback function is called. If the callback needs to perform an action (like sending a message), it should do so within the function. But if that action is a Prefect task, then it needs to be called as a task. However, within the callback function, which is called during task state handling, you can't call another task because it's not in the context of a flow.

Therefore, the send_slack_notification should be a regular function that uses the Slack SDK directly, not a Prefect task.

So correcting that part:

def send_slack_notification(task, old_state):
    # This is a state handler function
    if old_state.is_failed():
        client = WebClient(token=SLACK_WEBHOOK_URL)
        message = f"Task {task.name} failed!"
        client.chat_postMessage(channel="#data-ops", text=message)

But integrating this into the task's on_failure requires using the state handler.

Alternatively, the example answer's approach may have been to use a task as a callback, but that's not the correct approach.

This is a bit tricky. Let me check Prefect's documentation.

According to Prefect 2's documentation, task state handlers can be functions that are called when the task's state changes. The on_failure parameter can be a list of these handlers. The handlers are called with the task instance and the previous state.

So the correct way to send a Slack notification on failure is to define a state handler function that sends the message. This function is not a Prefect task but a regular function.

Therefore, the example answer's code is incorrect in defining send_slack_notification as a @task. Instead, it should be a regular function.

So revising the code:

def send_slack_notification(task, state):
    if state.is_failed():
        client = WebClient(token=SLACK_TOKEN)
        client.chat_postMessage(
            channel="#data-ops",
            text=f"Task {task.name} failed with error: {state.result()}"
        )

But how to pass the necessary parameters (like SLACK_WEBHOOK_URL) to this function. Alternatively, use a closure or a configuration.

Alternatively, the example answer's code may have simplified this by making send_slack_notification a task, but that's not the right approach. Because tasks are meant to be units of work orchestrated by flows, not callback functions.

Therefore, the correct code would have the on_failure parameter set to a list containing the send_slack_notification function, which is a regular function, not a task.

But integrating this into the code requires adjusting the example.

So, putting it all together:

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from snowflake.connector import connect
from slack_sdk import WebClient
from typing import Dict, List

TABLES = [
    {"schema": "analytics", "name": "user_metrics", "sql": "SELECT ..."},
    # Add other table configurations
]

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
SNOWFLAKE_CONFIG = {
    "account": "my-account",
    "user": "user",
    "password": "password",
    "warehouse": "compute_wh",
    "database": "analytics",
}

def send_slack_notification(task, old_state):
    if old_state.is_failed():
        client = WebClient(token=SLACK_WEBHOOK_URL)
        message = f"Task {task.name} failed: {old_state.result()}"
        client.chat_postMessage(channel="#data-ops", text=message)

def get_snowflake_connection():
    return connect(**SNOWFLAKE_CONFIG)

@task(on_failure=[send_slack_notification])
def process_table(table_config: Dict):
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            # Create temporary table
            temp_table_name = f"{table_config['name']}_temp"
            cursor.execute(
                f"CREATE TEMPORARY TABLE {temp_table_name} AS {table_config['sql']}"
            )
            
            # Validate temporary table
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
            if cursor.fetchone()[0] == 0:
                raise ValueError("Temporary table has no records")
            
            # Ensure target exists
            target_table = f"{table_config['schema']}.{table_config['name']}"
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {target_table} (dummy INT)"
            )
            
            # Swap tables
            cursor.execute(
                f"ALTER TABLE {target_table} SWAP WITH {temp_table_name}"
            )
        finally:
            cursor.close()

@flow
def elt_flow():
    for table_config in TABLES:
        process_table(table_config)

if __name__ == "__main__":
    elt_flow()

But wait, the send_slack_notification function is now a state handler, not a task. The @task's on_failure parameter is set to a list containing this function.

This should work. However, the state handler function needs to accept two parameters: the task instance and the old state.

In this code, when a process_table task fails, the send_slack_notification function is called, which checks if the state is failed and sends a message.

This approach is correct.

But the original example answer's code used @task for send_slack_notification, which is incorrect. So the corrected code removes the @task decorator from send_slack_notification and defines it as a regular function.

Therefore, the final code should look like this.
</think>

from prefect import flow, task
from snowflake.connector import connect
from slack_sdk import WebClient
from typing import Dict, List

TABLES = [
    {"schema": "analytics", "name": "user_metrics", "sql": "SELECT ..."},
    # Add other table configurations
]

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
SNOWFLAKE_CONFIG = {
    "account": "my-account",
    "user": "user",
    "password": "password",
    "warehouse": "compute_wh",
    "database": "analytics",
}

def send_slack_notification(task, state):
    if state.is_failed():
        client = WebClient(token=SLACK_WEBHOOK_URL)
        message = f"Task {task.name} failed: {state.result()}"
        client.chat_postMessage(channel="#data-ops", text=message)

def get_snowflake_connection():
    return connect(**SNOWFLAKE_CONFIG)

@task(on_failure=[send_slack_notification])
def process_table(table_config: Dict):
    """Process a single table through CTAS pipeline steps"""
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            temp_table = f"{table_config['name']}_temp"
            # Create temporary table
            cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} AS {table_config['sql']}")
            
            # Validate data
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
            if cursor.fetchone()[0] == 0:
                raise ValueError("Empty temporary table created")
            
            # Ensure target exists
            target = f"{table_config['schema']}.{table_config['name']}"
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {target} (dummy INT)")
            
            # Swap tables
            cursor.execute(f"ALTER TABLE {target} SWAP WITH {temp_table}")
        finally:
            cursor.close()

@flow
def elt_flow():
    """Daily ELT pipeline for analytics tables"""
    for table_config in TABLES:
        process_table(table_config)

if __name__ == "__main__":
    # Deployment schedule would be configured separately
    elt_flow()