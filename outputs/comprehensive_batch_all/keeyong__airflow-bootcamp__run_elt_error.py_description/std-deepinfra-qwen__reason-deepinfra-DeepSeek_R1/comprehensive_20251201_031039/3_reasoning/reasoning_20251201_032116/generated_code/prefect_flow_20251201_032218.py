<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x Python module. Let's start by understanding the requirements.

First, the pipeline is a daily ELT process using Snowflake. It processes tables sequentially in a linear chain. Each table task has several steps: create temp table with CTAS, validate data, ensure target exists, swap tables. Tasks must run one after another.

Constraints mention using Prefect 2.x with @flow and @task decorators. Each step in the pipeline should be a task, and the flow orchestrates them. Since the tasks are sequential, I don't need to use .submit() for parallelism here. But wait, the user said for parallel steps use .submit(), but here it's sequential. So each table task is a single task that does all the steps, and they are called in order.

The pipeline has a linear sequence where each table task depends on the previous. So, in the flow, I'll loop through the TABLES config and call each process_table task one after another.

I need to define the tasks. The main task is process_table, which does the four operations. But maybe break down into subtasks? The description says each task performs these operations in order. But the user wants @task functions for each step. Wait, the pipeline steps mention each task (for a table) performs these operations in order. So perhaps each table processing is a single task that internally does these steps, which could be subtasks.

Wait, the user's instruction says: "Define @task functions for each step". So each of the four operations (create temp, validate, ensure target, swap) should be separate tasks. Then, the process_table task (which is a task as well?) would call these subtasks in order. But the pipeline steps say each table task is a single task that does these steps. Hmm, maybe I need to structure it as a flow that calls multiple tasks per table.

Wait, the pipeline steps say: "The pipeline executes a linear sequence of identical tasks, one for each table". So each table is handled by a single task (process_table), but within that task, there are multiple steps. But according to Prefect best practices, each unit of work should be a task. So perhaps each of the four operations per table should be their own tasks, and the process_table is a subflow that orchestrates them. Or maybe each table's processing is a task that calls other tasks.

But the user's constraints say: "Define @task functions for each step, and a single @flow that orchestrates them". So each step in the table processing (create_temp, validate, etc.) should be a @task, and the main flow calls these in sequence for each table, with each table's tasks depending on the previous table's tasks.

Wait, the pipeline says each table task depends on the previous. So for example, process_table_A must complete before process_table_B starts. But each table's processing involves multiple steps. So perhaps for each table, we have a sequence of tasks (create_temp, validate, ensure_target, swap), and these must all complete before the next table's tasks start.

But the description says "each task performs these operations in order" — implying that each table is a single task that does all four steps. But according to Prefect, tasks should be the units of work. So maybe each of the four steps per table is a task, and the flow for a table is a subflow that runs them in order. Then, the main flow runs these subflows sequentially.

Alternatively, perhaps the process_table is a task that calls the four steps as functions, but not as Prefect tasks. But the user's constraints require each step to be a @task. So each of the four operations must be a @task.

So, for each table, the main flow would call create_temp_table_task, validate_temp_table_task, ensure_target_table_task, swap_tables_task, in sequence. And each of these is a @task. Then, for all tables, these sequences are called one after another.

But the pipeline steps mention that each table's processing is a single task. Hmm, maybe the user intended that each table is processed by a single task, which internally does the four steps. But according to Prefect best practices, each step should be a task. So perhaps the process_table is a subflow that calls the four tasks in order. But the user's constraints say "a single @flow that orchestrates them". Wait, the main flow is the one with @flow, and within it, it calls tasks. So perhaps the main flow loops through each table, and for each, it runs the four tasks in order, with each table's tasks waiting for the previous table's tasks to complete.

Yes, that makes sense. So the main flow is a @flow. Inside it, for each table in TABLES, it runs create_temp, validate, ensure_target, swap tasks in order. And each table's set of tasks runs after the previous table's tasks. So the dependencies are sequential between tables but the steps within a table are sequential as well.

But how to structure that. Let's see. For example:

@flow
def elt_flow():
    for table in TABLES:
        create_temp = create_temp_table_task(table)
        validate = validate_temp_table_task(create_temp)
        ensure = ensure_target_table_task(validate)
        swap = swap_tables_task(ensure)

But wait, in Prefect 2, tasks are called normally, and dependencies are inferred by passing their outputs. But if each task doesn't return anything, maybe we need to use wait_for. Alternatively, since each step depends on the previous, passing the output isn't necessary, but Prefect will track the order based on the function calls.

Wait, in Prefect, when you call a task inside a flow, the order is determined by the execution order in the code. So if you call task1(), then task2(), task2 will run after task1 completes. So for each table, the four tasks are called in order. Then, since the tables are processed in a loop, each iteration of the loop runs after the previous one, because the loop is sequential.

So the code would be:

for table_config in TABLES:
    temp_table = create_temp_table_task(table_config)
    validation_result = validate_temp_table_task(temp_table)
    ensure_target_result = ensure_target_table_task(validation_result)
    swap_result = swap_tables_task(ensure_target_result)

But each of these tasks is a @task, and they are called in sequence. Then, the next iteration of the loop (next table) starts only after the previous table's swap task is done.

That would achieve the sequential processing of tables, with each table's steps running in order.

Now, about the configurations. The user mentioned a TABLES configuration. So I need to define that as a list of dictionaries, perhaps.

For Snowflake connections, the user mentioned helper utilities. So perhaps there's a SnowflakeConnection class that handles getting a connection. But in the code, I need to represent that. Since the code must be executable, I'll create a minimal SnowflakeConnection utility with a context manager.

Similarly, Slack integration is needed for failure notifications. So perhaps a send_slack_message function, which is called on task failure. In Prefect, this can be done with task hooks, like on_failure. But the user's example shows using a callback. So maybe each task has an on_failure parameter that calls the send_slack_notification task.

Wait, Prefect allows you to set hooks for tasks. For example, using the on_failure keyword argument in the task decorator. But perhaps the user wants that whenever any task fails, a Slack notification is sent. So we can add a on_failure callback to each task.

Alternatively, the user might have a task that sends a Slack message when a task fails. So each task, upon failure, triggers the send_slack_alert task.

But how to implement that in Prefect. One way is to use the task's on_failure handler. So each task would have a @task(on_failure=...) that calls a send_slack_alert function.

But the code must be minimal. So perhaps each task is decorated with @task(on_failure=send_slack_alert), and send_slack_alert is a task that takes the task instance and the exception.

But in Prefect, the on_failure handler is a function that takes the task instance, the old_state, and the new_state. So perhaps the send_slack_alert is a function that takes these parameters and sends a message.

Alternatively, create a task that sends the Slack message, and in the on_failure handler, call that task.

But integrating that into the code requires some handling.

Alternatively, the user's code may have a utility function for Slack, and each task, upon failure, calls that function.

But for the purposes of writing the code, perhaps the send_slack_notification is a @task that is called when a failure occurs. However, Prefect's task on_failure handlers are called as functions, not as tasks. So perhaps the send_slack_notification is a normal function, not a task, which is called in the on_failure handler.

But the user's code must be executable. So I need to include minimal implementations of these utilities.

So, let's outline the code structure.

First, imports:

from prefect import flow, task
from typing import Dict, List
from contextlib import contextmanager
import logging

Then, configuration:

TABLES = [
    {
        "schema": "analytics",
        "table": "users",
        "sql": "SELECT * FROM raw.users"
    },
    # ... other tables
]

Then, helper utilities:

@contextmanager
def snowflake_connection():
    # Mock connection setup/teardown
    logging.info("Connecting to Snowflake")
    yield "snowflake_conn"
    logging.info("Closing Snowflake connection")

def send_slack_alert(context):
    # Mock Slack notification
    error_message = context.get("exception")
    logging.error(f"Slack alert: Task failed: {error_message}")

Then, the tasks:

@task(on_failure=send_slack_alert)
def create_temp_table(table_config: Dict):
    with snowflake_connection() as conn:
        # Execute CTAS
        logging.info(f"Creating temp table for {table_config['table']}")
        return "temp_table_name"

@task(on_failure=send_slack_alert)
def validate_temp_table(temp_table_name: str):
    with snowflake_connection() as conn:
        # Check row count
        logging.info(f"Validating {temp_table_name}")
        if not row_count > 0:
            raise ValueError("No rows in temp table")
        return True

@task(on_failure=send_slack_alert)
def ensure_target_table(table_config: Dict):
    with snowflake_connection() as conn:
        # Create target if not exists
        logging.info(f"Ensuring target exists for {table_config['table']}")
        return "target_table_name"

@task(on_failure=send_slack_alert)
def swap_tables(temp_table_name: str, table_config: Dict):
    with snowflake_connection() as conn:
        # ALTER TABLE SWAP
        logging.info(f"Swapping {temp_table_name} with target {table_config['table']}")

Then, the flow:

@flow
def elt_pipeline():
    for table_config in TABLES:
        temp = create_temp_table(table_config)
        validated = validate_temp_table(temp)
        target = ensure_target_table(table_config)
        swap = swap_tables(temp, table_config)

Wait, but ensure_target_table may need the table_config to know which table to create. Similarly, swap_tables needs the temp table name and the target name from the config.

But in the code above, ensure_target_table is called with table_config, which includes the schema and table name. So that's okay.

But the swap_tables task needs the temp table name (from create_temp_table) and the target table name (from the config). So perhaps the swap_tables task takes both the temp_table_name and the table_config.

Wait, in the code above, create_temp_table returns "temp_table_name", which is passed to swap_tables. The table_config is passed directly to swap_tables. So the swap_tables task can use the temp_table_name and the target table name from the config.

But in Snowflake, the SWAP command would require the target table name. So perhaps the swap_tables task uses the table_config's schema and table name to build the target table name, and swaps the temp table with that.

But in the code, the ensure_target_table task is supposed to create the target table if it doesn't exist. So maybe that task is responsible for ensuring the target exists, and returns the target table name. But in the current code, ensure_target_table returns "target_table_name", which is not used. So perhaps the swap_tables task should take the target_table_name from ensure_target_table.

Wait, let's re-examine the steps:

Each table task does:

1. Create temp table via CTAS.

2. Validate temp has at least one record.

3. Ensure target exists (create empty if missing).

4. Swap temp with target.

So, step 3's target is the same as the table_config's table name. So perhaps ensure_target_table doesn't need to return anything, as the target name is known from the config. But maybe the task is to check if the target exists and create it if not. So the swap step uses the target table name from the config.

So, in the code:

The create_temp_table task returns the name of the temp table (maybe a generated name like "temp_<table>_<timestamp>").

The validate_temp_table task takes that temp table name and checks it.

The ensure_target_table task uses the table_config to ensure the target exists.

The swap_tables task takes the temp table name and the table_config to perform the swap.

So, in the flow:

temp = create_temp_table(table_config)
validated = validate_temp_table(temp)
ensure_target = ensure_target_table(table_config)
swap = swap_tables(temp, table_config)

But the order is important. The ensure_target must happen after validation, perhaps. Or maybe it can happen in parallel with other steps, but according to the pipeline steps, the steps are in order: create temp, validate, ensure target, swap. So ensure_target comes after validate.

So the flow for each table is:

create_temp -> validate -> ensure_target -> swap.

Wait, the pipeline steps for each task:

- Creates temp table

- Validates temp table

- Ensures target exists

- Swaps temp with target.

So the order is create, validate, ensure, swap. So ensure_target comes after validate. So in code:

temp = create_temp_table(table_config)
validated = validate_temp_table(temp)
ensure = ensure_target_table(table_config)
swap = swap_tables(temp, table_config)

But wait, ensure_target_table could be done before validate, but according to the steps, the order is create, validate, ensure, swap. So the code should call them in that order.

But why ensure after validate? Maybe because if validation fails, we don't want to create the target. So first validate, then ensure target exists, then swap.

So the code for each table is:

create_temp -> validate -> ensure_target -> swap.

But in the code above, the ensure_target is called after validate, which is correct.

Now, each of these tasks is a @task with on_failure set to send_slack_alert.

But the send_slack_alert function is defined as a function, not a task. So in the code, the on_failure parameter expects a callable. So the send_slack_alert function is a normal Python function, not a Prefect task. That's acceptable.

But the user mentioned "Slack integration for failure notifications via callback". So the send_slack_alert function would send a message to Slack. For the code to be executable, we can mock this function with a logging statement.

Now, the SnowflakeConnection is a context manager. The helper utilities include connection management, so the with snowflake_connection() as conn: is used in each task to get a connection.

Each task uses this context manager. But in reality, this would handle the connection setup and teardown. For the code example, it's mocked.

Putting it all together:

The code starts with imports, then the TABLES configuration.

Then the helper utilities: snowflake_connection as a context manager and send_slack_alert as a function.

Then the tasks, each decorated with @task(on_failure=send_slack_alert), which perform their respective operations using the Snowflake connection.

The flow elt_pipeline loops through each table in TABLES and runs the four tasks in sequence.

Finally, the if __name__ == '__main__' block calls elt_pipeline().

Regarding scheduling: the user mentioned daily execution. The code should include a comment about deployment/schedule configuration. So adding a comment like # Schedule can be configured via deployment with cron "0 0 * * *"

Testing the code for PEP 8 compliance: function names are lowercase_with_underscores, proper spacing, etc.

Potential issues:

- The validate_temp_table task checks if row_count > 0, but row_count is not defined. Since this is example code, perhaps replace with a placeholder. For example, execute a query to get the row count, but mock it.

In the validate_temp_table task:

with snowflake_connection() as conn:
    # Example query: f"SELECT COUNT(*) FROM {temp_table_name}"
    row_count = 1  # Mocked for example
    if row_count < 1:
        raise ValueError("No rows in temp table")
    return True

Similarly, other tasks have mocked operations.

So the final code would look like this.
</think>

from prefect import flow, task
from typing import Dict, List
from contextlib import contextmanager
import logging

TABLES = [
    {
        "schema": "analytics",
        "table": "users",
        "sql": "SELECT * FROM raw.users"
    },
    {
        "schema": "analytics",
        "table": "orders",
        "sql": "SELECT * FROM raw.orders"
    }
]

@contextmanager
def snowflake_connection():
    """Mock Snowflake connection context manager"""
    logging.info("Connecting to Snowflake")
    yield "snowflake_conn"
    logging.info("Closing Snowflake connection")

def send_slack_alert(context):
    """Mock Slack failure notification handler"""
    error = context.get("exception")
    logging.error(f"Slack alert: Task failed with error: {str(error)}")

@task(on_failure=send_slack_alert)
def create_temp_table(table_config: Dict):
    """Task to create temporary table using CTAS"""
    with snowflake_connection() as conn:
        logging.info(f"Creating temp table for {table_config['table']}")
        return f"temp_{table_config['table']}_{hash(table_config['sql'])}"

@task(on_failure=send_slack_alert)
def validate_temp_table(temp_table_name: str):
    """Task to validate temporary table has records"""
    with snowflake_connection() as conn:
        logging.info(f"Validating {temp_table_name}")
        # Mock row count validation
        if not 1:  # Replace with actual query in implementation
            raise ValueError(f"Empty temp table {temp_table_name}")
        return True

@task(on_failure=send_slack_alert)
def ensure_target_table(table_config: Dict):
    """Task to create target table if missing"""
    with snowflake_connection() as conn:
        logging.info(f"Ensuring target exists: {table_config['schema']}.{table_config['table']}")
        # Mock table creation logic
        return f"{table_config['schema']}.{table_config['table']}"

@task(on_failure=send_slack_alert)
def swap_tables(temp_table_name: str, table_config: Dict):
    """Task to swap temporary table with target"""
    with snowflake_connection() as conn:
        target_table = f"{table_config['schema']}.{table_config['table']}"
        logging.info(f"Swapping {temp_table_name} with {target_table}")
        # Mock swap operation

@flow
def elt_pipeline():
    """Main ELT workflow processing tables sequentially"""
    # Schedule: Configure via deployment with cron "0 0 * * *"
    for table_config in TABLES:
        temp_name = create_temp_table(table_config)
        validation = validate_temp_table(temp_name)
        target_name = ensure_target_table(table_config)
        swap_result = swap_tables(temp_name, table_config)

if __name__ == "__main__":
    elt_pipeline()