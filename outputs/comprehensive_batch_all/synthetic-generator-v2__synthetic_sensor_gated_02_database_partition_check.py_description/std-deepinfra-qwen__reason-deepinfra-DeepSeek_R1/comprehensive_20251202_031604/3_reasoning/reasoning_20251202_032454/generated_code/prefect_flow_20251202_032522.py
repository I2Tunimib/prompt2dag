<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements. The pipeline is a daily ETL process that waits for a database partition before proceeding with sequential steps. 

First, the constraints mention using Prefect 2.x style with @flow and @task decorators. Each step from the description should be a task. The wait_partition step is a sensor that polls the database. In Prefect, sensors aren't a separate concept, so I'll need to implement this as a task that retries until the condition is met. But wait, Prefect 2 uses tasks with retries and maybe a loop, but the user specified using .submit() for parallel steps. However, in this case, the steps are sequential except for possible parallelism, which here there isn't any. The steps are linear: wait_partition → extract → transform → load.

The wait_partition task is a SqlSensor in Airflow, which polls the database. In Prefect, I can create a task that checks the partition and raises an exception if it's not ready. Then, using retries with a delay to simulate the sensor. The user mentioned reschedule mode with 5-minute intervals and 1-hour timeout. So, the task should retry every 5 minutes, up to 12 times (since 60 minutes / 5 = 12). So, setting retries=12 and retry_delay_seconds=300. But Prefect's retry mechanism is for failures, so maybe the task should return a boolean and the flow can wait, but that's not straightforward. Alternatively, the task can keep checking until the partition exists, using a loop inside the task with sleep. However, Prefect tasks are supposed to be deterministic in their execution. Hmm, perhaps using a while loop inside the task with a timeout. Let me think.

Alternatively, the task can raise an exception if the partition isn't found, and then retry with retries and retry_delay_seconds set to 12 retries every 5 minutes. That way, the task will keep retrying until the partition is available or until the timeout (1 hour) is reached. That seems feasible. So, the wait_partition task would execute a SQL check, and if the partition isn't there, raise an exception (like ValueError), triggering a retry. The retries would be 12 times with 300 seconds delay. But Prefect's retry mechanism is for task failures, so if the task fails (raises an exception), it will retry. So, the task can check the database, and if the partition isn't present, raise an exception. Each retry will wait 5 minutes. After 12 retries (total 1 hour), it will fail.

So, the wait_partition task would look something like this:

@task(retries=12, retry_delay_seconds=300)
def wait_partition():
    # execute query to check partition
    if not partition_exists:
        raise ValueError("Partition not ready")

But how to handle the database connection? The problem mentions a 'database_conn' connection. So, perhaps using a Prefect block to manage the connection. But the user's code should be executable, so maybe the tasks receive the connection as a parameter. Alternatively, the tasks can get the connection from a block. But to keep it simple, maybe the tasks accept a connection_id parameter. However, the user's code example might not include the actual database interaction logic, just placeholders. But the user said to avoid placeholders like <TODO>, so I need to include minimal, safe defaults or comments.

So, for each task, I'll write a function that has a docstring explaining what it does, and inside, a placeholder for the actual implementation but with a comment. For example, in wait_partition, execute a SQL query to check the information schema for the partition.

Next, the extract_incremental task runs a SQL query to get new orders from the daily partition. Then transform cleans and validates the data, and load upserts into the data warehouse.

All tasks are sequential. So, in the flow function, the order is wait_partition, then extract, then transform, then load. Each task is called in sequence, passing the output of the previous task as input where needed.

The schedule is daily, starting Jan 1, 2024, with catchup disabled. In Prefect, scheduling is handled via deployments. The user mentioned to include a comment about deployment/schedule configuration if schedules are mentioned. So, adding a comment at the end about the schedule.

Now, putting it all together:

- Import prefect modules: from prefect import flow, task
- Define each task with @task decorator, including retries for wait_partition.
- The flow function uses @flow decorator.
- The main guard calls the flow.

Wait, the wait_partition task needs to have retries with 5-minute intervals and 1-hour timeout. So retries=12 (since 12*5=60 minutes), retry_delay_seconds=300 (5 minutes).

The extract, transform, load tasks have retry policy of 2 retries with 5-minute delays. So adding retries=2 and retry_delay_seconds=300 to each of their @task decorators.

Now, code structure:

from prefect import flow, task
from datetime import datetime
import time  # For sleep in wait_partition if needed?

@task(retries=12, retry_delay_seconds=300)
def wait_partition():
    """
    Polls database information schema to check for daily partition availability.
    Raises exception if partition not found, triggering retries.
    """
    # Example implementation:
    # connection = get_database_connection()
    # query = "SELECT ... FROM information_schema.partitions WHERE ..."
    # result = execute_query(connection, query)
    # if not result:
    #     raise ValueError("Daily partition not available")
    pass  # Replace with actual implementation

@task(retries=2, retry_delay_seconds=300)
def extract_incremental():
    """
    Executes SQL query to extract new orders from the daily partition.
    """
    # query = "SELECT * FROM orders WHERE partition = current_date"
    # data = execute_query(connection, query)
    return []  # Simulated extracted data

@task(retries=2, retry_delay_seconds=300)
def transform(data):
    """
    Cleans customer info, validates order amounts, formats timestamps.
    """
    # cleaned_data = [clean(row) for row in data]
    # validated_data = [validate(row) for row in cleaned_data]
    return data  # Simulated transformed data

@task(retries=2, retry_delay_seconds=300)
def load(data):
    """
    Upserts data into fact_orders table and updates metrics.
    """
    # connection = get_data_warehouse_connection()
    # upsert(connection, 'fact_orders', data)
    pass

@flow
def daily_etl_orders_pipeline():
    wait_partition()
    extracted_data = extract_incremental()
    transformed_data = transform(extracted_data)
    load(transformed_data)

if __name__ == "__main__":
    # To configure schedule, use a deployment with:
    # prefect deploy --name daily_orders --interval 86400
    daily_etl_orders_pipeline()

Wait, but the SqlSensor in the original description uses reschedule mode. In Prefect, tasks are run in the same process by default, but if using .submit(), they run in a task runner. However, in this case, since all tasks are sequential, we don't need to use .submit(). The wait_partition task is a sensor that must complete before moving on. So the code as written calls each task in sequence.

But the original Airflow code uses SqlSensor which polls until the partition is available. In Prefect, the wait_partition task is set to retry 12 times with 5-minute intervals, which simulates the sensor's behavior. Each retry is a delay of 5 minutes. If after 12 retries (1 hour) the task still fails, the flow fails.

The extract_incremental task then runs after wait_partition completes. Then transform, then load.

The retry policies for each task are set with retries=2 and retry_delay_seconds=300 (5 minutes), as per the description.

The schedule is daily, starting Jan 1, 2024, catchup disabled. In Prefect, this would be handled via a deployment with a schedule. The comment mentions including a brief comment about deployment/schedule configuration, so adding a comment in the __main__ guard.

The code should be PEP 8 compliant, with docstrings for each task and flow.

I think that's all. Now, writing the code accordingly.
</think>

from prefect import flow, task
from datetime import datetime

@task(retries=12, retry_delay_seconds=300)
def wait_partition():
    """Poll database information schema for daily orders partition availability."""
    # Implementation would execute database query here
    # Raise exception if partition not detected to trigger retry
    # Example: check SELECT * FROM information_schema.partitions WHERE...
    # if not available: raise ValueError("Partition not ready")

@task(retries=2, retry_delay_seconds=300)
def extract_incremental():
    """Execute SQL query to fetch new orders from current date partition."""
    # Implementation would use database_conn to execute query
    # Return extracted data as list of records
    return [{"sample": "data"}]  # Simulated return

@task(retries=2, retry_delay_seconds=300)
def transform(data):
    """Clean, validate, and format extracted orders data."""
    # Implementation would include data processing steps
    return [{"transformed": "record"}]  # Simulated processed data

@task(retries=2, retry_delay_seconds=300)
def load(transformed_data):
    """Upsert records into data warehouse and update metrics."""
    # Implementation would connect to target system and load data

@flow
def daily_etl_orders_pipeline():
    """Main ETL flow with sensor-gated sequential execution."""
    wait_partition()
    raw_data = extract_incremental()
    processed_data = transform(raw_data)
    load(processed_data)

if __name__ == "__main__":
    # To configure daily schedule (starting 2024-01-01):
    # prefect deploy with --interval 86400 and --skip-if-running
    daily_etl_orders_pipeline()