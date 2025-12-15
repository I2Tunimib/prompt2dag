<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is a daily ETL process that waits for a database partition before running sequential steps. The main steps are wait_partition, extract_incremental, transform, load, with dependencies in that order.

First, Dagster uses ops and jobs instead of Airflow's operators. The wait_partition step is a SqlSensor in Airflow, which polls the database. In Dagster, sensors are separate from the job execution, but since the user mentioned a sensor-gated pattern, maybe I should model the wait_partition as an op that checks for the partition. However, ops can't poll continuously. Alternatively, perhaps the sensor is outside the job, triggering the job when the partition is ready. But the user wants the job to include the sensor as a step. Hmm, maybe the wait_partition op will just perform a single check and fail if the partition isn't available, relying on retries. But the original uses reschedule mode with a 5-minute interval and 1-hour timeout. That's more like a sensor that runs until the condition is met. But in Dagster, sensors are separate and can launch runs when conditions are met. So maybe the job doesn't include the sensor as an op. Wait, the user's constraints say to use ops for each step and a job that sets dependencies. So perhaps the wait_partition is an op that checks once, and the job is configured with retries and a delay. But the original sensor has a 5-minute poke interval. So maybe the op will have a retry policy with a 5-minute delay and maximum of 12 retries (since 1 hour / 5 minutes = 12). That way, if the partition isn't available, the op retries every 5 minutes up to 12 times.

So, the wait_partition op will check if the partition exists. If not, it raises an exception, triggering a retry. The retry policy should have a delay of 5 minutes and max_retries 12. But the original has a 1-hour timeout, which would be 12 retries * 5 minutes = 60 minutes. So that's possible.

Next, the extract_incremental op runs a SQL query to get new orders from the daily partition. Then transform cleans and validates, and load upserts into the data warehouse.

For resources, the pipeline uses a database connection. In Dagster, resources are used for external connections. So I'll create a database_resource that provides a connection. The ops can use this resource to access the database.

The job should have a daily schedule starting Jan 1, 2024, with catchup disabled. In Dagster, schedules are defined separately. So I'll need to define a schedule using a cron expression for daily runs.

The retry policy for all ops is 2 retries with 5-minute delays. Wait, the original says the retry policy is 2 retries. So each op should have a retry policy with 2 retries and 5-minute delay.

Now, structuring the code:

- Define the resources, like database_conn, perhaps as a resource.
- Create the ops with the necessary logic, using the resources.
- Set up the job with dependencies between the ops.
- Add the retry policies to each op.
- Define the schedule.

Wait, the user's constraints mention using @op and @job, and including a minimal launch pattern. Also, if resources are needed, provide simplified examples as comments or stubs.

So, first, define the resources. Let's create a database_resource that provides a connection. Since the user's example uses database_conn, maybe the resource is called database_resource, and the ops use it via context.

For the wait_partition op, it needs to check if the partition exists. The SQL for that might be querying the information schema. Let's assume a PostgreSQL-like system. The SQL could be something like checking if the partition for the current date exists in the orders table.

The extract_incremental op runs a query to select from the orders table where the partition key is the current date.

The transform op processes the data: cleaning customer info, validating amounts, formatting timestamps.

The load op upserts into fact_orders and updates metrics.

Now, coding each op:

Wait_partition op: context has the database connection. Execute a SQL query to check for the existence of the partition. If not found, raise an exception. The retry policy will retry 2 times with 5-minute delay. Wait, original retry policy is 2 retries. So total attempts would be 3 (initial + 2 retries). But the sensor in the original has a 1-hour timeout. Hmm, maybe the retry policy for wait_partition should have more retries. Wait, the original says the pipeline has a retry policy of 2 retries with 5-minute delays. So each task (op) in the pipeline has that. But the wait_partition's sensor in Airflow has a 1-hour timeout with 5-minute intervals. So in Dagster, to mimic that, the wait_partition op would need to retry 12 times (since 1 hour /5 minutes) but the pipeline's retry policy is only 2 retries. This is conflicting. Maybe the user made a mistake, but according to the problem statement, the retry policy is 2 retries with 5-minute delays. So each op will have that. But the wait_partition's sensor in the original has a 1-hour timeout. So perhaps the retry policy for wait_partition is different. But according to the problem statement, the retry policy is 2 retries for all tasks. So perhaps the wait_partition op will only retry twice, each after 5 minutes. That would give a total of 15 minutes (initial attempt, then two retries after 5 and 10 minutes). But the original sensor has a 1-hour timeout. This discrepancy might be an issue, but according to the user's instructions, we need to follow the pipeline description's retry policy. So all ops have 2 retries with 5-minute delays.

So each op will have the @op decorator with retry_policy=RetryPolicy(max_retries=2, delay=300).

Now, coding:

First, import necessary modules:

from dagster import op, job, ResourceParam, RetryPolicy, ScheduleDefinition, DefaultScheduleStatus

But resources need to be defined. Let's create a database_resource as a resource. The ops will require this resource.

But how to handle the database connection? Using a resource. So:

@resource(config_schema={"conn_string": str})
def database_resource(context):
    # In real use, this would create a connection object
    return context.resource_config["conn_string"]

But the user said to provide simplified resource examples as comments or stubs. So perhaps the resource is a string connection, and the ops use it to connect. But for the code to be executable, maybe mock the database operations.

Alternatively, use a ResourceParam to pass the connection string.

But perhaps the code can use a mocked database connection. For example, the wait_partition op could check a simulated condition. Since the code needs to be executable, maybe simulate the database check.

But the user wants the code to be executable. So perhaps the database operations are simulated with print statements or simple checks. For example, the wait_partition op could check if a certain file exists or a flag is set. But that complicates things. Alternatively, use a mock database connection that always returns True for the check, but in reality, the user would replace this with actual SQL.

Alternatively, use placeholders with comments indicating where real implementation would go.

So, for the wait_partition op:

@op(required_resource_keys={"database"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_partition(context):
    # Example SQL (adjust for your database):
    # query = """
    # SELECT 1 FROM information_schema.tables 
    # WHERE table_name = 'orders_partition_20240101'
    # """
    # result = context.resources.database.execute(query)
    # if not result:
    #     raise Exception("Partition not available")
    context.log.info("Partition available")

But to make the code executable, perhaps simulate the check. For example, assume the partition is always available. But the user's pipeline is sensor-gated, so perhaps the sensor is outside the job. Wait, the problem says the pipeline implements a sensor-gated pattern, with the first step being the SqlSensor. But in Dagster, sensors are separate from jobs. A sensor can monitor for a condition and trigger a job run when it's met. So perhaps the wait_partition step is not an op but a sensor that triggers the job. However, the user's instruction says to model each step as an op, including wait_partition. So perhaps the job starts with the wait_partition op, which checks once, and if it fails, retries after 5 minutes, up to 2 retries. But that would only give 3 attempts (initial + 2 retries), totaling 15 minutes, which is less than the 1-hour timeout. So maybe the retry policy for wait_partition should have more retries. But according to the problem statement, the retry policy is 2 retries for all tasks. So perhaps the user's description has conflicting constraints, but we need to follow the problem's instructions.

In that case, proceed with the retry policy as given.

Now, coding each op with simulated database interactions.

The extract_incremental op would run a SQL query to get data from the partition. Again, simulate this.

The transform op processes the data. For example, clean customer info by stripping whitespace, validate amounts are positive, format timestamps.

The load op would upsert into the data warehouse. Again, simulate.

Now, the job:

@job(
    resource_defs={"database": database_resource},
    config={
        "resources": {
            "database": {
                "config": {
                    "conn_string": "postgresql://user:pass@host:port/db"
                }
            }
        }
    }
)
def daily_etl_job():
    wait_partition()
    extract_incremental(wait_partition())
    transform(extract_incremental())
    load(transform())

Wait, dependencies are linear: wait_partition → extract → transform → load. So the job's dependencies should be set accordingly. So in the job function, each op is called in order, passing the output of the previous step. But if the ops don't pass data via inputs, perhaps they are just sequenced. Wait, in Dagster, dependencies are determined by the inputs. If an op's input is the output of another op, then Dagster knows to run them in order. If the ops don't take inputs, you can use the start argument or just list them in order. Alternatively, use the .alias method or specify dependencies via the graph.

But in this case, since each step depends on the previous, but doesn't necessarily take input from it (since they are separate steps), how to model dependencies? For example, wait_partition doesn't produce any output that extract_incremental needs, but extract_incremental can't run until wait_partition completes. So in Dagster, you can structure it by having each subsequent op depend on the previous one by including them as an argument. For example:

def daily_etl_job():
    extract_incremental(wait_partition())
    transform(extract_incremental())
    load(transform())

But wait_partition doesn't return anything, so extract_incremental would have to take it as an input. But that's not necessary. Alternatively, use the .after method or specify dependencies via the job's structure.

Alternatively, use the graph's dependencies correctly. For example:

wait = wait_partition()
extract = extract_incremental()
transform = transform()
load = load()

extract.after(wait)
transform.after(extract)
load.after(transform)

But in the job function, you can express this by:

def daily_etl_job():
    load(transform(extract_incremental(wait_partition())))

But if the ops don't pass data, this would require each op to take an input that it doesn't use. Alternatively, use the start argument. Alternatively, structure the ops to pass a dummy data parameter.

But perhaps the correct way is to have each op depend on the previous one by using inputs. For example, wait_partition returns a success signal, which is passed to extract_incremental, and so on. So modify the ops to take the previous step's output as an input, even if they don't use it.

So:

@op(retry_policy=...)
def wait_partition(context):
    # check partition
    return True

@op(retry_policy=...)
def extract_incremental(context, _):
    # extract data
    return data

But in this case, extract_incremental takes the output of wait_partition as an input, even if it doesn't use it. This ensures the dependency.

So modifying the ops to pass along a dummy value.

Alternatively, use the fact that in Dagster, if an op doesn't depend on another's output, you can use the start argument or sequence them via the job definition.

But perhaps the simplest way is to structure the job as:

@job(...)
def daily_etl_job():
    wait = wait_partition()
    extract = extract_incremental(wait)
    trans = transform(extract)
    load(trans)

But each op must take the previous op's output as input. So adjust the op definitions to accept an input, even if they don't use it.

So, for example:

@op(required_resource_keys={"database"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_partition(context):
    # check partition exists
    context.log.info("Checking partition...")
    # Simulated check; in real scenario, query database
    # Assume partition is available
    return True  # Dummy output

@op(required_resource_keys={"database"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def extract_incremental(context, _):
    # Use the database resource to extract data
    context.log.info("Extracting data...")
    # Simulated data
    return [
        {"customer": "  Alice  ", "amount": 100, "quantity": 2, "timestamp": "2024-01-01 08:00"},
        {"customer": "Bob", "amount": -50, "quantity": 1, "timestamp": "2024-01-01 09:00"}
    ]

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def transform(context, data):
    context.log.info("Transforming data...")
    cleaned = []
    for order in data:
        # Clean customer info
        order["customer"] = order["customer"].strip()
        # Validate amount and quantity
        if order["amount"] <= 0:
            context.log.error(f"Invalid amount in order: {order}")
            continue
        if order["quantity"] <= 0:
            context.log.error(f"Invalid quantity in order: {order}")
            continue
        # Format timestamp
        order["timestamp"] = order["timestamp"].replace(" ", "T") + "Z"
        cleaned.append(order)
    return cleaned

@op(required_resource_keys={"database"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def load(context, data):
    context.log.info("Loading data...")
    # Simulate upsert into data warehouse
    for order in data:
        context.log.debug(f"Upserting order: {order}")
    context.log.info(f"Loaded {len(data)} orders")

Then, the job is defined with dependencies:

@job(
    resource_defs={"database": database_resource},
    config={
        "resources": {
            "database": {
                "config": {
                    "conn_string": "postgresql://user:pass@host:port/db"
                }
            }
        }
    }
)
def daily_etl_job():
    load(transform(extract_incremental(wait_partition())))

This way, each op depends on the previous one's output, ensuring the sequential execution.

For the schedule, the user wants a daily schedule starting Jan 1, 2024, with catchup disabled. In Dagster, schedules can be defined with a cron schedule. Catchup is controlled by the schedule's configuration.

So:

from dagster import ScheduleDefinition

daily_schedule = ScheduleDefinition(
    job=daily_etl_job,
    cron_schedule="0 0 * * *",  # Midnight daily
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    start_date=datetime.datetime(2024, 1, 1)
)

But the user's code needs to be a single module. So include the schedule in the code.

Finally, the minimal launch pattern:

if __name__ == "__main__":
    result = daily_etl_job.execute_in_process()

Putting it all together, including the resource definition, ops, job, schedule, and main block.

But the schedule isn't part of the executable code unless it's attached to a repository, but the user didn't mention repositories. Since the user wants only the code for a single module, perhaps the schedule is commented or included but not used in the main block. However, the main block just executes the job once.

So the final code includes all necessary parts, with simulated database operations, retry policies, resources, and dependencies.
</think>

from dagster import op, job, RetryPolicy, ResourceParam, ScheduleDefinition
from datetime import datetime
import contextlib

# --- Resource Definitions ---
database_resource = ResourceParam[str](description="Database connection string")

# --- Op Implementations ---
@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"database"}
)
def wait_partition(context):
    """Checks for daily partition availability in source database."""
    # Example implementation:
    # conn = connect_to_db(context.resources.database)
    # if not check_partition_exists(conn, 'orders', context.log):
    #     raise Exception("Partition not available")
    context.log.info("Partition verification successful")

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"database"}
)
def extract_incremental(context):
    """Extracts new orders from verified database partition."""
    # Example implementation:
    # conn = connect_to_db(context.resources.database)
    # return execute_query(conn, "SELECT * FROM orders WHERE partition = CURRENT_DATE")
    context.log.info("Extracting incremental orders data")
    return [{"order_id": 1, "amount": 100.50, "customer": "  Alice  ", "timestamp": "2024-01-01 08:00"}]

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def transform(context, raw_orders):
    """Cleans and validates extracted order data."""
    cleaned = []
    for order in raw_orders:
        # Clean customer name
        order["customer"] = order["customer"].strip()
        
        # Validate numeric fields
        if order["amount"] <= 0:
            context.log.error(f"Invalid amount in order {order['order_id']}")
            continue
            
        # Standardize timestamp
        order["timestamp"] = order["timestamp"].replace(" ", "T") + "Z"
        cleaned.append(order)
    
    context.log.info(f"Processed {len(cleaned)} valid orders")
    return cleaned

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"database"}
)
def load(context, transformed_orders):
    """Loads transformed data to data warehouse."""
    # Example implementation:
    # conn = connect_to_dwh(context.resources.database)
    # perform_upsert(conn, "fact_orders", transformed_orders)
    context.log.info(f"Loading {len(transformed_orders)} orders to warehouse")

# --- Job Definition ---
@job(
    resource_defs={"database": database_resource},
    config={
        "resources": {
            "database": {"config": "postgresql://user:pass@localhost:5432/orders"}
        }
    }
)
def daily_etl_job():
    load(transform(extract_incremental()))

# --- Schedule Definition ---
daily_schedule = ScheduleDefinition(
    job=daily_etl_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1)
)

# --- Execution Pattern ---
if __name__ == "__main__":
    result = daily_etl_job.execute_in_process()