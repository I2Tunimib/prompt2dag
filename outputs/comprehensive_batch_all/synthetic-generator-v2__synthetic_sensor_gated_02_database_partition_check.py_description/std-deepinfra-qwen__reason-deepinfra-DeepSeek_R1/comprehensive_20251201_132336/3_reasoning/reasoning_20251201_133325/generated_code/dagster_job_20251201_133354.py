<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is a daily ETL process that waits for a database partition before running sequential steps. 

First, the wait_partition step is a SqlSensor that checks for the daily partition. In Dagster, sensors are separate from jobs, but since the user mentioned a sensor-gated pattern, maybe I should model the sensor as part of the job. Wait, but sensors in Dagster typically trigger jobs. Hmm, perhaps the user wants the job to start only when the sensor detects the partition. But the job itself can't have a sensor inside it. So maybe the sensor is external, and the job is triggered by it. However, the code needs to represent the job, so the wait_partition might be an op that checks the database. But how to handle that?

Wait, the original pipeline uses a sensor (SqlSensor) which is a task in Airflow. In Dagster, sensors are used to trigger job runs based on external state. So perhaps the wait_partition op is simulating the sensor's functionality by polling the database. So the op would perform the check, and if the partition isn't available, it would fail, and retries would handle the waiting. But the user mentioned reschedule mode with a 5-minute interval and 1-hour timeout. Retries with delays might achieve that.

So for the wait_partition op, I can create a function that checks the database for the partition. If not found, raise a failure, and use retries with a delay. But Dagster ops can have retry policies. So set retries=2 (since the timeout is 1 hour, which is 12 intervals of 5 minutes, but the retry policy in the problem says 2 retries with 5-minute delays. Wait, the original retry policy is 2 retries with 5-minute delays between attempts. So total 3 attempts? Or 2 retries meaning 3 attempts total. So the retry policy in Dagster would be RetryPolicy(max_retries=2, delay=300).

But the wait_partition is a sensor that polls every 5 minutes for up to an hour. So 12 attempts. But the retry policy here allows only 2 retries. That's conflicting. Maybe the user intended the sensor to retry for an hour, but the retry policy in the problem is for the tasks. Wait, the problem's retry policy is specified under "Orchestration & Scheduling" as 2 retries with 5-minute delays. So all ops should have that retry policy.

But the wait_partition in the original is a sensor with a 1-hour timeout. So perhaps the op should have a retry policy that allows enough retries to cover 1 hour. But according to the problem's retry policy, it's 2 retries. Hmm, maybe there's a misunderstanding here. The problem says the sensor has a 1-hour timeout, but the retry policy for tasks is 2 retries. So perhaps the wait_partition op is allowed to retry 2 times with 5 minutes delay each, totaling 15 minutes. But that doesn't cover the 1-hour timeout. Maybe the problem's retry policy applies to all tasks, including the sensor. But that's conflicting. Maybe the user made a mistake, but I need to follow the problem's constraints.

So, for each op, apply the retry policy of 2 retries with 5-minute delays. The wait_partition op will have this retry policy. But how to model the 1-hour timeout? Maybe the total time would be initial attempt + 2 retries * 5 minutes = 10 minutes. That's not enough. But perhaps the problem's retry policy is separate from the sensor's timeout. Maybe the sensor's timeout is handled by the sensor's own configuration, but in Dagster, since we're modeling the sensor as an op, perhaps we need to implement the polling within the op.

Alternatively, maybe the wait_partition op is a sensor that's external to the job, and the job is only triggered when the sensor detects the partition. But the problem says to model the pipeline as a Dagster job with ops. So perhaps the wait_partition op is a regular op that checks the database, and if the partition isn't present, raises a failure, which would trigger a retry after 5 minutes. With 2 retries, that gives up to 3 attempts (initial + 2 retries), each 5 minutes apart, totaling 15 minutes. But the original sensor has a 1-hour timeout. So perhaps the retry policy here is insufficient. But according to the problem's constraints, the retry policy is 2 retries with 5-minute delays. So maybe the user expects that, even though it doesn't cover the 1-hour timeout. I'll proceed with the given retry policy.

Next, the extract_incremental op runs a SQL query to get new orders from the partition. Then transform does cleaning and validation. Then load upserts into the data warehouse.

For dependencies, all ops are sequential: wait_partition → extract → transform → load.

Now, resources: the ops need database connections. So I'll define a database resource, perhaps using a simple connection object. But in Dagster, resources are provided via resource definitions. So I'll create a resource for the database connection, maybe a mock class for the purposes of the code.

Each op that needs the database (wait_partition, extract_incremental, load) will require the resource.

The config might include the partition date. Since the job runs daily, the partition date is likely based on the run time. So perhaps the job is configured with a date, which defaults to the current date. The sensor would check for that date's partition.

But in the code, how to pass the partition date? Maybe the ops take a configured value. For example, the extract_incremental op uses the partition date in its SQL query.

So, the job might have a config schema that includes the partition_date. Then, each op can access it via context.op_config.

Putting this together:

- Define a resource for the database connection.
- Define the ops with the necessary parameters.
- Apply the retry policy to each op.
- Set up the job with the linear dependencies.

Now, coding:

Start by importing necessary modules: dagster (op, job, resource, RetryPolicy).

Create a mock database resource class, maybe with a method to check partition existence and execute queries.

Define the wait_partition op that uses the database resource to check for the partition. If not found, raises an exception. The retry policy allows retries with delay.

The extract_incremental op uses the database resource to run the SQL query for extraction.

The transform op processes the data.

The load op uses the database resource to upsert into the data warehouse.

The job is defined with these ops in order.

In the config, the partition_date is specified, perhaps using a default of today's date.

The main block runs the job in process.

But how to handle the sensor-gated pattern? In Dagster, sensors are separate and launch job runs when conditions are met. But the problem says the pipeline implements a sensor-gated pattern, so the job should only run when the sensor detects the partition. However, the code provided is for the job, not the sensor. So perhaps the wait_partition op is part of the job and performs the check. If the check fails, the op fails, and with retries, it will retry until the partition exists (but with limited retries according to the policy). But that might not align with the original intent of waiting up to an hour. However, given the problem's constraints, perhaps this is acceptable.

So, the code will have:

- A resource representing the database connection.
- Each op requiring the resource where necessary.
- Retry policies applied to all ops.
- Config for the partition date.

Testing for PEP 8 compliance, function names, etc.

Now, writing the code:

First, the resources. The problem mentions database_conn, so create a resource that provides a connection. For simplicity, use a class with methods needed.

Then, the ops:

@op(required_resource_keys={"database"}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_partition(context):
    # Check if partition exists
    partition_date = context.op_config["partition_date"]
    if not context.resources.database.check_partition(partition_date):
        raise Exception(f"Partition {partition_date} not available")
    context.log.info(f"Partition {partition_date} available")

Similarly, extract_incremental would run a query:

@op(required_resource_keys={"database"}, retry_policy=RetryPolicy(...))
def extract_incremental(context):
    partition_date = context.op_config["partition_date"]
    query = f"SELECT * FROM orders WHERE partition_date = '{partition_date}'"
    data = context.resources.database.execute_query(query)
    return data

Transform op processes data:

@op(retry_policy=...)
def transform(context, data):
    # clean, validate, format
    cleaned_data = [clean_record(rec) for rec in data]
    validated_data = [validate_rec(rec) for rec in cleaned_data]
    formatted_data = [format_timestamps(rec) for rec in validated_data]
    return formatted_data

Load op:

@op(required_resource_keys={"database"}, retry_policy=...)
def load(context, data):
    context.resources.database.upsert_fact_orders(data)
    context.resources.database.update_metrics()

The job:

@job(
    resource_defs={
        "database": database_resource
    },
    config={
        "ops": {
            "wait_partition": {
                "config": {"partition_date": "2024-01-01"}
            },
            "extract_incremental": {
                "config": {"partition_date": "2024-01-01"}
            }
        }
    }
)
def daily_etl_job():
    extract_data = extract_incremental(wait_partition())
    transformed_data = transform(extract_data)
    load(transformed_data)

Wait, but the config for the ops should probably be under a single top-level config. Maybe the job's config specifies the partition_date once, and all ops can access it. Alternatively, each op's config is set individually. But in Dagster, the job config can be structured so that each op's config is under ops.<op_name>.config.

In the example, the job's config has ops.wait_partition.config.partition_date and ops.extract_incremental.config.partition_date. But since they both need the same date, perhaps the job's config is set with a default partition_date, and each op references that. Alternatively, use a shared configuration.

But for simplicity, perhaps the job's config has a top-level partition_date, and each op accesses it via context.op_config["partition_date"]. However, in Dagster, each op's config is specified under ops.<op_name>.config. So if all ops need the same partition_date, the job config would have to set it for each op. Alternatively, use a resource to pass the partition date, but that's not typical. Alternatively, have a root config value and each op's config uses that. But that's not straightforward.

Alternatively, the job's config could have a 'partition_date' parameter, and each op's config is configured to take that value. For example:

config_schema = {
    "partition_date": str,
}

Then, in the job's config, the user provides partition_date, and each op's config is set to that value. But in Dagster, each op's config is separate. So perhaps the job's config is structured as:

{
    "ops": {
        "wait_partition": {
            "config": {
                "partition_date": "2024-01-01"
            }
        },
        "extract_incremental": {
            "config": {
                "partition_date": "2024-01-01"
            }
        }
    }
}

But that's redundant. To avoid repetition, maybe the job's config has a top-level partition_date, and each op's config references it using variables. But Dagster doesn't support that directly. So perhaps the user is expected to pass the same date to each op's config.

Alternatively, create a shared configuration using Dagster's configuration system. For example, define a structured config for the job that includes the partition_date, and each op accesses it via context.job_config. But Dagster's op config is per-op, not job-wide.

Hmm, this complicates things. For simplicity, perhaps each op's config includes the partition_date, and the job's config provides the same value to each. In the example code, the job's config is set with the partition_date for each op. But that's repetitive. Alternatively, use a default value in the op's config.

Wait, perhaps the ops can have a default partition_date, which is the current date, but the job's config can override it. For example:

@op(config_schema={"partition_date": str})
def wait_partition(context):
    date = context.op_config["partition_date"]

But the job's schedule runs daily, so the partition_date would be the logical date. In Dagster, when a schedule triggers a job, it can pass the partition date as a config parameter. So the schedule would handle passing the date.

But the problem says the schedule is daily, starting Jan 1, 2024, catchup disabled. So the code should include a schedule, but the user's constraints say to return a single module. However, the problem says to generate the job module, not necessarily the schedule. But the code should be executable, so perhaps the config has a default partition_date.

In the job's config, set a default partition_date. For example, in the @job decorator's config, set the default to "2024-01-01".

But in the code example, the job's config is specified with the ops' configs. So perhaps the job's config includes the partition_date for each op that needs it.

Alternatively, create a single configuration entry for the job and have each op access it. But Dagster doesn't support that directly. Each op's config is separate.

So, perhaps the code will have each op that needs the partition_date take it as config, and the job's config provides the same value to each. For example:

In the job config:

{
    "ops": {
        "wait_partition": {
            "config": {
                "partition_date": "2024-01-01"
            }
        },
        "extract_incremental": {
            "config": {
                "partition_date": "2024-01-01"
            }
        }
    }
}

But this is redundant. However, for the sake of the example, it's manageable.

Now, putting it all together:

Define the database resource as a mock class with methods check_partition, execute_query, upsert_fact_orders, update_metrics.

The ops wait_partition, extract_incremental, transform, load are defined with their respective resources and config.

The job daily_etl_job sets the dependencies and includes the resource definition.

The main block runs the job with execute_in_process.

So the final code would look like this:
</think>

from dagster import op, job, resource, RetryPolicy

class DatabaseConnection:
    def check_partition(self, partition_date):
        # Implement actual partition check logic
        return True  # Simplified for example
    
    def execute_query(self, query):
        # Implement actual query execution
        return ["order1", "order2"]  # Sample data
    
    def upsert_fact_orders(self, data):
        # Implement actual upsert logic
        pass
    
    def update_metrics(self):
        # Implement metrics update
        pass

@resource
def database_resource(context):
    return DatabaseConnection()

@op(
    required_resource_keys={"database"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    config_schema={"partition_date": str}
)
def wait_partition(context):
    partition_date = context.op_config["partition_date"]
    if not context.resources.database.check_partition(partition_date):
        raise Exception(f"Partition {partition_date} not available")
    context.log.info(f"Partition {partition_date} available")

@op(
    required_resource_keys={"database"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    config_schema={"partition_date": str}
)
def extract_incremental(context):
    partition_date = context.op_config["partition_date"]
    query = f"SELECT * FROM orders WHERE partition_date = '{partition_date}'"
    data = context.resources.database.execute_query(query)
    return data

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def transform(context, data):
    # Implement actual transformation logic
    context.log.info("Cleaning and validating data")
    return [f"transformed_{item}" for item in data]

@op(
    required_resource_keys={"database"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def load(context, data):
    context.resources.database.upsert_fact_orders(data)
    context.resources.database.update_metrics()
    context.log.info("Data loaded successfully")

@job(
    resource_defs={"database": database_resource},
    config={
        "ops": {
            "wait_partition": {"config": {"partition_date": "2024-01-01"}},
            "extract_incremental": {"config": {"partition_date": "2024-01-01"}}
        }
    }
)
def daily_etl_job():
    data = extract_incremental(wait_partition())
    transformed_data = transform(data)
    load(transformed_data)

if __name__ == "__main__":
    result = daily_etl_job.execute_in_process()