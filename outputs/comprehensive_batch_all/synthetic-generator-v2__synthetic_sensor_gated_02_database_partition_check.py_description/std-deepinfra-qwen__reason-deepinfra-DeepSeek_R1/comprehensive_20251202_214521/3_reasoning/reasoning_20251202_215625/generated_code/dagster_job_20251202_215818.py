<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is a daily ETL process with a sensor-gated entry. The steps are wait_partition, extract_incremental, transform, load, in that order.

First, Dagster uses ops and jobs instead of Airflow's operators. The wait_partition is a SqlSensor in Airflow, but in Dagster, sensors are separate from the job. However, the user wants the job to be sensor-gated. Wait, but sensors in Dagster trigger runs, so maybe the wait_partition should be an op that checks for the partition. But the problem says to model each step as an op. So perhaps the first op is a sensor-like check. But how to handle that in Dagster?

Wait, the user's constraints mention using ops and a job with dependencies. So maybe the wait_partition is an op that checks for the partition's existence. But in Dagster, ops are part of a job's execution, so if the partition isn't available, the op would fail. But the original uses a sensor with reschedule mode. Hmm. Maybe the sensor is outside the job, but the user wants the job to start with the wait_partition op. So perhaps the wait_partition op will poll the database until the partition is available. But how to implement that with retries?

Alternatively, maybe the sensor is a separate Dagster sensor that triggers the job when the partition is available. But the user's instruction says to model each step as ops, including the sensor task. So perhaps the wait_partition op is designed to check for the partition and retry if it's not found. That way, the op can use Dagster's retry policy with a delay. Let me check the original's constraints: the wait_partition task uses reschedule mode with 5-minute poke interval and 1-hour timeout. So maybe in Dagster, we can model this with an op that raises a failure if the partition isn't found, and configure retries with a delay. But Dagster's retries are immediate by default. Alternatively, using a hook or a backoff library. But perhaps the simplest way is to have the op check once, and if not available, raise a failure, then rely on Dagster's retry mechanism with a delay. The original has a timeout of 1 hour, which would be 12 retries at 5 minutes each. So setting retries=12 and retry delay of 5 minutes. But the user's retry policy says 2 retries with 5-minute delays. Wait, the retry policy in the orchestration section applies to all tasks. Wait, the pipeline's retry policy is 2 retries with 5-minute delays. So perhaps each op should have a retry policy of 2 retries with 5-minute delay. But the wait_partition needs more retries? Hmm, maybe the original Airflow setup has the sensor retrying every 5 minutes for up to an hour (12 times), but the general retry policy for other tasks is 2 retries. So perhaps the wait_partition op needs a different retry policy. But the user's constraints say to model the dependencies as linear, with each step depending on the previous. So the job's structure is wait_partition → extract → transform → load.

So first, create the ops. Each op is a Python function decorated with @op. The wait_partition op needs to check the database for the partition. Let's assume a database connection is available via a resource. The user mentions resources/config, so perhaps we need to define a resource for the database connection. The original uses database_conn for both sensor and extraction. So in Dagster, we can create a resource that provides the database connection.

Then, the extract_incremental op runs a SQL query to get new orders from the daily partition. The transform op cleans and validates the data. The load op upserts into the data warehouse.

For the job, we need to set the dependencies. The ops are linear: wait_partition → extract → transform → load.

Now, considering the retry policy: each op should have a retry policy of 2 retries with 5-minute delay. In Dagster, you can set this with @op(retry_policy=...). So we need to define a retry policy with max_retries=2 and delay=300 seconds.

The sensor-gated entry: in Dagster, sensors can trigger job runs when certain conditions are met. But the user's pipeline uses a sensor (wait_partition) as the first step. However, in Dagster, sensors are separate from jobs. So perhaps the wait_partition op is part of the job, and the job is triggered by a sensor that starts it when the partition is available. But the user's code should model the wait_partition as an op. So maybe the job includes the wait_partition op, which checks for the partition and fails if not found, and the sensor is a separate Dagster sensor that triggers the job when the partition is available. But the user's code should not include the sensor, only the job. Wait, the user's code should return a single module with the job and ops. The pipeline description mentions that the wait_partition is a SqlSensor task. So perhaps in Dagster, the first op is a check that the partition exists. So the op will query the database and return success only if the partition exists. If not, it raises an exception, and the retry policy will retry it. But with the retry policy set to 2 retries, that's only 15 minutes total (assuming 5-minute delay between retries). But the original wait_partition has a 1-hour timeout. So maybe the wait_partition op needs a different retry policy. Wait, the original task has a 1-hour timeout with 5-minute intervals. That's 12 attempts. So perhaps the wait_partition op should have a retry policy with max_retries=11 (since initial attempt + 11 retries = 12 total) and delay of 5 minutes. But the general retry policy for the pipeline is 2 retries. Hmm, conflicting requirements. The user's pipeline has a retry policy of 2 retries for all tasks, but the wait_partition needs more. So perhaps the wait_partition op overrides the default retry policy. But how to handle that?

The user's constraints say: "Retry Policy: 2 retries with 5-minute delays between attempts". So all tasks have this retry policy. But the wait_partition's original setup has a 1-hour timeout with 5-minute intervals, which would require 12 retries. But according to the user's retry policy, it's 2 retries. So perhaps the user made a mistake, but we need to follow their specification. So all ops have retry_policy=RetryPolicy(max_retries=2, delay=300). But wait_partition would only retry twice, leading to a maximum of 3 attempts (original + 2 retries), totaling 15 minutes (5 minutes between each). But the original has a 1-hour timeout. This is conflicting. However, the user's instruction says to follow their constraints, so perhaps we proceed with 2 retries for all ops, including wait_partition. Maybe the pipeline's description has a mistake, but we follow the user's retry policy as specified.

So, each op will have the retry policy set with max_retries=2 and delay=300 seconds.

Now, code structure:

First, define the resources. The pipeline uses database_conn for the sensor and extraction. So in Dagster, we can define a resource called database_conn. Let's create a resource stub for it.

Then, define the ops with the retry policy.

The wait_partition op uses the database resource to check if the partition exists. For example, it could run a query against the information schema to see if the partition is present.

The extract_incremental op uses the database resource to execute the SQL query.

The transform op processes the data.

The load op writes to the data warehouse.

Now, code:

Start by importing necessary modules: from dagster import op, job, ResourceParam, RetryPolicy.

Define the retry policy as a variable since all ops use it.

retry_policy = RetryPolicy(max_retries=2, delay=300)

Then, define each op with the retry_policy parameter.

For the wait_partition op:

@op(retry_policy=retry_policy)
def wait_partition(context, database_conn):
    # Check if the daily partition exists
    partition_date = context.run_config.get('partition_date')
    query = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = %s
        AND partition_name = %s
    """
    # Assuming database_conn has a method execute_query that returns a result
    result = database_conn.execute_query(query, ('orders', f'orders_{partition_date}'))
    if not result:
        raise Exception("Partition not available")
    context.log.info("Partition detected")

But how to get the partition date? The original pipeline is scheduled daily starting Jan 1, 2024. So perhaps the partition date is based on the execution date. In Dagster, when a run is triggered, the partition can be passed via config. So the op would need to get the partition_date from the config.

So, the job would require a config schema that includes partition_date. So, in the job, we can define a config schema. Alternatively, the ops can access the run config via context.

Wait, the user's code should include minimal config examples. So perhaps the job uses a config to pass the partition date. So in the wait_partition op, the partition_date is retrieved from the run config.

But how to structure that? The op can access context.run_config. So in the code, the op would do:

partition_date = context.run_config.get('partition_date')

But the user might need to define a config schema for the job. Alternatively, use a Config schema.

Alternatively, the job can be configured with a partition_date. So when launching the job, the partition_date is provided via config.

So, in the main block, when executing the job, we pass in the config.

But for the code to be executable, perhaps the config is passed in when running the job. So in the __main__ block, the example config includes a partition_date.

But the user's code should be minimal. So perhaps the ops expect the partition_date to be in the config.

Now, the extract_incremental op would also need the partition_date to extract data for that day.

So, the code for extract_incremental:

@op(retry_policy=retry_policy)
def extract_incremental(context, database_conn):
    partition_date = context.run_config.get('partition_date')
    query = f"SELECT * FROM orders WHERE partition_date = '{partition_date}'"
    data = database_conn.execute_query(query)
    context.log.info(f"Extracted {len(data)} records")
    return data

But using f-strings for SQL queries is bad practice due to SQL injection, but for the sake of example, it's simplified.

The transform op takes the data from extract_incremental, processes it.

@op(retry_policy=retry_policy)
def transform(context, data):
    # Clean customer info, validate amounts, format timestamps
    cleaned_data = []
    for record in data:
        # Example cleaning steps
        record['customer_email'] = record['customer_email'].strip().lower()
        if record['amount'] < 0 or record['quantity'] <= 0:
            context.log.warning(f"Invalid record {record['order_id']}")
            continue
        record['timestamp'] = record['timestamp'].isoformat()
        cleaned_data.append(record)
    context.log.info(f"Transformed {len(cleaned_data)} valid records")
    return cleaned_data

The load op takes the transformed data and upserts into the data warehouse. Assume another resource for the data warehouse, but the original mentions target system uses a connection. Wait, the original says the target is a data warehouse with fact_orders. The extract uses database_conn (source), and load uses the target. So perhaps two resources: source_db and target_db. But the original says "Database Connection: Relies on database_conn connection for both sensor polling and data extraction". So the source and sensor use the same connection. The target is a different system. So perhaps the load op uses a different resource, like warehouse_conn.

But the user's code constraints say to provide simplified resource examples. So maybe the code has two resources: source_db and target_db. But the original mentions that the sensor and extract use database_conn. So perhaps the source_db is the database_conn, and the target_db is another resource.

But the user's code should include minimal stubs. So perhaps the resources are defined as part of the job. Let's see.

In Dagster, resources are provided to the job via a resource_defs parameter. So the job would have resources defined, like:

from dagster import resource

@resource
def source_db(_):
    # Stub for source database connection
    return {"execute_query": lambda query, params=None: []}  # dummy implementation

@resource
def target_db(_):
    # Stub for target data warehouse connection
    return {"upsert_data": lambda table, data: None}

But the original uses the same database_conn for sensor and extract. So perhaps the source_db is used by wait_partition and extract_incremental, and target_db is used by load.

So, the ops would have parameters for source_db and target_db as resources.

Wait, but in Dagster, resources are passed to ops via the resource parameter. Wait, no. Resources are available to ops via context.resources. Or, if the op requires a resource, it can be specified as a parameter. Wait, no. Resources are accessed through the context object. For example:

@op(required_resource_keys={'source_db'})
def wait_partition(context):
    context.resources.source_db.execute_query(...)

But perhaps the user's code uses ResourceParam, which is a way to inject resources as parameters. For example:

@op
def wait_partition(context, source_db: ResourceParam):
    source_db.execute_query(...)

So, the code would define the resources and then the ops require them as parameters.

So, in the code:

from dagster import job, op, ResourceParam, RetryPolicy

retry_policy = RetryPolicy(max_retries=2, delay=300)

@op(retry_policy=retry_policy)
def wait_partition(context, source_db: ResourceParam):
    partition_date = context.run_config.get('partition_date')
    # Example query to check partition existence
    query = """
        SELECT 1 FROM information_schema.partitions
        WHERE table_name = 'orders' AND partition_name = %s
    """
    partition_name = f"p{partition_date.replace('-', '')}"  # Example format
    result = source_db.execute_query(query, (partition_name,))
    if not result:
        raise Exception(f"Partition {partition_name} not available")
    context.log.info("Partition detected")

@op(retry_policy=retry_policy)
def extract_incremental(context, source_db: ResourceParam):
    partition_date = context.run_config.get('partition_date')
    query = f"SELECT * FROM orders WHERE partition_date = '{partition_date}'"
    data = source_db.execute_query(query)
    context.log.info(f"Extracted {len(data)} records")
    return data

@op(retry_policy=retry_policy)
def transform(context, data):
    # Processing logic here
    cleaned_data = []
    for record in data:
        # Clean customer info
        record['customer_email'] = record.get('customer_email', '').strip().lower()
        # Validate amount and quantity
        if record.get('amount', 0) < 0 or record.get('quantity', 0) <= 0:
            context.log.warning(f"Invalid record {record.get('order_id')}")
            continue
        # Format timestamp
        record['timestamp'] = record.get('timestamp').isoformat()
        cleaned_data.append(record)
    context.log.info(f"Transformed {len(cleaned_data)} records")
    return cleaned_data

@op(retry_policy=retry_policy)
def load(context, transformed_data, target_db: ResourceParam):
    # Upsert into fact_orders
    target_db.upsert_data('fact_orders', transformed_data)
    context.log.info(f"Loaded {len(transformed_data)} records into warehouse")

@job(
    resource_defs={
        'source_db': ResourceParam(dict(execute_query=lambda query, params=None: [])),
        'target_db': ResourceParam(dict(upsert_data=lambda table, data: None)),
    },
    config={
        'ops': {
            'wait_partition': {
                'config': {'partition_date': '2024-01-01'}  # Example default
            }
        }
    }
)
def daily_etl_job():
    load(transform(extract_incremental(wait_partition())))

if __name__ == '__main__':
    result = daily_etl_job.execute_in_process(
        run_config={
            'ops': {
                'wait_partition': {
                    'config': {'partition_date': '2024-01-01'}
                }
            }
        }
    )

Wait, but the resources are defined in the job's resource_defs. The source_db and target_db are stubbed as dictionaries with the required methods. The ResourceParam is used to indicate that the ops require these resources.

But in the code above, the job's resource_defs are set with ResourceParam instances wrapping the stubs. Wait, no. The resource_defs should map resource keys to resource definitions. So perhaps:

The source_db and target_db are defined using the @resource decorator.

Wait, perhaps the code should define the resources properly. Let me adjust.

from dagster import resource

@resource
def source_db(_):
    # Stub implementation
    class SourceDB:
        def execute_query(self, query, params=None):
            # In real scenario, this would execute the query and return results
            print(f"Executing query: {query} with params {params}")
            return [{'order_id': 1, 'customer_email': 'test@example.com', 'amount': 100, 'quantity': 2, 'timestamp': '2024-01-01 12:00:00'}]
    return SourceDB()

@resource
def target_db(_):
    # Stub implementation
    class TargetDB:
        def upsert_data(self, table, data):
            print(f"Upserting {len(data)} records into {table}")
    return TargetDB()

Then, the job uses these resources:

@job(
    resource_defs={
        'source_db': source_db,
        'target_db': target_db,
    }
)
def daily_etl_job():
    load(transform(extract_incremental(wait_partition())))

But in the ops, the parameters are named source_db and target_db, matching the resource keys. So the ops would have parameters with the same names as the resource keys.

Wait, in Dagster, if an op requires a resource, it's specified via the required_resource_keys parameter, or by using the ResourceParam in the function signature. For example:

@op(required_resource_keys={'source_db'})
def wait_partition(context):
    context.resources.source_db.execute_query(...)

But in the code above, the ops are written to receive the resources as parameters. That's possible if using the ResourceParam type annotation. For example:

@op
def wait_partition(context, source_db: ResourceParam):
    source_db.execute_query(...)

Yes, in Dagster, if you annotate a parameter with ResourceParam, and the name matches a resource key, it will be injected.

So combining all this, the code would:

- Define the resources using @resource.
- Define the ops with parameters for the resources using ResourceParam.
- Set the retry policies.
- Define the job with the resource_defs and the op dependencies.

Putting it all together:

The complete code would look like this.
</think>

from dagster import job, op, ResourceParam, RetryPolicy, resource

retry_policy = RetryPolicy(max_retries=2, delay=300)

@resource
def source_db(_):
    class SourceDBStub:
        def execute_query(self, query, params=None):
            print(f"Executing: {query} with {params}")
            return [{"order_id": 1, "customer_email": " test@EXAMPLE.com ", "amount": 100, "quantity": 2, "timestamp": "2024-01-01 12:00:00"}]
    return SourceDBStub()

@resource
def target_db(_):
    class TargetDBStub:
        def upsert_data(self, table, data):
            print(f"Upserting {len(data)} rows into {table}")
    return TargetDBStub()

@op(retry_policy=retry_policy)
def wait_partition(context, source_db: ResourceParam):
    partition_date = context.run_config["partition_date"]
    result = source_db.execute_query(
        "SELECT 1 FROM information_schema.partitions WHERE table_name=%s AND partition_name=%s",
        ("orders", f"p{partition_date.replace('-', '')}")
    )
    if not result:
        raise Exception("Partition not available")
    context.log.info("Partition ready")

@op(retry_policy=retry_policy)
def extract_incremental(context, source_db: ResourceParam):
    partition_date = context.run_config["partition_date"]
    data = source_db.execute_query(
        f"SELECT * FROM orders WHERE partition_date = '{partition_date}'"
    )
    context.log.info(f"Extracted {len(data)} records")
    return data

@op(retry_policy=retry_policy)
def transform(context, data):
    cleaned = []
    for record in data:
        record["customer_email"] = record["customer_email"].strip().lower()
        if record["amount"] < 0 or record["quantity"] <= 0:
            context.log.warning(f"Invalid order {record['order_id']}")
            continue
        record["timestamp"] = record["timestamp"].replace(" ", "T") + "Z"
        cleaned.append(record)
    context.log.info(f"Transformed {len(cleaned)} valid records")
    return cleaned

@op(retry_policy=retry_policy)
def load(context, transformed_data, target_db: ResourceParam):
    target_db.upsert_data("fact_orders", transformed_data)
    context.log.info("Data warehouse load complete")

@job(
    resource_defs={
        "source_db": source_db,
        "target_db": target_db
    },
    config={
        "ops": {
            "wait_partition": {"config": {"partition_date": "2024-01-01"}}
        }
    }
)
def daily_etl_job():
    load(transform(extract_incremental(wait_partition())))

if __name__ == "__main__":
    result = daily_etl_job.execute_in_process(
        run_config={
            "ops": {
                "wait_partition": {"config": {"partition_date": "2024-01-01"}}
            }
        }
    )