<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x Python module. Let me start by understanding the requirements.

First, the constraints mention using Prefect 2.x style with @flow and @task decorators. Each step should be a task, and the flow orchestrates them in order. For parallel steps, use .submit(), but here all steps are sequential except maybe the sensor, but the example shows linear dependencies.

The pipeline has four steps: wait_partition, extract_incremental, transform, load. They must run sequentially. The wait_partition is a sensor that polls the database. In Prefect, there isn't a direct SqlSensor, so I'll need to implement that as a task with a loop and sleep, but maybe using Prefect's built-in wait_for or something else. Alternatively, since Prefect tasks are synchronous, perhaps using a custom task that retries with a delay.

Wait_partition uses reschedule mode with 5-minute intervals and 1-hour timeout. Reschedule mode in Airflow means the task frees up the worker slot. In Prefect, tasks run in the main process unless using task runners, but maybe using retries with delays could simulate this. Alternatively, use a while loop with timeouts and sleeps, but that's not ideal. However, the user might expect a task that polls the database until the partition is available or timeout.

So, for wait_partition task, I'll create a function that checks the database partition. If not available, it raises an exception, and we can use retries with a delay to simulate the polling. The timeout is 1 hour, which is 12 retries (5 minutes * 12 = 60). So set retries=12, retry_delay_seconds=300. But Prefect's retry mechanism is for failures, not for waiting. Alternatively, the task could internally loop and check until the timeout. Hmm, maybe better to handle it within the task.

Wait_partition task: inside the task, loop for up to 1 hour, checking every 5 minutes. If the partition is found, return. Otherwise, raise an exception after timeout. But how to handle that in Prefect? Maybe the task will run once, and if the partition isn't available, it fails and retries. But retries would be immediate unless we have a retry delay. Wait, the user mentioned reschedule mode with 5-minute poke interval. So perhaps configure the task with retries=12 (since 12*5=60 minutes) and retry_delay_seconds=300. Each retry is a poke. If after 12 retries it's not available, the task fails.

Yes, that makes sense. So the wait_partition task will attempt to check the partition, and if not available, raise an exception. With retries and retry_delay, it will keep retrying every 5 minutes up to 12 times (total 1 hour). Once it succeeds, it proceeds.

Next, the extract_incremental task runs a SQL query. Then transform, then load. All sequential.

The flow is decorated with @flow, and each step is a @task. The tasks are called in order: wait_partition -> extract_incremental -> transform -> load.

For scheduling, the user mentioned a daily schedule starting Jan 1, 2024, catchup disabled. In Prefect, scheduling is handled via deployments. The code should include a comment about deployment configuration. So add a comment at the end about the schedule.

Now, code structure:

Import prefect modules: from prefect import flow, task. Also, need to handle database connections. The code should include placeholders for the database connections, but since the user wants executable code, maybe use mock functions with comments.

For example, the wait_partition task might call a function check_partition_exists(), which is a placeholder. Similarly, execute_query() for extract, etc.

So, code outline:

- Imports: prefect, time, maybe datetime.

Wait_partition task:

@task(retries=12, retry_delay_seconds=300)
def wait_partition():
    # code to check partition
    if not check_partition_exists():
        raise Exception("Partition not available")
    return True

But the check_partition_exists function needs to be defined. Since the user said to make the code executable, perhaps mock it with a function that returns True after some time, but for the sake of example, maybe just return True. But that's not realistic. Alternatively, simulate a check that eventually passes. But maybe the user expects a placeholder with a comment.

So, inside wait_partition task:

from prefect import task, flow
import time

def check_partition_exists():
    # Implement actual database check here
    # Simulating success after some time
    current_time = time.time()
    if current_time > ...:  # some condition
        return True
    else:
        return False

But perhaps for simplicity, the code will have placeholder functions with comments indicating where the actual implementation would go.

Similarly, the extract_incremental task would execute a SQL query. So:

def execute_extract_query():
    # Execute SQL query to extract new orders
    return []

But in code, maybe return sample data.

Transform task would process the data.

Load task would upsert into the data warehouse.

Putting it all together:

The code would have the tasks defined with @task, and the @flow orchestrating them in order.

Include the if __name__ == '__main__': block to run the flow.

Also, add a comment about the schedule configuration.

So, the final code would look something like:

from prefect import flow, task
import time
from datetime import datetime

# Placeholder for database connection handling
def get_database_conn():
    # Return a database connection
    return None

def check_partition_exists(conn, table_name, partition_date):
    # Implement actual partition check logic
    # Simulate partition availability after certain time
    current_hour = datetime.now().hour
    return current_hour >= 3  # Example: partition available after 3 AM

@task(retries=12, retry_delay_seconds=300)
def wait_partition():
    conn = get_database_conn()
    partition_date = datetime.today().strftime('%Y-%m-%d')
    if not check_partition_exists(conn, 'orders', partition_date):
        raise RuntimeError("Partition not available")
    return True

@task
def extract_incremental():
    conn = get_database_conn()
    # Execute SQL query to extract new orders
    query = f"SELECT * FROM orders WHERE partition_date = '{datetime.today().strftime('%Y-%m-%d')}'"
    # results = conn.execute(query).fetchall()
    # return results
    return [{"order_id": 1, "customer": "test", "amount": 100}]  # Sample data

@task
def transform(extracted_data):
    # Clean customer info, validate amounts, format timestamps
    transformed = []
    for record in extracted_data:
        # Example transformations
        record['customer'] = record['customer'].strip().upper()
        if record['amount'] < 0:
            raise ValueError("Invalid order amount")
        record['timestamp'] = datetime.now().isoformat()
        transformed.append(record)
    return transformed

@task
def load(transformed_data):
    # Upsert into data warehouse
    # conn = get_data_warehouse_conn()
    # Implement loading logic
    print(f"Loaded {len(transformed_data)} records into fact_orders")

@flow
def daily_etl_orders():
    wait_partition_result = wait_partition()
    extracted_data = extract_incremental(wait_partition_result)
    transformed_data = transform(extracted_data)
    load(transformed_data)

if __name__ == "__main__":
    # To create a deployment with schedule:
    # prefect deploy --name daily_orders_etl --interval 86400 --start-date 2024-01-01 --no-catchup
    daily_etl_orders()

Wait, but the tasks are called sequentially. The wait_partition_result is passed to extract_incremental, but since wait_partition returns True, maybe the dependency is just by calling order. In Prefect, even if you don't pass the result, the tasks are sequenced if they are called in order. But in the code above, extract_incremental is called with wait_partition_result, which ensures the dependency. Alternatively, since the flow is written as:

wait_partition()
extract_incremental()
transform()
load()

But Prefect automatically infers dependencies based on the order of function calls within the flow. However, when using .submit() for concurrent tasks, you need to pass futures. But here, all tasks are sequential, so calling them in order is sufficient.

But in the code above, the wait_partition_result is passed as an argument to extract_incremental, which would create a data dependency. However, wait_partition returns a boolean, which extract_incremental doesn't use. So perhaps better to call them in order without passing arguments.

So adjust the flow:

@flow
def daily_etl_orders():
    wait_partition()
    extracted_data = extract_incremental()
    transformed_data = transform(extracted_data)
    load(transformed_data)

But in Prefect, tasks called within a flow create dependencies based on their execution order. So even if you don't pass the output of wait_partition to extract_incremental, since extract_incremental is called after wait_partition, Prefect will run them in sequence.

But in the initial code example, the user's code passes the result, which is unnecessary. So better to remove that.

So modifying the flow:

@flow
def daily_etl_orders():
    wait_partition()
    extracted_data = extract_incremental()
    transformed_data = transform(extracted_data)
    load(transformed_data)

This way, the tasks are executed in order.

Also, the check_partition_exists function is a helper function. The wait_partition task uses retries with a delay to simulate the sensor's reschedule mode.

The retries=12 and retry_delay_seconds=300 (5 minutes) gives a total of 1 hour (12*5=60). If the task keeps failing (partition not available), after 12 retries, it will fail.

The extract_incremental task uses a SQL query with the current date. The transform task processes the data, and the load task upserts into the warehouse.

The code includes placeholder functions for database connections and queries, which would be replaced in a real implementation. But the code is executable as is, returning sample data.

The if __name__ block calls the flow, and there's a comment about deployment configuration with schedule.

I think this covers all the requirements. Now, check for PEP 8 compliance, like line lengths, whitespace, etc. Also, ensure all tasks are properly decorated and the flow is structured correctly.
</think>

from prefect import flow, task
import time
from datetime import datetime
from typing import List, Dict

def get_database_connection():
    """Return database connection (placeholder implementation)"""
    return None

def check_partition_available(conn, table: str, date: datetime) -> bool:
    """Check if daily partition exists (simulated implementation)"""
    return datetime.now().hour >= 2  # Simulate partition available after 2 AM

@task(retries=12, retry_delay_seconds=300)
def wait_partition():
    """Poll database for daily partition availability"""
    conn = get_database_connection()
    partition_date = datetime.today().date()
    
    if not check_partition_available(conn, "orders", partition_date):
        raise RuntimeError(f"Partition for {partition_date} not available")
    return True

@task
def extract_incremental() -> List[Dict]:
    """Execute SQL query to extract new orders from daily partition"""
    conn = get_database_connection()
    today = datetime.today().strftime("%Y-%m-%d")
    return [{"id": 1, "customer": "prefect", "amount": 99.99}]  # Sample data

@task
def transform(extracted_data: List[Dict]) -> List[Dict]:
    """Clean and validate order data"""
    processed = []
    for order in extracted_data:
        # Example transformation logic
        order["customer"] = order["customer"].strip().lower()
        order["timestamp"] = datetime.now().isoformat()
        if order["amount"] < 0:
            raise ValueError("Invalid order amount")
        processed.append(order)
    return processed

@task
def load(transformed_data: List[Dict]):
    """Load transformed data into data warehouse"""
    print(f"Loading {len(transformed_data)} records to fact_orders")  # Implement actual load logic

@flow
def daily_orders_etl():
    """Main ETL flow with sensor-gated execution"""
    wait_partition()
    raw_data = extract_incremental()
    cleaned_data = transform(raw_data)
    load(cleaned_data)

if __name__ == "__main__":
    # To deploy with schedule:
    # prefect deploy --name orders-etl --interval 86400 --start-date 2024-01-01 --no-catchup
    daily_orders_etl()