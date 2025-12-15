<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements. The pipeline has three sequential tasks: wait_for_file, validate_schema, and load_db. The first task uses a FileSensor to monitor for a specific file pattern, then the next tasks validate the schema and load into PostgreSQL.

First, I'll import the necessary Prefect modules. Since the user mentioned using Prefect 2.x, I'll use @flow and @task decorators. The FileSensor is part of prefect.filesystem, so I need to import that.

For the wait_for_file task, the sensor needs to check the /data/incoming directory every 30 seconds for up to 24 hours. Prefect's FileSensor has a timeout parameter, but wait_for_file might need to be a task that uses the sensor. Wait, in Prefect 2, sensors are tasks themselves. So maybe the wait_for_file is a task that uses the FileSensor. But how to structure that? Alternatively, perhaps the FileSensor is used within the flow. Let me check Prefect's documentation. Oh right, in Prefect 2, you can use the wait_for parameter in a flow, but for a task that acts as a sensor, maybe we need to create a task that uses the FileSensor. Alternatively, perhaps the FileSensor is a block. Wait, no. The FileSensor is a prefect.filesystem.FileSensor which is a task. So the wait_for_file task would be an instance of FileSensor configured with the right path and parameters.

Wait, the FileSensor in Prefect is a task that can be used to wait for a file. So the wait_for_file step can be implemented as a FileSensor task. So in the code, I'll create a task using FileSensor. But how to structure that? Let me think. The FileSensor has parameters like path, timeout, poke_interval. So in the flow, the wait_for_file task would be the FileSensor configured with path="/data/incoming/transactions_*.csv" (but the pattern is transactions_YYYYMMDD.csv, maybe using a date in the filename). Wait, the pattern is transactions_YYYYMMDD.csv, but the actual code would need to generate the expected filename based on the current date. Hmm, that's a complication. Because the sensor is looking for a file with today's date in the name. So perhaps the flow needs to generate the expected file name dynamically each day.

Wait, the pipeline runs daily, so each run is for a specific date. The file name would have the date of the run. So the flow should parameterize the file path with the current date. So maybe the flow takes a date parameter, which defaults to the current date. Then, the wait_for_file task checks for the file with that date.

But how to handle that in the sensor? The FileSensor's path can be a formatted string with the date. So in the flow, perhaps we pass the date as a parameter, then construct the path accordingly.

So the flow could have a parameter like run_date, which defaults to the current date. Then, the file path is built using that date. For example, path=f"/data/incoming/transactions_{run_date.strftime('%Y%m%d')}.csv".

But the user's pipeline description mentions that the FileSensor monitors for a file matching transactions_YYYYMMDD.csv. So each day's run is looking for that day's file. Therefore, the flow should generate the expected filename based on the execution date.

So the flow should accept a date parameter, perhaps with a default of the current date. Then, the sensor uses that date to form the file path.

But how to handle this in Prefect? Let's structure the flow with a parameter for the date. Then, in the flow, the sensor is configured with the path built using that date.

So the flow function would be something like:

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.today()
    file_path = f"/data/incoming/transactions_{run_date.strftime('%Y%m%d')}.csv"
    # Then use the file_path in the FileSensor

Wait, but the FileSensor is a task that needs to be called. So perhaps the wait_for_file task is the FileSensor task, configured with the path. But in Prefect 2, how do we use the FileSensor? Let me check. The FileSensor is part of the prefect.filesystem module. So we can create an instance of it as a task.

Wait, looking at Prefect's documentation, the FileSensor is a task that can be used within a flow. So the code would be:

from prefect.filesystem import FileSensor

@flow
def transaction_pipeline():
    sensor_task = FileSensor(
        path="/data/incoming/transactions_20240101.csv",  # example, but needs dynamic date
        timeout=24*60*60,  # 24 hours in seconds
        poke_interval=30
    )
    sensor_task()

But this approach hardcodes the date in the path. Instead, we need to generate the path dynamically based on the run date. So perhaps the flow takes a parameter for the run date, and constructs the path accordingly.

So modifying the flow:

from datetime import datetime
from prefect import flow, task
from prefect.filesystem import FileSensor

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.today()
    file_path = f"/data/incoming/transactions_{run_date.strftime('%Y%m%d')}.csv"
    wait_for_file = FileSensor(
        path=file_path,
        timeout=86400,  # 24*60*60
        poke_interval=30
    )
    wait_for_file()

But wait, in Prefect 2, tasks are defined with @task decorators. However, the FileSensor is a pre-built task. So using it directly in the flow as a task is acceptable.

Once the sensor task is completed (i.e., the file is found), the next tasks validate_schema and load_db can proceed.

But according to the pipeline description, the tasks are sequential: wait_for_file → validate_schema → load_db. So after the sensor task runs, the validate_schema task is called, then load_db.

But the validate_schema and load_db are custom tasks. So I need to define them with @task decorators.

So the validate_schema task takes the file path as an argument, validates the schema. Similarly, the load_db task takes the file path and loads it into PostgreSQL.

So putting it all together:

The flow function will first run the FileSensor task, then call validate_schema, passing the file path, then call load_db with the same file path.

Now, the FileSensor task's result isn't the file path, but the sensor's success. So perhaps after the sensor completes, we proceed to the next tasks, which need the file path. Therefore, the file path is constructed in the flow and passed to the tasks.

So the code outline would be:

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.today()
    file_path = f"/data/incoming/transactions_{run_date.strftime('%Y%m%d')}.csv"
    # Wait for file
    wait_for_file_task = FileSensor(
        path=file_path,
        timeout=86400,
        poke_interval=30
    )
    wait_for_file_task()
    # Validate schema
    validate_schema_task(file_path)
    # Load to DB
    load_db_task(file_path)

@task
def validate_schema_task(file_path: str):
    # Validation logic here

@task
def load_db_task(file_path: str):
    # Load logic here

But wait, the FileSensor task is a Prefect task, so when called within the flow, it's executed. Once it completes (i.e., the file is found), the flow proceeds to the next tasks.

Now, the validate_schema_task and load_db_task are @task decorated functions. So in the flow, they are called normally (since they are sequential), not with .submit(), unless there's parallelism. But the pipeline description says the steps are sequential, so no need for .submit().

Now, the validate_schema task needs to check the column names and data types. How to implement that? The task could read the CSV file, check the columns and their data types. For example, using pandas to read the CSV, then check the columns and dtypes.

Similarly, the load_db task connects to PostgreSQL and loads the data. Using psycopg2 or SQLAlchemy to copy the CSV into the table.

But the user's code needs to be executable, assuming required packages are installed. So I'll include the necessary imports inside the task functions. For example, in validate_schema_task, import pandas, and in load_db_task, import psycopg2.

But the user might not have these packages installed, but the code should assume they are. So the code should include the necessary logic with try-except blocks? Or just proceed, assuming the environment is set up.

The problem says to output executable code assuming required packages are installed. So I can proceed to write the code with those imports, even if they are not part of the standard library.

So, in validate_schema_task:

import pandas as pd
from datetime import date

def validate_schema_task(file_path):
    df = pd.read_csv(file_path)
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    if list(df.columns) != expected_columns:
        raise ValueError("Column names do not match expected schema")
    # Check data types
    # For transaction_id and customer_id, check they are strings
    if df['transaction_id'].dtype != 'object' or df['customer_id'].dtype != 'object':
        raise ValueError("transaction_id or customer_id are not strings")
    # Check amount is a decimal (float in pandas)
    if not pd.api.types.is_float_dtype(df['amount']):
        raise ValueError("amount is not a decimal")
    # Check transaction_date is a date
    try:
        pd.to_datetime(df['transaction_date'], format='%Y-%m-%d')
    except ValueError:
        raise ValueError("transaction_date has invalid date format")

Similarly, for load_db_task, using psycopg2 to connect and copy the CSV:

def load_db_task(file_path):
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",  # Assuming default db; adjust as needed
        user="postgres",    # Default user; adjust
        password="postgres" # Default password; adjust
    )
    cur = conn.cursor()
    try:
        copy_sql = sql.SQL("COPY public.transactions FROM {} WITH CSV HEADER").format(
            sql.Literal(file_path)
        )
        cur.execute(copy_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

But the database credentials are hardcoded here, which is not best practice. However, the problem states to use minimal safe defaults. So perhaps leave them as placeholders, but the user would need to adjust. Alternatively, use Prefect blocks for configuration. But the user's constraints don't mention using blocks, so perhaps hardcode with a comment indicating to adjust.

But the problem says to avoid placeholders like <TODO>, so perhaps use a comment indicating that the credentials should be configured properly, perhaps using environment variables or Prefect blocks. But for the code to be executable, maybe use default values that might work with a local PostgreSQL instance (like user 'postgres', password 'postgres', etc.), but include a comment.

Now, regarding scheduling: the pipeline should run daily starting Jan 1, 2024, with catchup disabled. The user mentioned to include a brief comment about deployment/schedule configuration. So in the code, after the flow definition, add a comment indicating the schedule.

In Prefect 2, schedules are defined when deploying the flow. So the code itself doesn't include the schedule, but the comment can mention it. For example:

# To deploy with a daily schedule:
# prefect deploy --name transaction_pipeline --schedule "0 0 * * *" --start-date 2024-01-01 --no-catchup

But the user's code should include the comment. So adding a comment in the module.

Now, retry policy: the flow should have 2 retries with 5-minute delays. In Prefect, retries can be configured at the task or flow level with decorator parameters. So adding retries to each task.

For example, adding @task(retries=2, retry_delay_seconds=300) to each task decorator.

But wait, the pipeline description says the retry policy is 2 retries with 5-minute delays between attempts. So each task should have retries=2 and retry_delay_seconds=300.

So updating the task decorators:

@task(retries=2, retry_delay_seconds=300)
def validate_schema_task(file_path: str):
    ...

@task(retries=2, retry_delay_seconds=300)
def load_db_task(file_path: str):
    ...

But the FileSensor task (wait_for_file) is a Prefect built-in task. How to set retries on it? Since it's part of the flow, perhaps the flow's settings apply, or we can set the retries on the sensor task. The FileSensor may have its own retry settings, but according to the pipeline description, the retry policy applies to the entire pipeline. So perhaps each task, including the sensor, should have retries=2. But the sensor's timeout is 24 hours, and it pokes every 30 seconds. Retries would mean that if the sensor fails (times out), it would retry. But the pipeline description says the sensor waits up to 24 hours. So maybe the sensor's timeout is 24*3600 seconds, and retries are not needed for the sensor itself. The retry policy applies to the validate and load tasks. So perhaps only those tasks have retries.

But the pipeline's retry policy is for the entire flow. Wait, the description says "Retry Policy: 2 retries with 5-minute delays between attempts". So each task in the flow should have this retry configuration. So adding the retry parameters to each @task decorator.

Now, putting all together:

The code starts with imports, then defines the tasks with retries, then the flow that orchestrates them.

The FileSensor is configured with the dynamic file path based on run_date.

The flow is called under the if __name__ == '__main__' guard.

So the complete code would look like:

from datetime import datetime
from prefect import flow, task
from prefect.filesystem import FileSensor

@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: str):
    """Validates CSV schema against expected columns and data types."""
    import pandas as pd

    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    expected_dtypes = {
        "transaction_id": "object",
        "customer_id": "object",
        "amount": "float64",
        "transaction_date": "datetime64[ns]"
    }

    df = pd.read_csv(file_path)
    
    # Validate column names
    if list(df.columns) != expected_columns:
        raise ValueError("Column names do not match expected schema")
    
    # Validate data types
    for col, dtype in expected_dtypes.items():
        if str(df[col].dtype) != dtype:
            raise ValueError(f"Column {col} has incorrect data type: expected {dtype}, found {df[col].dtype}")

@task(retries=2, retry_delay_seconds=300)
def load_to_postgresql(file_path: str):
    """Loads validated CSV data into PostgreSQL database."""
    import psycopg2
    from psycopg2 import sql

    # In production, use Prefect Blocks for credentials
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    
    try:
        copy_query = sql.SQL("COPY public.transactions FROM {} WITH (FORMAT CSV, HEADER)").format(
            sql.Literal(file_path)
        )
        cur.execute(copy_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

@flow
def transaction_pipeline(run_date: datetime = None):
    """Main pipeline orchestrating file processing workflow."""
    if run_date is None:
        run_date = datetime.today()
    
    file_path = f"/data/incoming/transactions_{run_date.strftime('%Y%m%d')}.csv"
    
    # Wait for file arrival
    FileSensor(
        path=file_path,
        timeout=86400,  # 24 hours
        poke_interval=30
    ).wait()
    
    # Process file
    validate_schema(file_path)
    load_to_postgresql(file_path)

if __name__ == "__main__":
    # Example: For local execution with specific date
    # transaction_pipeline(datetime(2024, 1, 1))
    transaction_pipeline()

# Deployment configuration note:
# Schedule: @daily starting 2024-01-01 with catchup disabled
# Use 'prefect deploy' with appropriate parameters

Wait, in Prefect 2, the FileSensor is a task that needs to be called. So using .wait() might not be correct. Let me check the correct usage. The FileSensor task's run method will block until the file is found or timeout. So in Prefect 2, when you create a FileSensor instance and call it, it will perform the waiting. So in the flow:

wait_for_file = FileSensor(path=file_path, timeout=86400, poke_interval=30)
wait_for_file()

But in Prefect 2, tasks are called as functions within the flow. So the code would be:

FileSensor(path=file_path, timeout=86400, poke_interval=30)()

Alternatively, assign to a variable and call it. But perhaps the correct syntax is to instantiate the sensor and then call it as a task.

Wait, perhaps the correct way is:

sensor_task = FileSensor(path=file_path, timeout=86400, poke_interval=30)
sensor_task()

But in Prefect, when you define a task, you use the @task decorator. However, FileSensor is a pre-defined task class. So creating an instance of it and calling it within the flow is correct.

But in the code above, the flow has:

FileSensor(...).wait()

Wait, perhaps the .wait() method is not correct. The FileSensor task's run method already handles the waiting. So just calling the task instance as a function would suffice.

So modifying the code:

In the flow:

FileSensor(
    path=file_path,
    timeout=86400,
    poke_interval=30
)()

But in Prefect 2, when you create a task instance, you call it like a function. So the code should be:

sensor = FileSensor(...)
sensor()

Alternatively, chain the instantiation and call:

FileSensor(...)()

So in the flow:

FileSensor(
    path=file_path,
    timeout=86400,
    poke_interval=30
)()

This would execute the sensor task.

Now, testing if this code would work. The sensor task is called, which waits for the file. Once found, proceeds to validate and load.

Another point: the validate_schema task uses pandas to check data types. The expected_dtypes for 'amount' is 'float64', which corresponds to decimal in the problem description. The transaction_date is expected to be a datetime, which matches the date type.

In the load task, the COPY command uses FORMAT CSV, HEADER. Assuming the CSV has a header row, which it should since the validation checks the column names.

The database connection details are hardcoded, which is not ideal, but per the problem's constraints, minimal safe defaults are used. In practice, Prefect blocks would be better for storing credentials.

The if __name__ == '__main__' block calls transaction_pipeline(), which uses the current date.

The deployment comment mentions the schedule, which would be configured via prefect deploy with the appropriate parameters.

So this code should meet all the requirements: uses Prefect 2.x tasks and flow, handles the sensor, sequential tasks with retries, dynamic file path based on date, and includes necessary imports and structure.
</think>

from datetime import datetime
from prefect import flow, task
from prefect.filesystem import FileSensor

@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: str):
    """Validates CSV schema against expected columns and data types."""
    import pandas as pd

    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    df = pd.read_csv(file_path)

    if list(df.columns) != expected_columns:
        raise ValueError("Column names do not match expected schema")
    
    if not all(df[col].dtype == "object" for col in ["transaction_id", "customer_id"]):
        raise ValueError("String columns contain invalid data types")
    
    try:
        pd.to_numeric(df["amount"], errors="strict")
    except ValueError:
        raise ValueError("Amount column contains non-decimal values")
    
    try:
        pd.to_datetime(df["transaction_date"])
    except ValueError:
        raise ValueError("Invalid date values in transaction_date column")

@task(retries=2, retry_delay_seconds=300)
def load_to_postgresql(file_path: str):
    """Loads validated CSV data into PostgreSQL database."""
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    
    try:
        copy_query = sql.SQL("""
            COPY public.transactions 
            FROM {} 
            WITH (FORMAT CSV, HEADER)
        """).format(sql.Literal(file_path))
        
        cur.execute(copy_query)
        conn.commit()
    finally:
        cur.close()
        conn.close()

@flow
def transaction_pipeline(run_date: datetime = None):
    """Main pipeline orchestrating file processing workflow."""
    if run_date is None:
        run_date = datetime.today()
    
    file_path = f"/data/incoming/transactions_{run_date.strftime('%Y%m%d')}.csv"
    
    FileSensor(
        path=file_path,
        timeout=86400,  # 24 hours
        poke_interval=30
    )()
    
    validate_schema(file_path)
    load_to_postgresql(file_path)

if __name__ == "__main__":
    transaction_pipeline()

# Deployment configuration note:
# Schedule: @daily starting 2024-01-01 with catchup disabled
# Use 'prefect deploy' with cron schedule "0 0 * * *"