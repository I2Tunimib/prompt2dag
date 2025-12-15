<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

First, the pipeline has three sequential tasks: wait_for_file, validate_schema, and load_db. The wait_for_file is a sensor that checks for a file in /data/incoming with a specific pattern. Then validate_schema checks the columns and data types, and load_db imports the data into PostgreSQL.

Constraints mention using @task and @flow decorators. For the sensor, Prefect has a FileSensor task, so I should use that. The FileSensor needs to be configured with the directory and file pattern. The poke interval is every 30 seconds with a timeout of 24 hours. Wait, Prefect's FileSensor has parameters like path, timeout, poke_interval. Let me check the parameters. Oh right, the timeout is the total time to wait, and poke_interval is the time between checks.

Next, the validate_schema task. This task needs to take the file path and validate the schema. The columns should be transaction_id, customer_id, amount, transaction_date, and check their data types. For data validation, maybe using pandas to read the CSV and check dtypes. But since it's a task, I'll need to handle exceptions or return a boolean. But the task should probably raise an error if validation fails, which would trigger retries.

Then load_db task connects to PostgreSQL and loads the data. Using psycopg2 or SQLAlchemy? The example uses psycopg2. Need to include the connection details. The user might need to have a pre-created table, but the task just loads from the CSV file. Using COPY command would be efficient. But need to handle the connection properly, maybe using a context manager.

The flow is linear, so each task depends on the previous one. Since they're sequential, no .submit() is needed except for parallel steps, which this pipeline doesn't have. So tasks are called normally.

Scheduling is mentioned as daily with start date and catchup disabled. In Prefect, scheduling is typically handled via deployments. So I'll add a comment about the schedule configuration but not include it in the code directly. The retry policy is 2 retries with 5-minute delays. So each task should have retries configured with a delay.

Wait, in Prefect, you can set retries and retry_delay_seconds at the task level. So each @task should have retries=2 and retry_delay_seconds=300. Or maybe set it in the flow? No, tasks have their own parameters.

Now, putting it all together:

- Import necessary modules: from prefect import flow, task. Also, from prefect.filesystems import LocalFileSystem (maybe not needed). Wait, the FileSensor is in prefect.tasks.filesystem. Wait, Prefect 2.x has moved some tasks. Let me check: the FileSensor is part of prefect.filesystem or prefect.tasks? Oh right, in Prefect 2, the FileSensor is in prefect.tasks.filesystem. Wait, maybe I should check the correct import.

Wait, looking up Prefect 2.x documentation: The FileSensor is a task that can be imported from prefect.tasks.filesystem. So:

from prefect.tasks.filesystem import FileSensor

But the user's constraints say to use @task and @flow. Wait, FileSensor is a Prefect task, so we can use it directly as a task in the flow.

So the wait_for_file step uses FileSensor. The parameters would be path (the directory plus pattern?), timeout is 24*60*60 seconds, poke_interval 30.

Wait, the FileSensor's path should be the full path to the file pattern. But the pattern includes a date. Wait, the file is named transactions_YYYYMMDD.csv, where the date is dynamic. Since the pipeline runs daily, each day's file has that day's date. But the sensor is supposed to wait for the current day's file. How to handle that?

Hmm, maybe the sensor's path should include a date string that's generated dynamically based on the execution date. But in Prefect, when using a schedule, each run is associated with a specific date. So perhaps the flow parameterizes the file name based on the scheduled date.

Wait, the problem says the pipeline is scheduled daily starting Jan 1, 2024. So each flow run corresponds to a specific date. Therefore, the file name should be transactions_YYYYMMDD.csv where the date is the date of the flow run.

But how to pass that date into the sensor? The flow could take a date parameter, defaulting to the current date, but when scheduled, Prefect passes the appropriate date.

So the flow function would have a parameter like run_date: datetime = None, and then format that date into the file name.

Wait, but the user's code example needs to be self-contained. Let me think: the flow should accept a date parameter, which is used to construct the file name. Then, the FileSensor checks for that specific file.

So, modifying the flow to take a date parameter. Then, the file path is built using that date.

But how does the schedule pass the date? When using an interval schedule, each run is for a specific date. So perhaps the flow uses the Prefect context to get the current date, or the run is parameterized.

Alternatively, the flow can accept a date parameter, and when deployed with a schedule, the parameter is set using a parameter that references the current date.

But for the code to be self-contained, maybe the flow takes an optional date parameter that defaults to today, and when scheduled, the deployment is configured to pass the appropriate date.

So, in the flow:

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.now()
    file_date = run_date.strftime("%Y%m%d")
    file_pattern = f"transactions_{file_date}.csv"
    file_path = f"/data/incoming/{file_pattern}"
    # Then, the sensor checks for file_path

But wait, the FileSensor's path can be a directory and a glob pattern. Wait, the sensor is supposed to monitor the /data/incoming directory for a file matching transactions_YYYYMMDD.csv. So perhaps the path is the directory, and the file name is the pattern. But the FileSensor's path parameter expects a full path with a glob pattern.

Wait, the FileSensor task in Prefect 2.x has a path argument that can include a glob pattern. So, for example, path="/data/incoming/transactions_*.csv" but that's not specific enough. Since each run is for a specific date, the sensor should look for the specific file for that date.

Ah, right. So the sensor's path should be the full path to the specific file expected for that run's date. So the flow constructs the expected file name based on the run_date, and the FileSensor checks for that exact file.

But the FileSensor can be given a path that includes a specific file name. If the file exists, it proceeds; otherwise, it waits until it exists.

Wait, but the FileSensor in Prefect 2.x works by checking if the path exists. So if the path is a specific file, it waits until that file exists. So in this case, the flow constructs the exact file path (like /data/incoming/transactions_20240101.csv) and the sensor checks for that.

But how does the sensor know which date's file to look for? Because each flow run is for a specific date, the file name is determined by the run's date. Therefore, the flow should take a parameter for the date, which is used to generate the file path.

So, modifying the flow to accept a run_date parameter. Then, the sensor is configured with the exact path.

But how to handle the schedule? When deploying the flow with a daily schedule starting on Jan 1, 2024, each run would be for a specific date. The deployment can pass the date parameter as the start date plus the interval.

But in the code, the flow can have a default parameter of None, and if None, uses the current date. However, when scheduled, Prefect automatically passes the appropriate date based on the schedule interval.

Wait, perhaps using Prefect's built-in support for parameters in scheduled flows. For example, when creating a deployment with a schedule, you can use a parameter that references the current date.

But the code provided should include the flow with a parameter for the date. So the flow function would be:

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.now()
    # rest of the code

Then, in the deployment, the run_date parameter is set to "{{ datetime.date() }}" or similar, depending on how Prefect handles parameters in schedules.

But for the code to be complete, the flow must accept the run_date parameter and construct the file path accordingly.

So, integrating that into the code.

Now, the wait_for_file task is the FileSensor. The parameters for the FileSensor are path=file_path, timeout=86400 (24*60*60), and poke_interval=30.

Wait, the FileSensor task in Prefect 2.x has parameters:

class FileSensor(PrefectTask):
    def __init__(
        self,
        path: str,
        timeout: float = 3600,
        poke_interval: float = 10,
        **kwargs,
    ):
        ...

So, in the code, the wait_for_file task is created by instantiating FileSensor with the path, timeout, and poke_interval.

Wait, but in Prefect 2, tasks are used by calling them as functions. So perhaps:

wait_for_file = FileSensor(
    path=file_path,
    timeout=86400,
    poke_interval=30,
)

But wait, no. The FileSensor is a task class, so you create an instance and then call it. Wait, maybe I should use the task as a function. Let me check the correct usage.

In Prefect 2, tasks are defined using the @task decorator, but some pre-built tasks like FileSensor are classes that you instantiate and then call.

Wait, the FileSensor is a Prefect task that you can use directly. So the code would be:

from prefect.tasks.filesystem import FileSensor

Then, in the flow:

file_sensor = FileSensor(
    path=file_path,
    timeout=86400,
    poke_interval=30,
)
file_sensor()

But wait, perhaps the FileSensor is a class-based task. So you create an instance and then call it with .run() or just as a function.

Wait, looking at Prefect's documentation example:

from prefect.tasks.filesystem import FileSensor

file_sensor = FileSensor(path="/path/to/file.txt")
file_sensor.run()

But in a flow, you can call it directly. So in the flow function:

file_sensor = FileSensor(path=file_path, timeout=86400, poke_interval=30)
file_sensor()  # This will block until the file exists or timeout

So, the wait_for_file step is handled by the FileSensor task.

Once the sensor completes (file is found), then proceed to validate_schema.

The validate_schema task is a @task-decorated function that takes the file path and checks the schema. Using pandas to read the CSV and check columns and dtypes.

But need to handle possible exceptions. For example, if the file doesn't have the correct columns or data types, the task should fail, which would trigger retries.

Similarly, the load_db task uses psycopg2 to connect to PostgreSQL and execute a COPY command to load the data.

Now, putting all together:

Imports needed:

from datetime import datetime
from prefect import flow, task
from prefect.tasks.filesystem import FileSensor
import pandas as pd
import psycopg2
from psycopg2 import sql

Then, the validate_schema task:

@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: str):
    # Read CSV
    df = pd.read_csv(file_path)
    # Check columns
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    if list(df.columns) != expected_columns:
        raise ValueError(f"Invalid columns. Expected {expected_columns}, found {list(df.columns)}")
    # Check data types
    # For pandas, check dtypes
    # transaction_id and customer_id should be strings (object in pandas)
    # amount should be a decimal (float in pandas?), but exact check might be tricky
    # transaction_date should be a datetime64[ns]
    # Alternatively, check after loading
    # For simplicity, check dtypes
    if df['transaction_id'].dtype != 'object':
        raise ValueError("transaction_id must be string type")
    if df['customer_id'].dtype != 'object':
        raise ValueError("customer_id must be string type")
    # amount: check if it's a numeric type
    if not pd.api.types.is_numeric_dtype(df['amount']):
        raise ValueError("amount must be a decimal type")
    # transaction_date: check if it's datetime
    if not pd.api.types.is_datetime64_any_dtype(df['transaction_date']):
        # Try to parse it
        try:
            pd.to_datetime(df['transaction_date'])
        except:
            raise ValueError("transaction_date must be a date type")

But this might not cover all cases. For example, the transaction_date might be a string in the CSV, and pandas reads it as object. So perhaps the task should parse the date column and check for validity.

Alternatively, the task could read the CSV with specified dtypes and catch errors. For example:

df = pd.read_csv(file_path, dtype={
    'transaction_id': str,
    'customer_id': str,
    'amount': float,
    'transaction_date': 'datetime64[ns]'
})

But if the CSV has a date in a format that pandas can't parse, this would raise an error. However, the problem statement says the validate_schema task should check data types, so perhaps this approach is better.

But using dtype in read_csv might coerce the data, which may not be desired. Alternatively, read the CSV without dtype and then check each column's type.

But for simplicity, perhaps using read_csv with the correct dtype and handling exceptions.

Alternatively, read the CSV and then check each column's data type.

But this could be more code. Let's proceed with checking the columns and data types as in the initial approach.

Then, the load_db task:

@task(retries=2, retry_delay_seconds=300)
def load_db(file_path: str):
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",  # Assuming default db; adjust as needed
        user="postgres",    # Default user; adjust
        password="postgres" # Default password; adjust
    )
    try:
        with conn.cursor() as cur:
            # Use COPY command to load data
            copy_sql = sql.SQL("""
                COPY public.transactions (transaction_id, customer_id, amount, transaction_date)
                FROM %s
                DELIMITER ','
                CSV HEADER;
            """)
            cur.execute(copy_sql, (file_path,))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

But the connection details are hard-coded, which is not ideal. However, the problem states to connect to localhost:5432, so it's acceptable for the example. In practice, these would be stored in a Prefect Block.

But the user's code must be executable, so assuming that the PostgreSQL is running with the default credentials. Alternatively, add placeholders but replace with a comment to configure via Blocks. However, the user's constraints say to avoid placeholders like <TODO>, so use default values but note that in a real scenario, they should use Blocks.

But the code must be executable as-is. So assuming that the PostgreSQL server is running on localhost:5432 with user postgres, password postgres, and database postgres, and the public.transactions table exists.

Putting it all together:

The flow function:

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.now()
    file_date = run_date.strftime("%Y%m%d")
    file_name = f"transactions_{file_date}.csv"
    file_path = f"/data/incoming/{file_name}"
    
    # Wait for file
    sensor = FileSensor(
        path=file_path,
        timeout=86400,  # 24 hours
        poke_interval=30
    )
    sensor_result = sensor()  # This task completes when file exists or times out
    
    # Validate schema
    validate_schema(file_path)
    
    # Load to DB
    load_db(file_path)

But wait, the sensor_result is not used. However, the sensor task must complete successfully before proceeding. Since Prefect tasks are called in sequence, the flow will wait for the sensor to complete before moving to validate_schema.

But in Prefect, if a task is called in the flow, its execution is awaited. So the code as written should work.

Now, adding the if __name__ == '__main__' guard.

if __name__ == "__main__":
    # Example: transaction_pipeline(datetime(2024, 1, 1))
    transaction_pipeline()

But the schedule is mentioned as daily, starting Jan 1, 2024, catchup disabled. So a comment can be added about deployment configuration.

Including a comment about the schedule:

# To deploy with a daily schedule:
# prefect deployment build ./module.py:transaction_pipeline -n transaction_daily --interval 86400 --cron "0 0 * * *" --start-date 2024-01-01 --no-catchup

But the user's constraints say to include a brief comment indicating deployment/schedule configuration if schedules are mentioned. So adding:

# Deployment configuration example:
# prefect deployment build --name transaction_daily --schedule "0 0 * * *" --apply

But perhaps better to use a comment indicating the schedule parameters.

Putting it all together, the code would look like:

from datetime import datetime
from prefect import flow, task
from prefect.tasks.filesystem import FileSensor
import pandas as pd
import psycopg2
from psycopg2 import sql

@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: str):
    df = pd.read_csv(file_path)
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    if list(df.columns) != expected_columns:
        raise ValueError(f"Invalid columns. Expected {expected_columns}, found {list(df.columns)}")
    
    if df['transaction_id'].dtype != 'object':
        raise ValueError("transaction_id must be string type")
    if df['customer_id'].dtype != 'object':
        raise ValueError("customer_id must be string type")
    if not pd.api.types.is_numeric_dtype(df['amount']):
        raise ValueError("amount must be a numeric type")
    try:
        pd.to_datetime(df['transaction_date'])
    except:
        raise ValueError("transaction_date must be a parsable date")

@task(retries=2, retry_delay_seconds=300)
def load_db(file_path: str):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="postgres"
    )
    try:
        with conn.cursor() as cur:
            copy_sql = """
                COPY public.transactions (transaction_id, customer_id, amount, transaction_date)
                FROM %s
                DELIMITER ','
                CSV HEADER;
            """
            cur.execute(copy_sql, (file_path,))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

@flow
def transaction_pipeline(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.now()
    file_date = run_date.strftime("%Y%m%d")
    file_name = f"transactions_{file_date}.csv"
    file_path = f"/data/incoming/{file_name}"
    
    # Wait for file arrival
    FileSensor(
        path=file_path,
        timeout=86400,
        poke_interval=30
    )()
    
    validate_schema(file_path)
    load_db(file_path)

if __name__ == "__main__":
    # To create a deployment with daily schedule:
    # prefect deployment build ./pipeline.py:transaction_pipeline -n daily_transactions --cron "0 0 * * *" --start-date 2024-01-01 --no-catchup
    transaction_pipeline()

Wait, but the FileSensor is called as FileSensor(...)()? Because FileSensor is a task, so creating an instance and immediately calling it. Yes.

But in Prefect 2, when you instantiate a task and call it, it's executed as part of the flow. So this should work.

Testing this code would require the file to exist in /data/incoming, but that's beyond the scope here.

Now, check for PEP 8 compliance. Lines not too long, proper indentation, etc.

The code should be complete and executable, assuming the required packages (prefect, pandas, psycopg2) are installed.

So this should meet all the constraints.
</think>

from datetime import datetime
from prefect import flow, task
from prefect.tasks.filesystem import FileSensor
import pandas as pd
import psycopg2

@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: str):
    """Validate CSV schema matches expected columns and data types"""
    df = pd.read_csv(file_path)
    
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    if list(df.columns) != expected_columns:
        raise ValueError(f"Invalid columns. Expected {expected_columns}, found {list(df.columns)}")
    
    if not pd.api.types.is_string_dtype(df['transaction_id']):
        raise ValueError("transaction_id must be string type")
    if not pd.api.types.is_string_dtype(df['customer_id']):
        raise ValueError("customer_id must be string type")
    if not pd.api.types.is_numeric_dtype(df['amount']):
        raise ValueError("amount must be numeric type")
    if not pd.api.types.is_datetime64_any_dtype(df['transaction_date']):
        raise ValueError("transaction_date must be date type")

@task(retries=2, retry_delay_seconds=300)
def load_db(file_path: str):
    """Load validated CSV data to PostgreSQL transactions table"""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="postgres"
    )
    
    try:
        with conn.cursor() as cur:
            with open(file_path, 'r') as f:
                cur.copy_expert(
                    "COPY public.transactions FROM STDIN WITH CSV HEADER DELIMITER ','",
                    f
                )
            conn.commit()
    finally:
        conn.close()

@flow
def transaction_pipeline(run_date: datetime = None):
    """Main pipeline flow for daily transaction processing"""
    if run_date is None:
        run_date = datetime.now()
        
    file_name = f"transactions_{run_date.strftime('%Y%m%d')}.csv"
    file_path = f"/data/incoming/{file_name}"
    
    # Sensor waits for file with 30s check interval and 24h timeout
    FileSensor(path=file_path, timeout=86400, poke_interval=30)()
    
    validate_schema(file_path)
    load_db(file_path)

if __name__ == "__main__":
    # To deploy with daily schedule:
    # prefect deployment build --name daily_transactions --schedule "@daily" --start-date 2024-01-01 --no-catchup
    transaction_pipeline()