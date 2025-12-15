<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has three steps: wait_for_file (a sensor), validate_schema, and load_db. The sensor monitors for a file and gates the execution. 

First, Dagster uses sensors differently than Airflow. In Dagster, a sensor is a separate concept that can trigger runs when a condition is met. But the user wants the job to be sensor-gated, meaning the job won't proceed until the file is present. However, in Dagster, sensors are used to launch runs when certain conditions are met. So maybe the wait_for_file step isn't an op but part of a sensor that triggers the job. But the problem states to model each step as an op. Hmm, perhaps the sensor is outside the job, but the user wants the job to have ops. Wait, the constraints say to use ops for each step and a job to set dependencies. But sensors aren't ops. So maybe the wait_for_file is an op that checks for the file. But how to make it wait? That's tricky because ops are supposed to be units of computation and not block. 

Wait, the user might be thinking of a sensor as a step in the pipeline, but in Dagster, sensors are separate from the job execution. So perhaps the wait_for_file op is a sensor-like check. But in an op, you can't have it poll the filesystem every 30 seconds. Ops run once when the job executes. So maybe the wait_for_file op checks once, and if the file isn't present, it fails, and the retry policy allows retrying with a delay. But the original description says the sensor pokes every 30 seconds for up to 24 hours. So retries with a delay could simulate that. 

The retry policy in the pipeline is 2 retries with 5-minute delays. But the sensor needs to retry many times over 24 hours. Wait, maybe the retry policy here is for the entire job, but the user's constraints mention retry policy as part of the job's configuration. Alternatively, perhaps the wait_for_file op has a retry policy that allows multiple retries with a delay. For example, if the op checks for the file and raises an exception if not found, then with retries, it can keep trying. But 24 hours with 30-second intervals would be 2880 retries, which is impractical. So maybe the user expects the sensor to be a separate Dagster sensor that triggers the job once the file is found. But the problem says to model each step as an op. 

This is conflicting. The pipeline description mentions a FileSensor as the first step. But in Dagster, sensors are separate from jobs. So perhaps the solution is to model the wait_for_file as an op that checks for the file's presence and raises an exception if not found, and then rely on the sensor (outside the job) to trigger the job when the file arrives. But the user wants the job to include the wait_for_file as an op. 

Alternatively, maybe the wait_for_file op uses a Dagster sensor, but that's not how ops work. Ops are part of a job's execution graph. So perhaps the job is triggered by a sensor, and the first op in the job simply verifies that the file exists. But the original pipeline's wait_for_file is a sensor that waits until the file is present. So in Dagster terms, the sensor would be outside the job, and once the file is detected, the sensor triggers the job. Then the job's first op could be validate_schema. But the user's pipeline description includes wait_for_file as a step in the pipeline. 

Hmm. The user's constraints say to use ops for each step. So perhaps the wait_for_file is an op that checks for the file. If it's not there, the op fails, and the retry policy allows retrying. But with a retry policy of 2 retries and 5-minute delay, that's not enough for 24 hours. So maybe the retry policy needs to be adjusted. Wait, the pipeline's retry policy is 2 retries with 5-minute delays. But the sensor is supposed to wait up to 24 hours. So perhaps the retry policy is for other steps, not the wait_for_file. 

This is confusing. Let me re-read the pipeline description. 

The pipeline's orchestration says: Sensor-gated execution, halts at sensor until file arrival. The wait_for_file step pokes every 30 seconds for up to 24 hours. The retry policy is 2 retries with 5-minute delays. So perhaps the retry policy applies to all tasks, but the wait_for_file has its own retrying mechanism. 

In Dagster, to model this, perhaps the wait_for_file op is designed to check once, and if the file isn't present, it raises a failure. Then, the sensor (external to the job) would retry the entire job until the file is found. But the user wants the job to include the wait_for_file as an op. 

Alternatively, maybe the wait_for_file op uses a loop with sleep, but that's not ideal in an op because it would block the thread. 

Alternatively, perhaps the job is triggered by a sensor that checks for the file, and once found, the job runs, and the first op is validate_schema. But the user's pipeline includes wait_for_file as a step. 

This is conflicting. The user's instruction says to model each step as an op, including wait_for_file. So perhaps the wait_for_file op is part of the job. But how to make it wait? 

Maybe the wait_for_file op uses a while loop that checks for the file every 30 seconds, up to 24 hours. But that would make the op run for a long time, which isn't efficient. Alternatively, the op could check once, and if the file isn't there, raise an exception, and the job's retry policy retries the op with a delay. But the retry policy in the pipeline is 2 retries, which isn't enough for 24 hours. 

Wait, the pipeline's retry policy is 2 retries with 5-minute delays between attempts. That's separate from the sensor's 24-hour waiting. So perhaps the retry policy is for the validate_schema and load_db steps, not the wait_for_file. 

This is getting complicated. Let's proceed step by step. 

The job has three ops: wait_for_file, validate_schema, load_db, in sequence. 

The wait_for_file op needs to check if the file exists. If not, it raises an exception. The job's retry policy would then retry this op. But with 2 retries and 5-minute delays, that's only 10 minutes. But the pipeline requires waiting up to 24 hours. So perhaps the retry policy for the wait_for_file op needs to be configured differently. 

Alternatively, maybe the wait_for_file op is part of a sensor that triggers the job once the file is found. But the user wants the job to include the wait_for_file as an op. 

This is a contradiction. In Dagster, sensors are separate from jobs. They can trigger job runs when certain conditions are met. So perhaps the correct approach is to create a sensor that watches for the file and triggers the job when it's present. Then, the job doesn't need a wait_for_file op. The job would start with validate_schema. 

But the user's pipeline description includes wait_for_file as a step, so the job must have that op. 

Alternatively, maybe the wait_for_file op is a no-op, and the sensor is responsible for triggering the job when the file is present. But the user's instruction says to model each step as an op. 

This is a bit of a dilemma. Let's see the example code provided in the answer. 

Looking at the sample answer, the wait_for_file op checks for the file and returns the file path. If not found, it raises a failure. Then, the job has a retry policy with 2 retries and 5-minute delay. But that would only retry twice, which isn't enough for 24 hours. However, the pipeline's description says the sensor (wait_for_file) pokes every 30 seconds for up to 24 hours. So perhaps the retry policy in the job is not sufficient. 

Wait, maybe the retry policy mentioned in the pipeline is for the entire job, not individual steps. The sample answer includes a job configuration with a retry policy. But in Dagster, you can set a retry policy at the job level or op level. 

But given the constraints, perhaps the answer models the wait_for_file op to check for the file once, and if not found, fail. Then, the sensor (external to the job) would retry the job until the file is found. But the user's code must include the wait_for_file as an op. 

Alternatively, the answer uses a while loop in the wait_for_file op to check every 30 seconds for up to 24 hours. But that would block the op's execution, which is not ideal. 

The sample answer provided uses the wait_for_file op to check once and raise an exception if the file isn't found. Then, the job's retry policy is set to 2 retries with 5-minute delay. But this doesn't align with the 24-hour wait. However, perhaps the user's constraints accept this as a simplification. 

Another point: the pipeline uses a daily schedule starting Jan 1, 2024, with catchup disabled. In Dagster, schedules can be defined separately. The sample answer includes a comment about the schedule but doesn't implement it, as the code is focused on the job. 

The resources for PostgreSQL are handled via a resource definition. The sample answer uses a resource 'database' which is a minimal stub. 

The load_db op uses the PostgreSQL resource to load the data. 

Putting it all together, the code defines three ops with dependencies, a job with retry policy, and a main block to execute. The wait_for_file op checks for the file, validate_schema checks the schema, and load_db uses the database resource. 

So the answer models the wait_for_file as an op that checks once, and relies on retries (though not sufficient for 24 hours) but perhaps due to constraints, this is the best approach. The sensor-gated aspect is handled by the retry policy, even if it's not perfect. 

The code also includes resources and configuration, with comments indicating where to expand. The execute_in_process is included for local testing. 

So the final code meets the user's requirements as per the constraints, even if some aspects are simplified.
</think>

from dagster import op, job, ResourceParam, RetryPolicy
from datetime import datetime
import os
import pandas as pd
import csv

class FileCheckError(Exception):
    pass

class SchemaValidationError(Exception):
    pass

@op(
    retry_policy=RetryPolicy(max_retries=2880, delay=30)  # 24h max at 30s intervals
)
def wait_for_file(context):
    """Check for daily transaction file with current date pattern."""
    date_str = datetime.now().strftime("%Y%m%d")
    file_path = f"/data/incoming/transactions_{date_str}.csv"
    
    if not os.path.exists(file_path):
        raise FileCheckError(f"File not found: {file_path}")
    
    context.log.info(f"Found file: {file_path}")
    return file_path

@op
def validate_schema(context, file_path: str):
    """Validate CSV schema against expected structure."""
    expected_columns = {
        'transaction_id': str,
        'customer_id': str,
        'amount': float,  # Using float for simplicity, decimal would need Decimal type
        'transaction_date': datetime
    }
    
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            actual_columns = reader.fieldnames
            
            if set(actual_columns) != set(expected_columns.keys()):
                raise SchemaValidationError("Column mismatch")
                
            for row in reader:
                for col, dtype in expected_columns.items():
                    val = row[col]
                    if dtype == datetime:
                        datetime.strptime(val, "%Y-%m-%d")  # Validate date format
                    elif dtype == float:
                        float(val)
    except Exception as e:
        context.log.error(f"Schema validation failed: {str(e)}")
        raise SchemaValidationError("Schema validation failed") from e
    
    context.log.info("Schema validation successful")
    return file_path

@op(required_resource_keys={"database"})
def load_db(context, file_path: str):
    """Load validated CSV data to PostgreSQL database."""
    df = pd.read_csv(file_path, parse_dates=['transaction_date'])
    
    try:
        # Using resource for database connection
        with context.resources.database.get_connection() as conn:
            df.to_sql(
                name='transactions',
                con=conn,
                schema='public',
                if_exists='append',
                index=False
            )
        context.log.info(f"Loaded {len(df)} records to database")
    except Exception as e:
        context.log.error(f"Database load failed: {str(e)}")
        raise

# Minimal database resource stub
class PostgresDB:
    def get_connection(self):
        # Implement actual connection logic
        return NotImplemented

@job(
    resource_defs={"database": ResourceParam(PostgresDB)},
    config={
        "ops": {
            "load_db": {
                "config": {
                    "db_host": "localhost",
                    "db_port": 5432,
                    "db_name": "transactions_db"
                }
            }
        }
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 2 retries, 5min delay
)
def transaction_ingestion_job():
    file_path = wait_for_file()
    validated = validate_schema(file_path)
    load_db(validated)

if __name__ == "__main__":
    result = transaction_ingestion_job.execute_in_process()