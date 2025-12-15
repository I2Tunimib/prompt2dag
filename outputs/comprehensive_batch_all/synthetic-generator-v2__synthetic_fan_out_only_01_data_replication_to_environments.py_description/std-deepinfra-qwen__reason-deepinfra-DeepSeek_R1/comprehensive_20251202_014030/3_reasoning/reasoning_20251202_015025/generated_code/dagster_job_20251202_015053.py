<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has four steps: dump_prod_csv, copy_dev, copy_staging, and copy_qa. The first step runs sequentially, then the next three run in parallel. So the dependencies are dump_prod_csv → all three copy tasks, which are independent.

Dagster uses ops and jobs. Each step will be an op. The job will set the dependencies so that the three copy ops depend on the dump op. Since they're parallel, they don't depend on each other.

The user mentioned using Bash operators, but in Dagster, we can use the @op decorator and execute shell commands within the ops. So each op will run a shell command. For example, dump_prod_csv might run a command to create a CSV file. The copy ops will each load that CSV into their respective environments.

I need to make sure that the dump_prod_csv generates a dated file. Maybe the op can get the current date and use that in the filename. The other ops will need the path to this file. So the dump op should return the file path, and the copy ops will take that as an input. That way, Dagster handles the dependency by passing the output of dump_prod_csv to each copy op.

Wait, but in the original description, the three copy tasks are triggered by the dump task. So in Dagster terms, each copy op should depend on the dump op. So in the job definition, each copy op is downstream of dump_prod_csv. That way, they all start after the dump completes.

Now, for the shell commands. The user said all tasks are implemented as Bash operators executing shell commands. So each op will run a specific command. For example, the dump_prod_csv op might run something like "pg_dump -U user -d db -f /tmp/file.csv". The copy commands would be similar but target different environments. But since this is a minimal example, maybe the commands are placeholders with echo for simplicity, but structured to show the real commands in comments.

The user also mentioned resources and config. Maybe the database connections (like credentials) would be handled via resources. But the problem says to provide simplified resource examples as comments or stubs. So perhaps I can define a database_resource with config, but not fully implement it. Or maybe just use config for the file path.

The pipeline has a daily schedule, retries, owner tags, etc. For the job, I can include tags in the @job decorator. Retries can be set with the retry_policy. The schedule isn't part of the code here unless they want a schedule definition, but the user asked for the job module. The launch pattern at the bottom is just the if __name__ block with execute_in_process.

So putting it all together:

- Define each op with @op. The dump op creates the file and returns the path. The copy ops take that path as input and run their commands.

- The job will have the three copy ops depending on the dump op.

- Add tags to the job for owner and categories.

- Include a retry policy with 2 attempts and 5-minute delay. In Dagster, that's a RetryPolicy defined and passed to the ops. But the user says "standard retry configuration", so maybe each op uses the same policy. So I can create a retry_policy variable and apply it to each op.

Wait, the user mentioned that all tasks have the retry config. So each op should have the same retry policy. So I'll define a retry_policy with max_retries=2 and delay=300 seconds (5 minutes). Then add it to each @op decorator.

For resources, maybe the database connections are handled via resources. But since the user said to provide minimal stubs, perhaps each op uses a config_schema to get the target environment's credentials. Alternatively, since the example uses shell commands, maybe the ops just take the file path from the dump op and the environment names are hardcoded in the commands. But the problem states to avoid placeholders, so maybe the commands are commented examples.

So the dump_prod_csv op could generate a dated filename, using datetime. The op's compute function gets the current date, creates the file path, runs the dump command, and returns the path. The copy ops then take that path as input and run their respective copy commands.

Testing if the code will be executable. Need to make sure all ops are properly defined and dependencies set. The job is assembled with the dump op followed by the three copy ops, each depending on the dump.

Also, include the necessary imports: from dagster import op, job, RetryPolicy.

Putting it all into code:

First, define the retry policy.

retry_policy = RetryPolicy(max_retries=2, delay=300)

Then each op uses retry_policy=retry_policy.

The dump_prod_csv op:

@op(retry_policy=retry_policy)
def dump_prod_csv(context):
    # Example command (replace with actual production dump command):
    # pg_dump -U prod_user -h prod_host -d prod_db -f {file_path}
    import datetime
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    file_path = f"/tmp/prod_snapshot_{date_str}.csv"
    context.log.info(f"Generating production CSV at {file_path}")
    # Simulate file creation
    with open(file_path, "w") as f:
        f.write("mock_data")
    return file_path

Each copy op:

@op(retry_policy=retry_policy)
def copy_dev(context, file_path):
    # Example command (replace with actual dev load command):
    # psql -U dev_user -h dev_host -d dev_db -c "\copy table FROM '{file_path}'"
    context.log.info(f"Loading {file_path} into DEV environment")
    # Simulate load
    with open(file_path, "r") as f:
        data = f.read()
    context.log.info(f"DEV copy complete. Data: {data[:20]}...")

Similarly for copy_staging and copy_qa, changing the log messages.

Then the job:

@job(
    tags={"owner": "data_engineering", "category": "replication,database,fanout"},
)
def database_replication_job():
    file_path = dump_prod_csv()
    copy_dev(file_path)
    copy_staging(file_path)
    copy_qa(file_path)

Finally, the main block:

if __name__ == "__main__":
    database_replication_job.execute_in_process()

This should satisfy all constraints. The code is self-contained, uses ops with dependencies, retry policies, tags, and minimal resource/config stubs (the commands are commented). The shell commands are placeholders but shown as comments. The actual file handling is simulated with Python code for testability.
</think>

from dagster import op, job, RetryPolicy
from datetime import datetime

retry_policy = RetryPolicy(max_retries=2, delay=300)  # 5 minutes in seconds

@op(retry_policy=retry_policy)
def dump_prod_csv(context):
    """Creates dated CSV snapshot from production database"""
    date_str = datetime.now().strftime("%Y%m%d")
    file_path = f"/tmp/prod_snapshot_{date_str}.csv"
    
    # Example production dump command (replace with actual):
    # pg_dump -U prod_user -h prod_host -d prod_db -f {file_path}
    context.log.info(f"Generating production CSV at {file_path}")
    
    # Simulate file creation
    with open(file_path, "w") as f:
        f.write("mock_data")
    return file_path

@op(retry_policy=retry_policy)
def copy_dev(context, file_path):
    """Loads snapshot into Development environment"""
    # Example load command (replace with actual):
    # psql -U dev_user -h dev_host -d dev_db -c "\copy table FROM '{file_path}'"
    context.log.info(f"Loading {file_path} into DEV environment")
    
    # Simulate load process
    with open(file_path, "r") as f:
        data = f.read()
    context.log.info(f"DEV copy complete. Data: {data[:20]}...")

@op(retry_policy=retry_policy)
def copy_staging(context, file_path):
    """Loads snapshot into Staging environment"""
    context.log.info(f"Loading {file_path} into STAGING environment")
    with open(file_path, "r") as f:
        data = f.read()
    context.log.info(f"STAGING copy complete. Data: {data[:20]}...")

@op(retry_policy=retry_policy)
def copy_qa(context, file_path):
    """Loads snapshot into QA environment"""
    context.log.info(f"Loading {file_path} into QA environment")
    with open(file_path, "r") as f:
        data = f.read()
    context.log.info(f"QA copy complete. Data: {data[:20]}...")

@job(
    tags={"owner": "data_engineering", "category": "replication,database,fanout"},
)
def database_replication_job():
    file_path = dump_prod_csv()
    copy_dev(file_path)
    copy_staging(file_path)
    copy_qa(file_path)

if __name__ == "__main__":
    database_replication_job.execute_in_process()