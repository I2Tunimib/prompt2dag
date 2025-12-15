from dagster import (
    op,
    job,
    resource,
    ScheduleDefinition,
    Definitions,
    In,
    Nothing,
)
import subprocess


@resource(config_schema={"host": str, "port": int, "username": str})
def hive_local_resource(init_context):
    """Resource for connecting to local Hive instance.
    
    In production, this would return a live Hive connection using
    libraries like pyhive or impyla.
    """
    return {
        "host": init_context.resource_config["host"],
        "port": init_context.resource_config["port"],
        "username": init_context.resource_config["username"],
    }


@op
def run_after_loop() -> str:
    """Execute a bash command to echo the current system username."""
    try:
        # Execute bash command with shell=True to allow command substitution
        result = subprocess.run(
            "echo Current username: $(whoami)",
            capture_output=True,
            text=True,
            check=True,
            shell=True,
        )
        output = result.stdout.strip()
        print(output)
        return output
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Bash command failed: {e.stderr}") from e


@op(
    ins={"upstream": In(Nothing)},
    required_resource_keys={"hive_local"},
)
def hive_script_task(context) -> str:
    """Execute Hive operations: create database, create table, and insert record.
    
    This op depends on the bash task completing successfully but does not
    consume its output.
    """
    hive_config = context.resources.hive_local
    
    # Define Hive queries to execute
    hive_queries = [
        "CREATE DATABASE IF NOT EXISTS mydb",
        "CREATE TABLE IF NOT EXISTS mydb.test_af (test INT)",
        "INSERT INTO mydb.test_af VALUES (2)",
    ]
    
    # Simulate query execution (in real implementation, use Hive client)
    for query in hive_queries:
        print(f"Executing Hive query: {query}")
        # Real implementation would be:
        # from pyhive import hive
        # with hive.Connection(**hive_config) as conn:
        #     with conn.cursor() as cursor:
        #         cursor.execute(query)
    
    return "Hive operations completed successfully"


@job(resource_defs={"hive_local": hive_local_resource})
def hive_script_job():
    """Linear job that runs bash command followed by Hive operations.
    
    Execution flow:
    1. run_after_loop: Echo current username
    2. hive_script_task: Create Hive DB/table and insert record
    """
    # Define dependency: hive_script_task runs after run_after_loop
    hive_script_task(run_after_loop())


# Schedule for daily execution at 1:00 AM
hive_script_schedule = ScheduleDefinition(
    job=hive_script_job,
    cron_schedule="0 1 * * *",  # Daily at 1:00 AM
)


# Definitions object to bundle job and schedule for Dagster deployment
defs = Definitions(
    jobs=[hive_script_job],
    schedules=[hive_script_schedule],
)


# For running the job directly
if __name__ == "__main__":
    # Example configuration for local execution
    run_config = {
        "resources": {
            "hive_local": {
                "config": {
                    "host": "localhost",
                    "port": 10000,
                    "username": "hive",
                }
            }
        }
    }
    
    # Execute the job
    result = hive_script_job.execute_in_process(run_config=run_config)
    print(f"Job execution result: {'SUCCESS' if result.success else 'FAILED'}")