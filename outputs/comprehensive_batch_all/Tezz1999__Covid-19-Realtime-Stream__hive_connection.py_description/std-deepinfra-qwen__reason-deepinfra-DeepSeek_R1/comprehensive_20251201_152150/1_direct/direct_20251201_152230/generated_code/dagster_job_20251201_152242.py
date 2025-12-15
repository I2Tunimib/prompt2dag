from dagster import op, job, resource, execute_in_process
import subprocess
import datetime


@resource
def hive_local_resource():
    # Simplified resource for Hive connection
    # In a real scenario, this would connect to the Hive cluster
    pass


@op(required_resource_keys={"hive_local"})
def run_after_loop(context):
    """Runs a Bash command to print the current system username."""
    result = subprocess.run(["echo", "$USER"], capture_output=True, text=True)
    context.log.info(f"Current username: {result.stdout.strip()}")


@op(required_resource_keys={"hive_local"})
def hive_script_task(context):
    """Executes a multi-statement Hive query to create a database, table, and insert a test record."""
    hive_query = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """
    # In a real scenario, this would execute the Hive query using the Hive connection
    context.log.info(f"Executing Hive query: {hive_query}")


@job(
    resource_defs={
        "hive_local": hive_local_resource
    },
    description="A simple linear pipeline that runs a Bash command and a Hive script."
)
def hive_script_job():
    hive_script_task(run_after_loop())


if __name__ == '__main__':
    result = hive_script_job.execute_in_process()