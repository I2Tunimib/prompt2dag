from dagster import op, job, resource, execute_in_process
import subprocess
import datetime


@resource
def hive_local_resource():
    # Simplified resource for Hive connection
    return {
        'host': 'localhost',
        'port': 10000,
        'database': 'default',
        'username': 'user',
        'password': 'password'
    }


@op(required_resource_keys={'hive_local'})
def run_hive_script(context):
    """Run a multi-statement Hive query to create a database, table, and insert a test record."""
    hive_conn = context.resources.hive_local
    hive_query = """
    CREATE DATABASE IF NOT EXISTS mydb;
    USE mydb;
    CREATE TABLE IF NOT EXISTS test_af (test INT);
    INSERT INTO test_af VALUES (2);
    """
    command = f"hive -e \"{hive_query}\" -u {hive_conn['username']} -p {hive_conn['password']} -h {hive_conn['host']} -p {hive_conn['port']}"
    subprocess.run(command, shell=True, check=True)


@op
def run_bash_command():
    """Run a Bash command to print the current system username."""
    result = subprocess.run(['echo', '$(whoami)'], capture_output=True, text=True, check=True)
    print(result.stdout.strip())


@job(resource_defs={'hive_local': hive_local_resource})
def hive_script_job():
    """A simple linear job that first runs a Bash command and then a Hive script."""
    run_hive_script(run_bash_command())


if __name__ == '__main__':
    result = hive_script_job.execute_in_process()