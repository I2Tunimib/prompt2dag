from dagster import op, job, ResourceDefinition, execute_in_process
import subprocess
import datetime


@op
def run_after_loop(context):
    """Runs a Bash command to print the current system username."""
    result = subprocess.run(['echo', '$(whoami)'], capture_output=True, text=True)
    context.log.info(f"Current username: {result.stdout.strip()}")


@op(required_resource_keys={"hive_connection"})
def hive_script_task(context):
    """Executes a multi-statement Hive query to create a database, table, and insert a test record."""
    hive_conn = context.resources.hive_connection
    hive_query = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """
    hive_conn.execute(hive_query)
    context.log.info("Hive script executed successfully.")


@job
def hive_script_job():
    """A Dagster job that orchestrates the execution of a Bash command followed by a Hive script."""
    hive_script_task(run_after_loop())


# Simplified resource for Hive connection
class HiveConnection:
    def execute(self, query):
        # Placeholder for actual Hive execution logic
        print(f"Executing Hive query: {query}")


hive_local_resource = ResourceDefinition.hardcoded_resource(HiveConnection(), "hive_local")


if __name__ == '__main__':
    result = hive_script_job.execute_in_process(
        resources={"hive_connection": hive_local_resource},
        run_config={
            "ops": {
                "hive_script_task": {
                    "config": {
                        "hive_connection": "hive_local"
                    }
                }
            }
        }
    )