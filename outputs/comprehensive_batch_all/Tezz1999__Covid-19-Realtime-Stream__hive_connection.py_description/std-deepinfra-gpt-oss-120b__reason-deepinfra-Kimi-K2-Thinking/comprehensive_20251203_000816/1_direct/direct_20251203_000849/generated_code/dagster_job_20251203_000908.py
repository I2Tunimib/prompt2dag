import subprocess
from dagster import InitResourceContext, ResourceDefinition, job, op


class HiveResource:
    """Simple stub representing a Hive connection."""

    def __init__(self, connection_name: str):
        self.connection_name = connection_name


def hive_resource_init(context: InitResourceContext) -> HiveResource:
    """
    Initialize the Hive resource.

    In a real deployment this would read connection details from
    ``context.resource_config``. Here we return a stub.
    """
    return HiveResource(connection_name="hive_local")


@op(required_resource_keys={"hive"})
def run_after_loop(context):
    """
    Echo the current system username using a Bash command.
    """
    try:
        result = subprocess.run(
            ["whoami"], capture_output=True, text=True, check=True
        )
        username = result.stdout.strip()
        context.log.info(f"Current user: {username}")
        return username
    except subprocess.CalledProcessError as exc:
        context.log.error(f"Failed to obtain username: {exc}")
        raise


@op(required_resource_keys={"hive"})
def hive_script_task(context, previous_output):
    """
    Execute a Hive script that creates a database, a table, and inserts a record.

    The ``previous_output`` argument ensures this op runs after ``run_after_loop``.
    """
    hive_sql = """
    CREATE DATABASE IF NOT EXISTS mydb;
    CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
    INSERT INTO mydb.test_af VALUES (2);
    """
    try:
        # Attempt to run the Hive CLI; this will work only if Hive is installed.
        cmd = ["hive", "-e", hive_sql]
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True
        )
        context.log.info("Hive script executed successfully.")
        context.log.debug(f"Hive output: {result.stdout}")
        return result.stdout
    except FileNotFoundError:
        # Hive CLI not available – simulate execution.
        context.log.warning("Hive client not found; simulating execution.")
        context.log.info("Simulated Hive script execution.")
        return "simulated"
    except subprocess.CalledProcessError as exc:
        context.log.error(f"Hive script failed: {exc.stderr}")
        raise


@job(
    resource_defs={
        "hive": ResourceDefinition.hardcoded_resource(HiveResource("hive_local"))
    }
)
def hive_script_job():
    """
    Dagster job that runs a Bash command followed by a Hive script.
    """
    hive_script_task(run_after_loop())


if __name__ == "__main__":
    result = hive_script_job.execute_in_process()
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")