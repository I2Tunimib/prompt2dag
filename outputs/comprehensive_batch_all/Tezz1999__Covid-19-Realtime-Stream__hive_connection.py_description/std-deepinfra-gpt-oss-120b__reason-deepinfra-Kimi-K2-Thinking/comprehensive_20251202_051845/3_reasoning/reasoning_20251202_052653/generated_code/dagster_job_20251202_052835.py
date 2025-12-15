from dagster import op, job, schedule, Field, resource, InitResourceContext
import subprocess
from typing import Any


@resource(config_schema={
    "host": Field(str, default_value="localhost"),
    "port": Field(int, default_value=10000),
    "username": Field(str, default_value="hive"),
})
def hive_local_resource(init_context: InitResourceContext) -> dict:
    """Provides Hive connection configuration."""
    return init_context.resource_config


@op
def run_after_loop(context) -> str:
    """Echo current system username via bash command."""
    try:
        result = subprocess.run(
            "echo 'Current username:' $(whoami)",
            capture_output=True,
            text=True,
            check=True,
            shell=True
        )
        output = result.stdout.strip()
        context.log.info(f"Bash output: {output}")
        return output
    except subprocess.CalledProcessError as e:
        context.log.error(f"Bash failed: {e.stderr}")
        raise


@op(required_resource_keys={"hive_local"})
def hive_script_task(context, _bash_output: str) -> str:
    """Create Hive database/table and insert test record."""
    hive_config = context.resources.hive_local
    
    queries = [
        "CREATE DATABASE IF NOT EXISTS mydb",
        "CREATE TABLE IF NOT EXISTS mydb.test_af (test INT)",
        "INSERT INTO mydb.test_af VALUES (2)"
    ]
    
    context.log.info(
        f"Connecting to Hive at {hive_config['host']}:{hive_config['port']}"
    )
    
    # Simulate Hive execution - replace with actual client (e.g., pyhive) in production
    for query in queries:
        context.log.info(f"Executing: {query}")
    
    return "Hive operations completed"


@job(
    resource_defs={"hive_local": hive_local_resource},
    description="Linear pipeline for basic Hive database operations"
)
def hive_script_job():
    """Bash command followed by Hive operations."""
    hive_script_task(run_after_loop())


@schedule(
    cron_schedule="0 1 * * *",
    job=hive_script_job,
    execution_timezone="UTC"
)
def hive_script_schedule(_context):
    """Daily at 1:00 AM UTC."""
    return {}


if __name__ == '__main__':
    result = hive_script_job.execute_in_process()
    assert result.success