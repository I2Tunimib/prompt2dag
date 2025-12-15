import subprocess
from typing import Any, Dict

from dagster import op, job, resource, OpExecutionContext


@resource
def hive_connection(_):
    """Stub resource for Hive connection configuration."""
    return {
        "host": "localhost",
        "port": 10000,
        "database": "default",
        "auth": "NONE",
    }


@op
def run_after_loop(context: OpExecutionContext) -> str:
    """Execute bash command to echo current system username."""
    context.log.info("Executing bash command")
    
    try:
        result = subprocess.run(
            ["bash", "-c", "echo $USER"],
            capture_output=True,
            text=True,
            check=True,
        )
        username = result.stdout.strip()
        context.log.info(f"Username: {username}")
        return username
    except subprocess.CalledProcessError as e:
        context.log.error(f"Command failed: {e}")
        raise


@op(required_resource_keys={"hive"})
def hive_script_task(context: OpExecutionContext, upstream_output: str) -> Dict[str, Any]:
    """Execute Hive operations: create DB, table, and insert record."""
    context.log.info(f"Upstream output: {upstream_output}")
    
    hive_config = context.resources.hive
    queries = [
        "CREATE DATABASE IF NOT EXISTS mydb",
        "USE mydb",
        "CREATE TABLE IF NOT EXISTS test_af (test INT)",
        "INSERT INTO test_af VALUES (2)",
    ]
    
    context.log.info("Executing Hive queries (stubbed):")
    for query in queries:
        context.log.info(f"  - {query}")
    
    return {
        "database": "mydb",
        "table": "test_af",
        "status": "success",
    }


@job(resource_defs={"hive": hive_connection})
def hive_script_job():
    """Linear job replicating hive_script Airflow DAG (daily at 1:00 AM)."""
    hive_script_task(run_after_loop())


if __name__ == "__main__":
    result = hive_script_job.execute_in_process()
    assert result.success
    print("Job completed successfully")