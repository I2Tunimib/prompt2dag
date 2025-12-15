from dagster import (
    op,
    job,
    ScheduleDefinition,
    resource,
    Field,
    String,
    RetryPolicy,
    OpExecutionContext,
)
from datetime import datetime
import os


@resource(config_schema={"connection_string": Field(String, default_value="dummy_connection")})
def database_resource(init_context):
    """Stub resource for database connections."""
    return {"connection_string": init_context.resource_config["connection_string"]}


@resource(config_schema={"temp_dir": Field(String, default_value="/tmp")})
def filesystem_resource(init_context):
    """Stub resource for filesystem operations."""
    return {"temp_dir": init_context.resource_config["temp_dir"]}


@op(
    required_resource_keys={"prod_db", "filesystem"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def dump_prod_csv(context: OpExecutionContext) -> str:
    """Creates a CSV snapshot from the production database."""
    temp_dir = context.resources.filesystem["temp_dir"]
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(temp_dir, f"prod_snapshot_{date_str}.csv")
    
    context.log.info(f"Dumping production database to {csv_path}")
    
    # In production, use: subprocess.run(["bash", "-c", f"mysqldump --csv prod_db > {csv_path}"], check=True)
    with open(csv_path, "w") as f:
        f.write("id,data\n1,test\n")
    
    return csv_path


@op(
    required_resource_keys={"dev_db", "filesystem"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def copy_dev(context: OpExecutionContext, csv_path: str):
    """Loads the CSV snapshot into the Development database."""
    context.log.info(f"Loading CSV into Development DB from {csv_path}")
    
    # In production, use: subprocess.run(["bash", "-c", f"mysql dev_db < {csv_path}"], check=True)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")


@op(
    required_resource_keys={"staging_db", "filesystem"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def copy_staging(context: OpExecutionContext, csv_path: str):
    """Loads the CSV snapshot into the Staging database."""
    context.log.info(f"Loading CSV into Staging DB from {csv_path}")
    
    # In production, use: subprocess.run(["bash", "-c", f"mysql staging_db < {csv_path}"], check=True)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")


@op(
    required_resource_keys={"qa_db", "filesystem"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def copy_qa(context: OpExecutionContext, csv_path: str):
    """Loads the CSV snapshot into the QA database."""
    context.log.info(f"Loading CSV into QA DB from {csv_path}")
    
    # In production, use: subprocess.run(["bash", "-c", f"mysql qa_db < {csv_path}"], check=True)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")


@job(
    tags={
        "team": "data_engineering",
        "pipeline_type": "replication",
        "category": "database",
        "pattern": "fanout",
    },
    resource_defs={
        "prod_db": database_resource,
        "dev_db": database_resource,
        "staging_db": database_resource,
        "qa_db": database_resource,
        "filesystem": filesystem_resource,
    },
)
def database_replication_pipeline():
    """Daily database replication pipeline with fan-out pattern."""
    csv_path = dump_prod_csv()
    copy_dev(csv_path)
    copy_staging(csv_path)
    copy_qa(csv_path)


daily_schedule = ScheduleDefinition(
    job=database_replication_pipeline,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
)


if __name__ == "__main__":
    run_config = {
        "resources": {
            "prod_db": {"config": {"connection_string": "prod_db_connection"}},
            "dev_db": {"config": {"connection_string": "dev_db_connection"}},
            "staging_db": {"config": {"connection_string": "staging_db_connection"}},
            "qa_db": {"config": {"connection_string": "qa_db_connection"}},
            "filesystem": {"config": {"temp_dir": "/tmp/dagster_replication"}},
        }
    }
    
    result = database_replication_pipeline.execute_in_process(run_config=run_config)
    assert result.success