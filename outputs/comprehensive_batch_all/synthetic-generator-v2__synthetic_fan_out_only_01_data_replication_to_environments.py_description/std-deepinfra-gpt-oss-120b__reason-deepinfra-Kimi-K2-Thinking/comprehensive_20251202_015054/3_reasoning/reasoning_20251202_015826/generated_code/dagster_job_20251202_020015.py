from datetime import datetime
import os

from dagster import (
    op,
    job,
    ScheduleDefinition,
    DefaultScheduleStatus,
    RetryPolicy,
    Out,
    Output,
    In,
    resource,
    Field,
    String,
)


# Minimal resource stubs for database connections and filesystem
@resource(config_schema={"connection_string": Field(String, default_value="postgresql://localhost:5432/prod")})
def prod_db_resource(init_context):
    """Production database connection resource."""
    return {"connection_string": init_context.resource_config["connection_string"]}


@resource(config_schema={"connection_string": Field(String, default_value="postgresql://localhost:5432/dev")})
def dev_db_resource(init_context):
    """Development database connection resource."""
    return {"connection_string": init_context.resource_config["connection_string"]}


@resource(config_schema={"connection_string": Field(String, default_value="postgresql://localhost:5432/staging")})
def staging_db_resource(init_context):
    """Staging database connection resource."""
    return {"connection_string": init_context.resource_config["connection_string"]}


@resource(config_schema={"connection_string": Field(String, default_value="postgresql://localhost:5432/qa")})
def qa_db_resource(init_context):
    """QA database connection resource."""
    return {"connection_string": init_context.resource_config["connection_string"]}


@resource(config_schema={"temp_dir": Field(String, default_value="/tmp")})
def filesystem_resource(init_context):
    """Filesystem resource for temporary storage."""
    return {"temp_dir": init_context.resource_config["temp_dir"]}


# Pipeline ops
@op(
    required_resource_keys={"prod_db", "filesystem"},
    out={"csv_path": Out(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"step": "extract"},
)
def dump_prod_csv(context):
    """
    Creates a CSV snapshot from the production database.
    Generates a dated file in the temporary directory.
    """
    prod_db = context.resources.prod_db
    filesystem = context.resources.filesystem
    temp_dir = filesystem["temp_dir"]
    
    date_str = datetime.utcnow().strftime("%Y%m%d")
    csv_path = os.path.join(temp_dir, f"prod_snapshot_{date_str}.csv")
    
    # Bash command equivalent:
    # psql $PROD_DB_CONNECTION -c "COPY (SELECT * FROM important_table) TO '$CSV_PATH' WITH CSV HEADER"
    context.log.info(f"Dumping production database to {csv_path}")
    context.log.info(f"Using DB connection: {prod_db['connection_string']}")
    
    os.makedirs(temp_dir, exist_ok=True)
    with open(csv_path, 'w') as f:
        f.write("id,name,value\n")
        f.write("1,test1,100\n")
        f.write("2,test2,200\n")
    
    context.log.info(f"CSV snapshot created successfully at {csv_path}")
    return Output(csv_path, output_name="csv_path")


@op(
    required_resource_keys={"dev_db", "filesystem"},
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"step": "load", "target": "dev"},
)
def copy_dev(context, csv_path):
    """
    Loads the CSV snapshot into the Development database environment.
    """
    dev_db = context.resources.dev_db
    context.log.info(f"Loading CSV into Development database from {csv_path}")
    context.log.info(f"Using DB connection: {dev_db['connection_string']}")
    
    # Bash command equivalent:
    # psql $DEV_DB_CONNECTION -c "COPY important_table FROM '$CSV_PATH' WITH CSV HEADER"
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    context.log.info("Development database loaded successfully")


@op(
    required_resource_keys={"staging_db", "filesystem"},
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"step": "load", "target": "staging"},
)
def copy_staging(context, csv_path):
    """
    Loads the CSV snapshot into the Staging database environment.
    """
    staging_db = context.resources.staging_db
    context.log.info(f"Loading CSV into Staging database from {csv_path}")
    context.log.info(f"Using DB connection: {staging_db['connection_string']}")
    
    # Bash command equivalent:
    # psql $STAGING_DB_CONNECTION -c "COPY important_table FROM '$CSV_PATH' WITH CSV HEADER"
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    context.log.info("Staging database loaded successfully")


@op(
    required_resource_keys={"qa_db", "filesystem"},
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"step": "load", "target": "qa"},
)
def copy_qa(context, csv_path):
    """
    Loads the CSV snapshot into the QA database environment.
    """
    qa_db = context.resources.qa_db
    context.log.info(f"Loading CSV into QA database from {csv_path}")
    context.log.info(f"Using DB connection: {qa_db['connection_string']}")
    
    # Bash command equivalent:
    # psql $QA_DB_CONNECTION -c "COPY important_table FROM '$CSV_PATH' WITH CSV HEADER"
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    context.log.info("QA database loaded successfully")


# Job definition with fan-out pattern
@job(
    resource_defs={
        "prod_db": prod_db_resource,
        "dev_db": dev_db_resource,
        "staging_db": staging_db_resource,
        "qa_db": qa_db_resource,
        "filesystem": filesystem_resource,
    },
    tags={
        "owner": "data_engineering",
        "category": "replication",
        "type": "database",
        "pattern": "fanout",
    },
)
def database_replication_pipeline():
    """
    Daily snapshot of production database distributed to three downstream environments.
    Follows a fan-out pattern with parallel execution.
    """
    csv_path = dump_prod_csv()
    
    # Fan-out: three parallel tasks that execute concurrently
    copy_dev(csv_path)
    copy_staging(csv_path)
    copy_qa(csv_path)


# Daily schedule definition
daily_replication_schedule = ScheduleDefinition(
    job=database_replication_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    catchup=False,  # Disable historical backfills
    tags={
        "owner": "data_engineering",
        "category": "replication",
    },
)


# Minimal launch pattern for testing
if __name__ == "__main__":
    result = database_replication_pipeline.execute_in_process(
        run_config={
            "resources": {
                "prod_db": {"config": {"connection_string": "postgresql://localhost:5432/prod"}},
                "dev_db": {"config": {"connection_string": "postgresql://localhost:5432/dev"}},
                "staging_db": {"config": {"connection_string": "postgresql://localhost:5432/staging"}},
                "qa_db": {"config": {"connection_string": "postgresql://localhost:5432/qa"}},
                "filesystem": {"config": {"temp_dir": "/tmp/dagster_replication"}},
            }
        }
    )
    
    if result.success:
        print("Pipeline execution succeeded!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")