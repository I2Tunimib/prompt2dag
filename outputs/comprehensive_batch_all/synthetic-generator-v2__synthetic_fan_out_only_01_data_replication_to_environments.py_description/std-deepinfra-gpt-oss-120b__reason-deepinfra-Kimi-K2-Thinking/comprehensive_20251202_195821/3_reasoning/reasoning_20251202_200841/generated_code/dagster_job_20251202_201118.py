from dagster import (
    op,
    job,
    ScheduleDefinition,
    DefaultScheduleStatus,
    RetryPolicy,
    Out,
    Output,
    MetadataValue,
    OpExecutionContext,
)
import subprocess
import datetime

# Minimal configuration stub for database connections
# In production, use Dagster resources to manage connections securely
# DB_CONNECTIONS = {
#     "prod": "postgresql://user:pass@prod-host:5432/prod_db",
#     "dev": "postgresql://user:pass@dev-host:5432/dev_db",
#     "staging": "postgresql://user:pass@staging-host:5432/staging_db",
#     "qa": "postgresql://user:pass@qa-host:5432/qa_db"
# }


@op(
    out=Out(str, description="Path to the generated CSV snapshot"),
    tags={"step": "extract"},
    retry_policy=RetryPolicy(max_retries=1, delay=300),  # 2 total attempts, 5 min delay
)
def dump_prod_csv_op(context: OpExecutionContext) -> str:
    """
    Creates a CSV snapshot from the production database.
    Generates a dated file in the temporary directory.
    """
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    csv_path = f"/tmp/prod_snapshot_{date_str}.csv"
    
    # Example command - replace with actual production DB dump command
    # Real command might use: psql, mysqldump, or other database client
    command = f"echo 'Simulating production DB dump to {csv_path}' && touch {csv_path}"
    
    context.log.info(f"Creating production DB snapshot at {csv_path}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        context.log.info(f"Dump command output: {result.stdout}")
        
        # Simulate CSV content creation
        with open(csv_path, 'w') as f:
            f.write("id,name,value\n")
            f.write("1,example,100\n")
        
        yield Output(
            csv_path,
            metadata={
                "snapshot_path": MetadataValue.path(csv_path),
                "snapshot_date": date_str,
                "row_count": 2,
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"Failed to dump production DB: {e.stderr}")
        raise


@op(
    tags={"step": "load", "target": "dev"},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def copy_dev_op(context: OpExecutionContext, csv_path: str) -> None:
    """
    Loads the CSV snapshot into the Development database environment.
    """
    context.log.info(f"Loading CSV snapshot from {csv_path} to Dev DB")
    
    # Example command - replace with actual dev DB load command
    # Real command might use: psql -c "COPY table FROM '{csv_path}' CSV HEADER"
    command = f"echo 'Loading {csv_path} to Development database'"
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        context.log.info(f"Dev copy command output: {result.stdout}")
        
        context.add_output_metadata(
            metadata={
                "target_environment": "development",
                "source_file": csv_path,
                "status": "success",
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"Failed to copy to Dev DB: {e.stderr}")
        raise


@op(
    tags={"step": "load", "target": "staging"},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def copy_staging_op(context: OpExecutionContext, csv_path: str) -> None:
    """
    Loads the CSV snapshot into the Staging database environment.
    """
    context.log.info(f"Loading CSV snapshot from {csv_path} to Staging DB")
    
    # Example command - replace with actual staging DB load command
    command = f"echo 'Loading {csv_path} to Staging database'"
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        context.log.info(f"Staging copy command output: {result.stdout}")
        
        context.add_output_metadata(
            metadata={
                "target_environment": "staging",
                "source_file": csv_path,
                "status": "success",
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"Failed to copy to Staging DB: {e.stderr}")
        raise


@op(
    tags={"step": "load", "target": "qa"},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def copy_qa_op(context: OpExecutionContext, csv_path: str) -> None:
    """
    Loads the CSV snapshot into the QA database environment.
    """
    context.log.info(f"Loading CSV snapshot from {csv_path} to QA DB")
    
    # Example command - replace with actual QA DB load command
    command = f"echo 'Loading {csv_path} to QA database'"
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        context.log.info(f"QA copy command output: {result.stdout}")
        
        context.add_output_metadata(
            metadata={
                "target_environment": "qa",
                "source_file": csv_path,
                "status": "success",
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"Failed to copy to QA DB: {e.stderr}")
        raise


@job(
    tags={
        "team": "data_engineering",
        "pipeline_type": "replication",
        "pattern": "fanout",
        "category": "database",
    },
    metadata={
        "owner": "data_engineering",
        "description": "Daily production database snapshot replication to downstream environments",
    }
)
def database_replication_job():
    """
    Daily snapshot of production database distributed to three downstream environments.
    Follows a fan-out pattern with parallel environment copies.
    Supports up to 3 concurrent tasks as specified.
    """
    # Execute dump first
    csv_path = dump_prod_csv_op()
    
    # Fan-out: three parallel copy tasks that execute concurrently
    # No synchronization points between them
    copy_dev_op(csv_path)
    copy_staging_op(csv_path)
    copy_qa_op(csv_path)


# Daily schedule with no catchup
# Catchup is disabled by default when using DefaultScheduleStatus.RUNNING
# and can be controlled via Dagster's scheduler configuration
database_replication_schedule = ScheduleDefinition(
    job=database_replication_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={
        "frequency": "daily",
        "catchup": "disabled",
    }
)


if __name__ == '__main__':
    # Execute the job in-process for testing and development
    result = database_replication_job.execute_in_process()
    print(f"Job execution {'succeeded' if result.success else 'failed'}")