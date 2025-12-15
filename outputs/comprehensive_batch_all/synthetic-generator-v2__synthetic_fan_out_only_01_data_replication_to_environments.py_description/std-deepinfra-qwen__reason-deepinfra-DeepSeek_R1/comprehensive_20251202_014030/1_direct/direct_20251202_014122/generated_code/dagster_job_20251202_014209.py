from dagster import op, job, ResourceDefinition, execute_in_process, RetryPolicy, daily_schedule

# Simplified resource stubs
db_resource = ResourceDefinition.hardcoded_resource({"prod": "prod_db", "dev": "dev_db", "staging": "staging_db", "qa": "qa_db"})


@op(required_resource_keys={"db"})
def dump_prod_csv(context):
    """Creates a CSV snapshot from the production database."""
    prod_db = context.resources.db["prod"]
    csv_file = f"/tmp/prod_db_snapshot_{context.run_start_time.strftime('%Y-%m-%d')}.csv"
    context.log.info(f"Dumping production database to {csv_file}")
    # Simulate the dump process
    with open(csv_file, "w") as f:
        f.write("data")
    return csv_file


@op
def copy_dev(context, csv_file):
    """Loads the CSV snapshot into the Development database environment."""
    dev_db = context.resources.db["dev"]
    context.log.info(f"Copying {csv_file} to Development database {dev_db}")
    # Simulate the copy process
    context.log.info(f"Copy to Development completed")


@op
def copy_staging(context, csv_file):
    """Loads the CSV snapshot into the Staging database environment."""
    staging_db = context.resources.db["staging"]
    context.log.info(f"Copying {csv_file} to Staging database {staging_db}")
    # Simulate the copy process
    context.log.info(f"Copy to Staging completed")


@op
def copy_qa(context, csv_file):
    """Loads the CSV snapshot into the QA database environment."""
    qa_db = context.resources.db["qa"]
    context.log.info(f"Copying {csv_file} to QA database {qa_db}")
    # Simulate the copy process
    context.log.info(f"Copy to QA completed")


@job(
    resource_defs={"db": db_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "data_engineering", "category": "replication", "type": "database", "pattern": "fanout"}
)
def database_replication_job():
    csv_file = dump_prod_csv()
    copy_dev(csv_file)
    copy_staging(csv_file)
    copy_qa(csv_file)


@daily_schedule(
    pipeline_name="database_replication_job",
    start_date="2023-01-01",
    execution_time="00:00",
    execution_timezone="UTC",
    tags_fn=lambda _: {"owner": "data_engineering", "category": "replication", "type": "database", "pattern": "fanout"},
    solid_selection=None,
    mode="default",
    should_execute=None,
    environment_vars=None,
    partition_fn=None,
    execution_day_of_month=None,
    execution_day_of_week=None,
    execution_date_range=None,
    execution_date_format=None,
    end_date=None,
    cron_schedule="0 0 * * *",
    name="daily_database_replication",
    description="Daily database replication pipeline",
    tags={"owner": "data_engineering", "category": "replication", "type": "database", "pattern": "fanout"},
    default_status="RUNNING",
    solid_retry_policy=RetryPolicy(max_retries=2, delay=300),
    execution_timezone="UTC",
    execution_params=None,
    execution_timezone="UTC",
    execution_timezone="UTC",
)
def daily_database_replication(_date):
    return {}


if __name__ == "__main__":
    result = database_replication_job.execute_in_process()