from dagster import op, job, Field, String, In, Out, execute_in_process

@op(
    config_schema={
        "output_path": Field(String, default_value="/tmp/prod_dump.csv", is_required=False)
    },
    out={"csv_file_path": Out(dagster_type=String)}
)
def dump_prod_csv(context):
    """
    Creates a CSV snapshot from the production database, generating a dated file in the temporary directory.
    """
    output_path = context.op_config["output_path"]
    # Simulate database dump to CSV
    context.log.info(f"Dumping production database to {output_path}")
    return output_path

@op(
    ins={"csv_file_path": In(dagster_type=String)},
    config_schema={
        "environment": Field(String, is_required=True)
    }
)
def copy_to_environment(context, csv_file_path):
    """
    Loads the CSV snapshot into the specified database environment.
    """
    environment = context.op_config["environment"]
    context.log.info(f"Copying {csv_file_path} to {environment} database")
    # Simulate loading CSV into the database
    return f"{csv_file_path} copied to {environment}"

@op(
    ins={"csv_file_path": In(dagster_type=String)}
)
def copy_dev(context, csv_file_path):
    """
    Loads the CSV snapshot into the Development database environment.
    """
    return copy_to_environment(context, csv_file_path)

@op(
    ins={"csv_file_path": In(dagster_type=String)}
)
def copy_staging(context, csv_file_path):
    """
    Loads the CSV snapshot into the Staging database environment.
    """
    return copy_to_environment(context, csv_file_path)

@op(
    ins={"csv_file_path": In(dagster_type=String)}
)
def copy_qa(context, csv_file_path):
    """
    Loads the CSV snapshot into the QA database environment.
    """
    return copy_to_environment(context, csv_file_path)

@job(
    description="Database replication pipeline that creates a daily snapshot of the production database and distributes it to three downstream environments.",
    tags={
        "owner": "data_engineering",
        "category": "replication",
        "type": "database",
        "pattern": "fanout"
    },
    config={
        "ops": {
            "copy_dev": {"config": {"environment": "Development"}},
            "copy_staging": {"config": {"environment": "Staging"}},
            "copy_qa": {"config": {"environment": "QA"}}
        }
    },
    tags={
        "owner": "data_engineering",
        "category": "replication",
        "type": "database",
        "pattern": "fanout"
    },
    executor_def={
        "config": {
            "max_concurrent": 3,
            "retries": {
                "enabled": {
                    "max_retries": 2,
                    "delay": 300
                }
            }
        }
    }
)
def database_replication_job():
    csv_file_path = dump_prod_csv()
    copy_dev(csv_file_path)
    copy_staging(csv_file_path)
    copy_qa(csv_file_path)

if __name__ == '__main__':
    result = database_replication_job.execute_in_process()