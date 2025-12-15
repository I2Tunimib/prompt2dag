from dagster import op, job, resource, Field, In, Out, execute_in_process
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from typing import Dict, Any

# Simplified resource for database connection
@resource(config_schema={"connection_string": Field(str, is_required=True)})
def db_resource(context):
    engine = create_engine(context.resource_config["connection_string"])
    return engine.connect()

# Simplified configuration for max_db_entry_age_in_days
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 30

@op(required_resource_keys={"db"}, out=Out(Dict[str, Any]))
def print_configuration(context):
    """
    Initializes cleanup parameters and calculates the max_date for data deletion.
    """
    max_db_entry_age_in_days = context.op_config.get("max_db_entry_age_in_days", DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS)
    max_date = datetime.utcnow() - timedelta(days=max_db_entry_age_in_days)
    context.log.info(f"Max date for deletion: {max_date}")
    return {"max_date": max_date}

@op(required_resource_keys={"db"}, ins={"config": In(Dict[str, Any])})
def cleanup_airflow_db(context, config):
    """
    Performs the actual database cleanup based on the max_date.
    """
    max_date = config["max_date"]
    enable_delete = context.op_config.get("enable_delete", True)
    tables = ["dag_run", "task_instance", "log", "xcom", "sla_miss", "dag_model"]

    for table in tables:
        query = text(f"DELETE FROM {table} WHERE execution_date < :max_date")
        if enable_delete:
            context.resources.db.execute(query, {"max_date": max_date})
            context.log.info(f"Deleted records from {table} older than {max_date}")
        else:
            context.log.info(f"Dry run: Would delete records from {table} older than {max_date}")

@job(resource_defs={"db": db_resource})
def airflow_db_cleanup_job():
    """
    Linear job for Airflow database maintenance.
    """
    config = print_configuration()
    cleanup_airflow_db(config)

if __name__ == "__main__":
    result = airflow_db_cleanup_job.execute_in_process(
        run_config={
            "ops": {
                "print_configuration": {
                    "config": {
                        "max_db_entry_age_in_days": 30
                    }
                },
                "cleanup_airflow_db": {
                    "config": {
                        "enable_delete": True
                    }
                }
            },
            "resources": {
                "db": {
                    "config": {
                        "connection_string": "sqlite:///airflow.db"
                    }
                }
            }
        }
    )