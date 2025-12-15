from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    resource,
)
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


# Mock Airflow database resource
@resource
def airflow_db_resource(_):
    """Mock resource simulating Airflow's metadata database connection."""
    return MockAirflowDb()


class MockAirflowDb:
    """Simulates Airflow's metadata database for demonstration purposes."""
    
    def __init__(self):
        self.logger = logging.getLogger("airflow.db.cleanup")
    
    def get_session(self):
        return MockSession(self.logger)
    
    def dispose(self):
        pass


class MockSession:
    def __init__(self, logger):
        self.logger = logger
    
    def query(self, model):
        return MockQuery(model, self.logger)
    
    def execute(self, statement):
        self.logger.info(f"Executing SQL: {statement}")
        return MockResult()
    
    def commit(self):
        self.logger.info("Committing database transaction")
    
    def close(self):
        pass


class MockQuery:
    def __init__(self, model, logger):
        self.model = model
        self.logger = logger
        self.filters = []
    
    def filter(self, condition):
        self.filters.append(str(condition))
        return self
    
    def delete(self, synchronize_session=False):
        self.logger.info(
            f"Deleting from {self.model.__name__} with filters: {self.filters}"
        )
        return 0
    
    def count(self):
        self.logger.info(
            f"Counting records in {self.model.__name__} with filters: {self.filters}"
        )
        return 0


class MockResult:
    def scalar(self):
        return 0


# Configuration schema
class CleanupConfig(Config):
    """Configuration for Airflow database cleanup."""
    
    max_db_entry_age_in_days: int = 30
    enable_delete: bool = False
    tables_to_cleanup: List[str] = [
        "dag_run",
        "task_instance",
        "log",
        "xcom",
        "sla_miss",
        "dag",
        "task_reschedule",
        "task_fail",
        "rendered_task_instance_fields",
        "import_error",
        "job",
    ]


# Simplified model representations
class DagRun:
    __name__ = "dag_run"
    execution_date = "execution_date"


class TaskInstance:
    __name__ = "task_instance"
    execution_date = "execution_date"


class Log:
    __name__ = "log"
    dttm = "dttm"


class XCom:
    __name__ = "xcom"
    execution_date = "execution_date"


class SlaMiss:
    __name__ = "sla_miss"
    execution_date = "execution_date"


class DagModel:
    __name__ = "dag"


class TaskReschedule:
    __name__ = "task_reschedule"
    execution_date = "execution_date"


class TaskFail:
    __name__ = "task_fail"
    execution_date = "execution_date"


class RenderedTaskInstanceFields:
    __name__ = "rendered_task_instance_fields"
    execution_date = "execution_date"


class ImportError:
    __name__ = "import_error"
    timestamp = "timestamp"


class Job:
    __name__ = "job"
    latest_heartbeat = "latest_heartbeat"


# Ops
@op
def print_configuration(
    context: OpExecutionContext, config: CleanupConfig
) -> Dict[str, Any]:
    """
    Initializes cleanup parameters and calculates the maximum date threshold.
    
    Returns:
        Dictionary containing cleanup parameters for downstream tasks.
    """
    context.log.info("Initializing Airflow DB cleanup configuration")
    
    # Calculate max_date based on retention period
    max_date = datetime.utcnow() - timedelta(days=config.max_db_entry_age_in_days)
    
    context.log.info(f"Retention period: {config.max_db_entry_age_in_days} days")
    context.log.info(f"Max date threshold: {max_date.isoformat()}")
    context.log.info(f"Enable delete: {config.enable_delete}")
    context.log.info(f"Tables to cleanup: {config.tables_to_cleanup}")
    
    return {
        "max_date": max_date,
        "enable_delete": config.enable_delete,
        "tables_to_cleanup": config.tables_to_cleanup,
    }


@op(
    required_resource_keys={"db"},
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def cleanup_airflow_db(
    context: OpExecutionContext, cleanup_params: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Performs the actual database cleanup based on the configuration.
    
    Args:
        cleanup_params: Dictionary containing max_date, enable_delete, and tables_to_cleanup.
    """
    context.log.info("Starting Airflow database cleanup")
    
    max_date = cleanup_params["max_date"]
    enable_delete = cleanup_params["enable_delete"]
    tables_to_cleanup = cleanup_params["tables_to_cleanup"]
    
    summary = {}
    
    # Map table names to model classes
    model_map = {
        "dag_run": DagRun,
        "task_instance": TaskInstance,
        "log": Log,
        "xcom": XCom,
        "sla_miss": SlaMiss,
        "dag": DagModel,
        "task_reschedule": TaskReschedule,
        "task_fail": TaskFail,
        "rendered_task_instance_fields": RenderedTaskInstanceFields,
        "import_error": ImportError,
        "job": Job,
    }
    
    db = context.resources.db
    session = db.get_session()
    
    try:
        for table_name in tables_to_cleanup:
            if table_name not in model_map:
                context.log.warning(f"Unknown table '{table_name}', skipping")
                summary[table_name] = {"status": "skipped", "reason": "unknown_table"}
                continue
            
            model = model_map[table_name]
            context.log.info(f"Processing table: {table_name}")
            
            try:
                # Build query based on model type
                query = session.query(model)
                
                # Apply date filter based on model's date column
                if table_name == "log":
                    query = query.filter(model.dttm < max_date)
                elif table_name == "import_error":
                    query = query.filter(model.timestamp < max_date)
                elif table_name == "job":
                    query = query.filter(model.latest_heartbeat < max_date)
                elif table_name == "dag":
                    # Special case for DagModel - requires different logic
                    context.log.info(
                        f"Skipping {table_name} - requires special handling"
                    )
                    summary[table_name] = {
                        "status": "skipped",
                        "reason": "special_handling_required",
                    }
                    continue
                else:
                    query = query.filter(model.execution_date < max_date)
                
                # Count records to be deleted
                record_count = query.count()
                context.log.info(
                    f"Found {record_count} records to delete from {table_name}"
                )
                
                if enable_delete:
                    if record_count > 0:
                        # Perform deletion
                        deleted_count = query.delete(synchronize_session=False)
                        context.log.info(
                            f"Deleted {deleted_count} records from {table_name}"
                        )
                        summary[table_name] = {
                            "status": "deleted",
                            "count": deleted_count,
                        }
                    else:
                        context.log.info(f"No records to delete from {table_name}")
                        summary[table_name] = {"status": "no_records", "count": 0}
                else:
                    context.log.info(
                        f"ENABLE_DELETE is False - dry run for {table_name}"
                    )
                    summary[table_name] = {
                        "status": "dry_run",
                        "count": record_count,
                    }
            
            except Exception as e:
                context.log.error(f"Error processing {table_name}: {str(e)}")
                summary[table_name] = {"status": "error", "error": str(e)}
                # Continue with other tables instead of failing entirely
        
        session.commit()
        context.log.info("Database cleanup completed successfully")
        
    except Exception as e:
        context.log.error(f"Database cleanup failed: {str(e)}")
        raise
    
    finally:
        session.close()
    
    return summary


# Job definition
@job(
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def airflow_db_cleanup_job():
    """
    Linear job that cleans up old metadata from Airflow's internal database.
    """
    cleanup_params = print_configuration()
    cleanup_airflow_db(cleanup_params)


# Schedule definition
airflow_db_cleanup_schedule = ScheduleDefinition(
    job=airflow_db_cleanup_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
)


# Definitions object to tie everything together
defs = Definitions(
    jobs=[airflow_db_cleanup_job],
    schedules=[airflow_db_cleanup_schedule],
    resources={
        "db": airflow_db_resource,
    },
)


# Launch pattern for local execution
if __name__ == "__main__":
    # Execute the job with default configuration
    result = airflow_db_cleanup_job.execute_in_process(
        run_config={
            "ops": {
                "print_configuration": {
                    "config": {
                        "max_db_entry_age_in_days": 30,
                        "enable_delete": False,
                    }
                }
            },
            "resources": {
                "db": {
                    "config": {}
                }
            }
        }
    )
    
    # Print the result summary
    if result.success:
        cleanup_result = result.output_for_node("cleanup_airflow_db")
        print(f"\nCleanup Summary: {cleanup_result}")
    else:
        print(f"\nJob failed: {result.failure_data}")