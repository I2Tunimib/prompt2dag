from datetime import datetime, timedelta
from typing import List

from dagster import (
    Config,
    InitResourceContext,
    InputDefinition,
    Output,
    OutputDefinition,
    ResourceDefinition,
    op,
    job,
    get_dagster_logger,
)


class DBResource:
    """Stub resource representing a database connection."""

    def __init__(self, connection_string: str = "sqlite:///:memory:"):
        self.connection_string = connection_string

    def get_session(self):
        """Return a mock session object."""
        # In a real implementation this would return a SQLAlchemy session.
        return None


def db_resource_factory(init_context: InitResourceContext) -> DBResource:
    """Factory for the DBResource."""
    # Configuration could be passed via init_context.resource_config if needed.
    return DBResource()


class PrintConfig(Config):
    """Configuration for the print_configuration op."""

    retention_days: int = 30
    enable_delete: bool = False


class CleanupConfig(Config):
    """Configuration for the cleanup_airflow_db op."""

    enable_delete: bool = False


@op(
    out=OutputDefinition(datetime),
    config_schema=PrintConfig,
    description="Initializes cleanup parameters and computes the max_date threshold.",
)
def print_configuration(context) -> datetime:
    cfg: PrintConfig = context.op_config
    logger = get_dagster_logger()
    logger.info("Loading cleanup configuration.")
    logger.info("Retention period (days): %s", cfg.retention_days)
    logger.info("Enable delete: %s", cfg.enable_delete)

    max_date = datetime.utcnow() - timedelta(days=cfg.retention_days)
    logger.info("Computed max_date threshold: %s", max_date.isoformat())
    return Output(max_date)


@op(
    ins={"max_date": InputDefinition(datetime)},
    out=OutputDefinition(bool),
    config_schema=CleanupConfig,
    required_resource_keys={"db"},
    description="Performs database cleanup based on the provided max_date.",
)
def cleanup_airflow_db(context, max_date: datetime) -> bool:
    cfg: CleanupConfig = context.op_config
    logger = get_dagster_logger()
    logger.info("Starting cleanup with max_date: %s", max_date.isoformat())
    logger.info("Enable delete: %s", cfg.enable_delete)

    # List of tables/models to clean. In a real implementation these would be SQLAlchemy models.
    tables: List[str] = [
        "DagRun",
        "TaskInstance",
        "Log",
        "XCom",
        "SlaMiss",
        "DagModel",
        "TaskReschedule",
        "TaskFail",
        "RenderedTaskInstanceFields",
        "ImportError",
        "Job",
    ]

    # Retrieve a mock session from the DB resource.
    db_resource: DBResource = context.resources.db
    session = db_resource.get_session()

    for table_name in tables:
        # Placeholder for query construction.
        logger.info(
            "Would query %s for records older than %s", table_name, max_date.isoformat()
        )
        if cfg.enable_delete:
            logger.info("Would delete identified records from %s", table_name)
        else:
            logger.info("Deletion disabled; only reporting would-be deletions for %s", table_name)

    logger.info("Cleanup operation completed.")
    return Output(True)


@job(
    description="Linear job that computes a date threshold and cleans Airflow metadata tables.",
)
def airflow_cleanup_job():
    max_date = print_configuration()
    cleanup_airflow_db(max_date)


if __name__ == "__main__":
    result = airflow_cleanup_job.execute_in_process(
        run_config={
            "ops": {
                "print_configuration": {"config": {"retention_days": 30, "enable_delete": False}},
                "cleanup_airflow_db": {"config": {"enable_delete": False}},
            },
            "resources": {"db": {"config": {}}},
        },
        resources={"db": ResourceDefinition.resource_fn(db_resource_factory)},
    )
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")