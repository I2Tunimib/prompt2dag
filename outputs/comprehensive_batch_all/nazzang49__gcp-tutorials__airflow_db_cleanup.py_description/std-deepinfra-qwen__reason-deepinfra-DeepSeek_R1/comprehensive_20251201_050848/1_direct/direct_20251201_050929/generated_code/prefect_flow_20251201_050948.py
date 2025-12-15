from prefect import flow, task
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel
from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# Configuration
ENABLE_DELETE = True
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 30


@task
def print_configuration(max_db_entry_age_in_days: int = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS):
    """
    Initializes cleanup parameters and calculates the maximum date threshold for data deletion.
    """
    max_date = datetime.utcnow() - timedelta(days=max_db_entry_age_in_days)
    log.info(f"Max date for deletion: {max_date}")
    return max_date


@task
@provide_session
def cleanup_airflow_db(max_date: datetime, session: Session = None):
    """
    Performs the actual database cleanup by deleting records older than the max_date.
    """
    models = [DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel]
    for model in models:
        query = session.query(model).filter(model.execution_date < max_date)
        log.info(f"Deleting {model.__name__} records older than {max_date}")
        if ENABLE_DELETE:
            query.delete(synchronize_session=False)
            session.commit()
        else:
            log.info(f"Dry run: {model.__name__} records older than {max_date} would be deleted")
    session.close()


@flow(name="Airflow Metadata Database Cleanup")
def airflow_db_cleanup_flow():
    """
    Orchestrates the Airflow metadata database cleanup process.
    """
    max_date = print_configuration()
    cleanup_airflow_db(max_date)


if __name__ == '__main__':
    airflow_db_cleanup_flow()

# Optional: Deployment/schedule configuration
# Deployment configuration can be added using Prefect's deployment API or through the Prefect UI.
# Example:
# from prefect.deployments import Deployment
# from prefect.orion.schemas.schedules import CronSchedule
# deployment = Deployment.build_from_flow(
#     flow=airflow_db_cleanup_flow,
#     name="daily-cleanup",
#     schedule=CronSchedule(cron="0 0 * * *", timezone="UTC"),
#     work_queue_name="default",
#     parameters={},
#     tags=["airflow", "cleanup"]
# )
# deployment.apply()