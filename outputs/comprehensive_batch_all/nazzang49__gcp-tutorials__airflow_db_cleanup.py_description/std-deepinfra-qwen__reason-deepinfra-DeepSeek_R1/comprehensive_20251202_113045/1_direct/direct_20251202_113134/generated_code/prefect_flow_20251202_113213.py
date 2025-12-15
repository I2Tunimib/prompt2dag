from prefect import flow, task
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel
from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

# Configuration
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 30
ENABLE_DELETE = True

@task
def print_configuration(max_db_entry_age_in_days: int = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS):
    """
    Initializes cleanup parameters and calculates the max_date threshold.
    """
    log = LoggingMixin().log
    log.info(f"Starting database cleanup with max_db_entry_age_in_days: {max_db_entry_age_in_days}")
    
    max_date = datetime.utcnow() - timedelta(days=max_db_entry_age_in_days)
    log.info(f"Max date threshold for data deletion: {max_date}")
    
    return max_date

@task
@provide_session
def cleanup_airflow_db(max_date: datetime, session=None):
    """
    Performs the actual database cleanup based on the max_date threshold.
    """
    log = LoggingMixin().log
    log.info(f"Cleaning up database entries older than: {max_date}")

    models = [DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel]
    for model in models:
        log.info(f"Processing model: {model.__name__}")
        
        query = session.query(model).filter(model.execution_date < max_date)
        if ENABLE_DELETE:
            query.delete(synchronize_session=False)
            session.commit()
            log.info(f"Deleted {query.count()} records from {model.__name__}")
        else:
            log.info(f"Dry run: Would have deleted {query.count()} records from {model.__name__}")

@flow
def airflow_db_cleanup_flow(max_db_entry_age_in_days: int = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS):
    """
    Orchestrates the Airflow database cleanup workflow.
    """
    max_date = print_configuration(max_db_entry_age_in_days)
    cleanup_airflow_db(max_date)

if __name__ == '__main__':
    # Optional: Configure deployment/schedule
    # Example: Deploy this flow to run daily at midnight UTC
    # from prefect.deployments import Deployment
    # from prefect.orion.schemas.schedules import CronSchedule
    # deployment = Deployment.build_from_flow(
    #     flow=airflow_db_cleanup_flow,
    #     name="Airflow DB Cleanup",
    #     schedule=CronSchedule(cron="0 0 * * *"),
    #     work_queue_name="default",
    # )
    # deployment.apply()

    # Run the flow locally for testing
    airflow_db_cleanup_flow()