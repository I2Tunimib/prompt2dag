from prefect import flow, task, get_run_logger
from prefect.variables import Variable
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import os

# Table configuration for Airflow metadata cleanup
# In production, import actual models from airflow.models instead
TABLE_CONFIG = [
    {"name": "dag_run", "date_column": "execution_date"},
    {"name": "task_instance", "date_column": "execution_date"},
    {"name": "log", "date_column": "dttm"},
    {"name": "xcom", "date_column": "execution_date"},
    {"name": "sla_miss", "date_column": "execution_date"},
    {"name": "task_reschedule", "date_column": "execution_date"},
    {"name": "task_fail", "date_column": "execution_date"},
    {"name": "rendered_task_instance_fields", "date_column": "execution_date"},
    {"name": "import_error", "date_column": "timestamp"},
]


@task(
    name="initialize-cleanup-config",
    retries=1,
    retry_delay_seconds=60,
    description="Load configuration and calculate max_date threshold"
)
def initialize_cleanup_config(
    retention_days: Optional[int] = None,
    enable_delete: Optional[bool] = None
) -> Dict[str, Any]:
    """
    Initialize cleanup configuration with retention period and cutoff date.
    """
    logger = get_run_logger()
    
    # Load configuration from Prefect variables or use safe defaults
    if retention_days is None:
        try:
            retention_days = int(Variable.get("max_db_entry_age_in_days", default="30"))
        except Exception:
            retention_days = 30
    
    if enable_delete is None:
        try:
            enable_delete = Variable.get("enable_delete", default="false").lower() == "true"
        except Exception:
            enable_delete = False
    
    # Calculate cutoff date (UTC)
    max_date = datetime.utcnow() - timedelta(days=retention_days)
    
    config = {
        "retention_days": retention_days,
        "enable_delete": enable_delete,
        "max_date": max_date,
    }
    
    logger.info(
        f"Configuration: retention_days={retention_days}, enable_delete={enable_delete}, "
        f"max_date={max_date.isoformat()}"
    )
    
    return config


@task(
    name="cleanup-database-tables",
    retries=1,
    retry_delay_seconds=60,
    description="Clean old records from Airflow metadata tables"
)
def cleanup_database_tables(config: Dict[str, Any]) -> Dict[str, int]:
    """
    Execute cleanup queries on Airflow metadata database.
    
    Returns statistics of deleted records per table.
    """
    logger = get_run_logger()
    
    # Get database connection string from environment
    db_url = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    if not db_url:
        raise ValueError("AIRFLOW__CORE__SQL_ALCHEMY_CONN environment variable must be set")
    
    # Create database engine and session
    engine = create_engine(db_url, pool_pre_ping=True, echo=False)
    Session = sessionmaker(bind=engine)
    
    stats = {}
    max_date = config["max_date"]
    enable_delete = config["enable_delete"]
    
    with Session() as session:
        for table_info in TABLE_CONFIG:
            table_name = table_info["name"]
            date_column = table_info["date_column"]
            
            try:
                # Count records to be deleted
                count_query = text(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE {date_column} < :max_date
                """)
                count = session.execute(count_query, {"max_date": max_date}).scalar()
                
                logger.info(f"Table {table_name}: {count} records older than {max_date}")
                
                if count > 0:
                    if enable_delete:
                        # Perform deletion
                        delete_query = text(f"""
                            DELETE FROM {table_name}
                            WHERE {date_column} < :max_date
                        """)
                        result = session.execute(delete_query, {"max_date": max_date})
                        session.commit()
                        deleted = result.rowcount
                        logger.info(f"Deleted {deleted} records from {table_name}")
                        stats[table_name] = deleted
                    else:
                        logger.info(f"ENABLE_DELETE=False - would delete {count} from {table_name}")
                        stats[table_name] = 0
                else:
                    stats[table_name] = 0
                    
            except Exception as e:
                logger.error(f"Error processing {table_name}: {e}")
                stats[table_name] = -1
                session.rollback()
                # Continue with next table instead of failing flow
    
    logger.info(f"Cleanup completed. Statistics: {stats}")
    return stats


@flow(
    name="airflow-db-maintenance",
    description="Daily Airflow metadata database maintenance"
)
def airflow_db_maintenance_flow(
    retention_days: Optional[int] = None,
    enable_delete: Optional[bool] = None
) -> Dict[str, int]:
    """
    Main flow for daily Airflow database maintenance.
    
    This flow cleans up old metadata records to prevent database bloat.
    For scheduled execution, deploy with Prefect server/cloud.
    """
    logger = get_run_logger()
    logger.info("Starting Airflow DB maintenance flow")
    
    # Execute tasks sequentially
    config = initialize_cleanup_config(retention_days, enable_delete)
    stats = cleanup_database_tables(config)
    
    logger.info(f"Flow completed. Statistics: {stats}")
    return stats


if __name__ == "__main__":
    # Local execution for testing
    # For production scheduling, deploy to Prefect:
    # prefect deploy airflow_db_maintenance_flow.py:airflow_db_maintenance_flow \
    #   --name "daily-airflow-db-maintenance" \
    #   --cron "0 0 * * *" \
    #   --timezone "UTC" \
    #   --description "Daily Airflow metadata cleanup"
    #
    # Email notifications can be configured via Prefect Automations
    # in Prefect Cloud or through custom task callbacks
    
    airflow_db_maintenance_flow()