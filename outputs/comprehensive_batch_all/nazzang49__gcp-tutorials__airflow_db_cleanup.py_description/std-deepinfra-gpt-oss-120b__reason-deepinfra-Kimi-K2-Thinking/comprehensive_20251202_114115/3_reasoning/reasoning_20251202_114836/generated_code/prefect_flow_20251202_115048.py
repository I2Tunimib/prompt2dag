from prefect import flow, task
from datetime import datetime, timedelta
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Query
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv(
    "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
    "postgresql://airflow:airflow@localhost:5432/airflow"
)

# Import Airflow models
try:
    from airflow.models import (
        DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel,
        TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError
    )
    from airflow.jobs.base_job import BaseJob
    AIRFLOW_MODELS_AVAILABLE = True
except ImportError:
    logger.warning("Airflow models not available. Install apache-airflow to enable cleanup.")
    AIRFLOW_MODELS_AVAILABLE = False

# Default configuration
DEFAULT_RETENTION_DAYS = 30
DEFAULT_ENABLE_DELETE = False


@task(retries=1, retry_delay_seconds=60)
def print_configuration(retention_days: int, enable_delete: bool) -> Dict[str, Any]:
    """
    Initialize cleanup parameters and calculate max_date threshold.
    
    Args:
        retention_days: Number of days to retain data
        enable_delete: Whether to actually perform deletions
        
    Returns:
        Configuration dictionary with max_date, retention_days, and enable_delete
    """
    logger.info("Initializing Airflow DB cleanup configuration")
    
    # Calculate max_date threshold
    max_date = datetime.utcnow() - timedelta(days=retention_days)
    
    logger.info(f"Retention period: {retention_days} days")
    logger.info(f"Max date threshold: {max_date.isoformat()}")
    logger.info(f"Enable delete: {enable_delete}")
    
    return {
        "max_date": max_date,
        "retention_days": retention_days,
        "enable_delete": enable_delete
    }


@task(retries=1, retry_delay_seconds=60)
def cleanup_airflow_db(config: Dict[str, Any]) -> None:
    """
    Perform database cleanup based on the provided configuration.
    
    Args:
        config: Configuration dictionary from print_configuration task
    """
    if not AIRFLOW_MODELS_AVAILABLE:
        raise RuntimeError(
            "Airflow models are not available. "
            "Install apache-airflow to perform cleanup."
        )
    
    max_date = config["max_date"]
    enable_delete = config["enable_delete"]
    
    logger.info(
        f"Starting Airflow DB cleanup for records older than {max_date.isoformat()}"
    )
    
    # Define cleanup configuration for each table
    cleanup_configs: List[Dict[str, Any]] = [
        {"model": DagRun, "date_column": "execution_date"},
        {"model": TaskInstance, "date_column": "execution_date"},
        {"model": Log, "date_column": "dttm"},
        {"model": XCom, "date_column": "execution_date"},
        {"model": SlaMiss, "date_column": "execution_date"},
        {"model": DagModel, "date_column": "last_parsed_time"},
    ]
    
    # Optional tables (version-specific)
    optional_models = [
        (TaskReschedule, "execution_date"),
        (TaskFail, "execution_date"),
        (RenderedTaskInstanceFields, "execution_date"),
        (ImportError, "timestamp"),
        (BaseJob, "latest_heartbeat"),
    ]
    
    # Add optional tables if they exist
    for model, date_column in optional_models:
        try:
            # Check if model is available
            _ = model.__tablename__
            cleanup_configs.append({"model": model, "date_column": date_column})
        except Exception:
            logger.debug(f"Optional model {model.__name__} not available, skipping")
    
    # Perform cleanup
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    
    total_deleted = 0
    
    with Session() as session:
        for table_config in cleanup_configs:
            model = table_config["model"]
            date_column = table_config["date_column"]
            
            try:
                # Get the date attribute
                date_attr = getattr(model, date_column)
                
                # Query for old records
                query: Query = session.query(model).filter(date_attr < max_date)
                
                # Count records to delete
                count = query.count()
                
                if count == 0:
                    logger.debug(f"No old records found in {model