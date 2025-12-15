from dagster import op, job, Config, OpExecutionContext, ResourceParam, Definitions
from datetime import datetime, timedelta
from typing import List, Dict, Any
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, declarative_base
import logging

# Set up logging
logger = logging.getLogger(__name__)

# SQLAlchemy base for Airflow models
Base = declarative_base()

# Simplified Airflow model definitions for cleanup
class DagRun(Base):
    __tablename__ = "dag_run"
    id = sa.Column(sa.Integer, primary_key=True)
    execution_date = sa.Column(sa.DateTime)
    dag_id = sa.Column(sa.String(250))

class TaskInstance(Base):
    __tablename__ = "task_instance"
    id = sa.Column(sa.Integer, primary_key=True)
    execution_date = sa.Column(sa.DateTime)
    dag_id = sa.Column(sa.String(250))
    task_id = sa.Column(sa.String(250))

class Log(Base):
    __tablename__ = "log"
    id = sa.Column(sa.Integer, primary_key=True)
    dttm = sa.Column(sa.DateTime)
    dag_id = sa.Column(sa.String(250))

class XCom(Base):
    __tablename__ = "xcom"
    id = sa.Column(sa.Integer, primary_key=True)
    execution_date = sa.Column(sa.DateTime)
    dag_id = sa.Column(sa.String(250))
    task_id = sa.Column(sa.String(250))

class SlaMiss(Base):
    __tablename__ = "sla_miss"
    id = sa.Column(sa.Integer, primary_key=True)
    execution_date = sa.Column(sa.DateTime)
    dag_id = sa.Column(sa.String(250))

class DagModel(Base):
    __tablename__ = "dag"
    id = sa.Column(sa.Integer, primary_key=True)
    last_parsed_time = sa.Column(sa.DateTime)
    dag_id = sa.Column(sa.String(250))

# Configuration schema
class AirflowDBCleanupConfig(Config):
    """Configuration for Airflow database cleanup job."""
    retention_days: int = 30
    enable_delete: bool = False
    database_url: str = "sqlite:///airflow.db"  # Default SQLite for demonstration

# Database resource
def airflow_db_resource(context):
    """Resource that provides a SQLAlchemy engine and session for Airflow metadata DB."""
    config = context.resource_config
    engine = sa.create_engine(config["database_url"])
    Base.metadata.create_all(engine)  # Create tables if they don't exist
    Session = sessionmaker(bind=engine)
    return {"engine": engine, "session": Session}

# Op 1: Print configuration and calculate max_date
@op
def print_configuration(
    context: OpExecutionContext, config: AirflowDBCleanupConfig
) -> Dict[str, Any]:
    """
    Initializes cleanup parameters and calculates the maximum date threshold.
    
    Returns:
        Dictionary containing max_date and configuration values
    """
    # Calculate max_date based on retention period
    max_date = datetime.utcnow() - timedelta(days=config.retention_days)
    
    context.log.info(f"Retention days: {config.retention_days}")
    context.log.info(f"Max date threshold: {max_date}")
    context.log.info(f"Enable delete: {config.enable_delete}")
    context.log.info(f"Database URL: {config.database_url}")
    
    return {
        "max_date": max_date,
        "retention_days": config.retention_days,
        "enable_delete": config.enable_delete,
    }

# Op 2: Cleanup Airflow database
@op
def cleanup_airflow_db(
    context: OpExecutionContext,
    config: AirflowDBCleanupConfig,
    db_resource: ResourceParam,
    cleanup_params: Dict[str, Any],
) -> None:
    """
    Performs database cleanup by deleting records older than max_date.
    
    Args:
        cleanup_params: Dictionary containing max_date and configuration from previous op
    """
    max_date = cleanup_params["max_date"]
    enable_delete = cleanup_params["enable_delete"]
    session = db_resource["session"]()
    
    # Define tables to clean up with their date columns and optional filters
    tables_to_cleanup = [
        {"model": DagRun, "date_column": "execution_date", "filter_by": None},
        {"model": TaskInstance, "date_column": "execution_date", "filter_by": None},
        {"model": Log, "date_column": "dttm", "filter_by": None},
        {"model": XCom, "date_column": "execution_date", "filter_by": None},
        {"model": SlaMiss, "date_column": "execution_date", "filter_by": None},
        {"model": DagModel, "date_column": "last_parsed_time", "filter_by": None},
    ]
    
    try:
        for table_config in tables_to_cleanup:
            model = table_config["model"]
            date_column = table_config["date_column"]
            filter_by = table_config["filter_by"]
            
            # Build query to find old records
            date_col = getattr(model, date_column)
            query = session.query(model).filter(date_col < max_date)
            
            # Apply additional filters if specified
            if filter_by:
                for key, value in filter_by.items():
                    query = query.filter(getattr(model, key) == value)
            
            # Count records to be deleted
            count = query.count()
            table_name = model.__tablename__
            
            context.log.info(f"Found {count} records to delete from {table_name}")
            
            # Perform deletion if enabled
            if enable_delete and count > 0:
                query.delete(synchronize_session=False)
                context.log.info(f"Deleted {count} records from {table_name}")
            else:
                context.log.info(f"Skipping deletion from {table_name} (enable_delete={enable_delete})")
        
        session.commit()
        context.log.info("Database cleanup completed successfully")
        
    except Exception as e:
        session.rollback()
        context.log.error(f"Error during database cleanup: {str(e)}")
        raise
    finally:
        session.close()

# Define the job
@job(
    description="Linear Airflow database maintenance workflow that removes old metadata entries",
    resource_defs={"db_resource": airflow_db_resource},
)
def airflow_db_cleanup_job():
    """
    Job that orchestrates Airflow database cleanup.
    Linear execution: print_configuration -> cleanup_airflow_db
    """
    cleanup_params = print_configuration()
    cleanup_airflow_db(cleanup_params)

# Launch pattern
if __name__ == "__main__":
    # Example execution with default config
    result = airflow_db_cleanup_job.execute_in_process(
        run_config={
            "resources": {
                "db_resource": {
                    "config": {
                        "database_url": "sqlite:///airflow.db"
                    }
                }
            },
            "ops": {
                "print_configuration": {
                    "config": {
                        "retention_days": 30,
                        "enable_delete": False,
                        "database_url": "sqlite:///airflow.db"
                    }
                },
                "cleanup_airflow_db": {
                    "config": {
                        "retention_days": 30,
                        "enable_delete": False,
                        "database_url": "sqlite:///airflow.db"
                    }
                }
            }
        }
    )
    
    if result.success:
        print("Job executed successfully!")
    else:
        print(f"Job failed: {result.output_for_node('cleanup_airflow_db')}")