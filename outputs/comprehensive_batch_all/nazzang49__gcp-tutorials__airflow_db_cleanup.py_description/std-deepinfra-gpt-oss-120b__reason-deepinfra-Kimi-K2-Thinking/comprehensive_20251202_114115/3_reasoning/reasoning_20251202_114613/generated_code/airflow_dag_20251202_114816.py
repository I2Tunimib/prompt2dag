from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.settings import Session
from airflow.version import version as airflow_version

# Model imports with version handling
try:
    from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel
except ImportError:
    # Fallback for older versions
    from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel

# Optional models
optional_models = {}
try:
    from airflow.models import TaskReschedule
    optional_models['TaskReschedule'] = TaskReschedule
except ImportError:
    pass

try:
    from airflow.models import TaskFail
    optional_models['TaskFail'] = TaskFail
except ImportError:
    pass

try:
    from airflow.models import RenderedTaskInstanceFields
    optional_models['RenderedTaskInstanceFields'] = RenderedTaskInstanceFields
except ImportError:
    pass

try:
    from airflow.models import ImportError
    optional_models['ImportError'] = ImportError
except ImportError:
    pass

try:
    # Job model name changed in different versions
    from airflow.models import Job
    optional_models['Job'] = Job
except ImportError:
    try:
        from airflow.jobs.base_job import BaseJob
        optional_models['Job'] = BaseJob
    except ImportError:
        pass

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
with DAG(
    dag_id='airflow_db_cleanup',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['maintenance', 'database'],
    doc_md="""# Airflow DB Cleanup DAG
    This DAG performs periodic cleanup of old metadata entries from Airflow's internal database.
    """
) as dag:

    def print_configuration(**kwargs):
        """Load configuration and calculate max_date threshold."""
        ti = kwargs['ti']
        
        # Load configuration from Airflow Variables
        retention_days = Variable.get(
            "max_db_entry_age_in_days", 
            default_var=30
        )
        enable_delete = Variable.get(
            "enable_delete", 
            default_var=False
        )
        
        # Calculate max_date threshold
        max_date = datetime.utcnow() - timedelta(days=int(retention_days))
        
        # Log configuration
        logging.info(f"Retention days: {retention_days}")
        logging.info(f"Max date threshold: {max_date}")
        logging.info(f"Enable delete: {enable_delete}")
        
        # Push to XCom for downstream tasks
        ti.xcom_push(key='max_date', value=max_date.isoformat())
        ti.xcom_push(key='enable_delete', value=enable_delete)
        ti.xcom_push(key='retention_days', value=retention_days)
        
        return "Configuration loaded successfully"

    def cleanup_airflow_db(**kwargs):
        """Perform database cleanup based on configuration."""
        ti = kwargs['ti']
        
        # Pull configuration from XCom
        max_date_str = ti.xcom_pull(task_ids='print_configuration', key='max_date')
        enable_delete = ti.xcom_pull(task_ids='print_configuration', key='enable_delete')
        
        if not max_date_str:
            raise ValueError("max_date not found in XCom. Ensure print_configuration ran successfully.")
        
        max_date = datetime.fromisoformat(max_date_str)
        
        logging.info(f"Starting cleanup of records older than {max_date}")
        logging.info(f"Delete mode enabled: {enable_delete}")
        
        # Define models to clean up
        # Core models that always exist
        models_to_clean = [
            {'model': DagRun, 'date_column': 'execution_date'},
            {'model': TaskInstance, 'date_column': 'execution_date'},
            {'model': Log, 'date_column': 'dttm'},
            {'model': XCom, 'date_column': 'execution_date'},
            {'model': SlaMiss, 'date_column': 'execution_date'},
            {'model': DagModel, 'date_column': 'last_parsed_time'},
        ]
        
        # Add optional models if they exist
        if 'TaskReschedule' in optional_models:
            models_to_clean.append({
                'model': optional_models['TaskReschedule'], 
                'date_column': 'execution_date'
            })
        
        if 'TaskFail' in optional_models:
            models_to_clean.append({
                'model': optional_models['TaskFail'], 
                'date_column': 'execution_date'
            })
        
        if 'RenderedTaskInstanceFields' in optional_models:
            models_to_clean.append({
                'model': optional_models['RenderedTaskInstanceFields'], 
                'date_column': 'execution_date'
            })
        
        if 'ImportError' in optional_models:
            models_to_clean.append({
                'model': optional_models['ImportError'], 
                'date_column': 'timestamp'
            })
        
        if 'Job' in optional_models:
            models_to_clean.append({
                'model': optional_models['Job'], 
                'date_column': 'latest_heartbeat'
            })
        
        session = Session()
        
        try:
            for model_config in models_to_clean:
                model = model_config['model']
                date_column = model_config['date_column']
                
                # Build query
                query = session.query(model)
                
                # Apply date filter based on column type
                if hasattr(model, date_column):
                    date_attr = getattr(model, date_column)
                    # Handle different date column types
                    if date_column == 'dttm':
                        # Log table uses 'dttm'
                        records = query.filter(date_attr < max_date).all()
                    elif date_column == 'timestamp':
                        # ImportError uses 'timestamp'
                        records = query.filter(date_attr < max_date).all()
                    elif date_column == 'latest_heartbeat':
                        # Job uses 'latest_heartbeat'
                        records = query.filter(date_attr < max_date).all()
                    elif date_column == 'last_parsed_time':
                        # DagModel uses 'last_parsed_time'
                        records = query.filter(date_attr < max_date).all()
                    else:
                        # Default for execution_date and others
                        records = query.filter(date_attr < max_date).all()
                    
                    record_count = len(records)
                    
                    if record_count > 0:
                        logging.info(
                            f"Found {record_count} old records in {model.__tablename__}"
                        )
                        
                        if enable_delete:
                            # Perform deletion
                            query.delete(synchronize_session=False)
                            session.commit()
                            logging.info(
                                f"Deleted {record_count} records from {model.__tablename__}"
                            )
                        else:
                            logging.info(
                                f"DRY RUN: Would delete {record_count} records from {model.__tablename__}"
                            )
                    else:
                        logging.info(f"No old records found in {model.__tablename__}")
                else:
                    logging.warning(
                        f"Model {model.__name__} does not have {date_column} column, skipping"
                    )
            
            logging.info("Database cleanup completed successfully")
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error during database cleanup: {str(e)}")
            raise
        finally:
            session.close()

    # Define tasks
    print_configuration_task = PythonOperator(
        task_id='print_configuration',
        python_callable=print_configuration,
        provide_context=True,
    )

    cleanup_airflow_db_task = PythonOperator(
        task_id='cleanup_airflow_db',
        python_callable=cleanup_airflow_db,
        provide_context=True,
    )

    # Define dependencies
    print_configuration_task >> cleanup_airflow_db_task