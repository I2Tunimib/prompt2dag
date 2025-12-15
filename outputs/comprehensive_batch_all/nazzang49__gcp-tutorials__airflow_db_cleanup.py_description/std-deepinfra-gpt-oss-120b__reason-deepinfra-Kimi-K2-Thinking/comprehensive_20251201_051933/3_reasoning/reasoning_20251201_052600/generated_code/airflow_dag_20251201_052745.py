from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.settings import Session
from sqlalchemy import and_

# Configure logging
logger = logging.getLogger(__name__)

# Core models that should exist in all Airflow 2.x versions
from airflow.models import (
    DagRun,
    TaskInstance,
    Log,
    XCom,
    SlaMiss,
    DagModel,
)

# Optional models that may vary by Airflow version
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
    from airflow.models import ImportError as ImportErrorModel
    optional_models['ImportError'] = ImportErrorModel
except ImportError:
    pass

try:
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
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

# DAG definition
with DAG(
    dag_id='airflow_db_maintenance',
    default_args=default_args,
    description='Periodic cleanup of old metadata from Airflow MetaStore',
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['maintenance', 'database', 'cleanup'],
) as dag:
    
    def print_configuration(**context) -> str:
        """
        Load cleanup configuration and calculate max_date threshold.
        Pushes max_date and enable_delete to XCom for downstream tasks.
        """
        retention_days = int(Variable.get('max_db_entry_age_in_days', default_var=30))
        enable_delete = Variable.get('ENABLE_DELETE', default_var=False)
        
        if isinstance(enable_delete, str):
            enable_delete = enable_delete.lower() == 'true'
        
        max_date = datetime.utcnow() - timedelta(days=retention_days)
        
        logger.info(f"Retention period: {retention_days} days")
        logger.info(f"Max date threshold: {max_date}")
        logger.info(f"Enable delete: {enable_delete}")
        
        ti = context['ti']
        ti.xcom_push(key='max_date', value=max_date)
        ti.xcom_push(key='enable_delete', value=enable_delete)
        
        return f"Configuration loaded: max_date={max_date}, enable_delete={enable_delete}"
    
    def cleanup_airflow_db(**context) -> str:
        """
        Perform database cleanup by deleting old records from Airflow metadata tables.
        """
        ti = context['ti']
        max_date = ti.xcom_pull(task_ids='print_configuration', key='max_date')
        enable_delete = ti.xcom_pull(task_ids='print_configuration', key='enable_delete')
        
        if max_date is None:
            raise ValueError("max_date not found in XCom. Ensure print_configuration task ran successfully.")
        
        logger.info(f"Starting cleanup with max_date: {max_date}, enable_delete: {enable_delete}")
        
        tables_to_clean: List[Dict[str, Any]] = [
            {'model': DagRun, 'date_field': 'execution_date'},
            {'model': TaskInstance, 'date_field': 'execution_date'},
            {'model': Log, 'date_field': 'dttm'},
            {'model': XCom, 'date_field': 'execution_date'},
            {'model': SlaMiss, 'date_field': 'execution_date'},
            {'model': DagModel, 'date_field': 'last_parsed_time'},
        ]
        
        if 'TaskReschedule' in optional_models:
            tables_to_clean.append({'model': optional_models['TaskReschedule'], 'date_field': 'execution_date'})
        if 'TaskFail' in optional_models:
            tables_to_clean.append({'model': optional_models['TaskFail'], 'date_field': 'execution_date'})
        if 'RenderedTaskInstanceFields' in optional_models:
            tables_to_clean.append({'model': optional_models['RenderedTaskInstanceFields'], 'date_field': 'execution_date'})
        if 'ImportError' in optional_models:
            tables_to_clean.append({'model': optional_models['ImportError'], 'date_field': 'timestamp'})
        if 'Job' in optional_models:
            tables_to_clean.append({'model': optional_models['Job'], 'date_field': 'latest_heartbeat'})
        
        session = Session()
        deleted_counts = {}
        
        try:
            for table_config in tables_to_clean:
                model = table_config['model']
                date_field = table_config['date_field']
                table_name = getattr(model, '__tablename__', model.__name__)
                
                try:
                    if model == DagModel:
                        date_column = getattr(model, date_field)
                        query = session.query(model).filter(
                            and_(
                                date_column < max_date,
                                model.is_active == False
                            )
                        )
                    else:
                        date_column = getattr(model, date_field)
                        query = session.query(model).filter(date_column < max_date)
                    
                    count = query.count()
                    
                    if count == 0:
                        logger.info(f"No records to delete from {table_name}")
                        deleted_counts[table_name] = 0
                        continue
                    
                    logger.info(f"Found {count} records to delete from {table_name}")
                    
                    if enable_delete:
                        logger.info(f"Deleting {count} records from {table_name}...")
                        deleted_count = query.delete(synchronize_session=False)
                        session.commit()
                        logger.info(f"Successfully deleted {deleted_count} records from {table_name}")
                        deleted_counts[table_name] = deleted_count
                    else:
                        logger.info(f"ENABLE_DELETE is False. Would have deleted {count} records from {table_name}")
                        deleted_counts[table_name] = 0
                        
                except Exception as e:
                    logger.error(f"Error processing {table_name}: {str(e)}")
                    session.rollback()
                    deleted_counts[table_name] = -1
                    continue
                    
        finally:
            session.close()
        
        logger.info("Cleanup summary:")
        for table_name, count in deleted_counts.items():
            status = "ERROR" if count == -1 else f"{count} records deleted"
            logger.info(f"  {table_name}: {status}")
        
        return f"Database cleanup completed: {deleted_counts}"
    
    print_configuration_task = PythonOperator(
        task_id='print_configuration',
        python_callable=print_configuration,
    )
    
    cleanup_airflow_db_task = PythonOperator(
        task_id='cleanup_airflow_db',
        python_callable=cleanup_airflow_db,
    )
    
    print_configuration_task >> cleanup_airflow_db_task