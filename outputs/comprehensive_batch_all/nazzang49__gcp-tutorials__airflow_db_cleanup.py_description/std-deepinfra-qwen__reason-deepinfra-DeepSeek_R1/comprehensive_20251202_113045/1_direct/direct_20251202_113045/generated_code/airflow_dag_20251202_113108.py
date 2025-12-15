from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta, datetime
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email': ['admin@example.com']
}

# Define the DAG
with DAG(
    dag_id='airflow_db_maintenance',
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    catchup=False,
    default_args=default_args,
    description='Periodic cleanup of old metadata entries from Airflow MetaStore'
) as dag:

    def print_configuration(**kwargs):
        """
        Initializes cleanup parameters and calculates max_date for data deletion.
        """
        # Load configuration from DAG run variables or fallback to defaults
        max_db_entry_age_in_days = Variable.get('max_db_entry_age_in_days', default_var=30)
        enable_delete = Variable.get('enable_delete', default_var='False').lower() == 'true'

        # Calculate max_date for data deletion
        max_date = datetime.utcnow() - timedelta(days=int(max_db_entry_age_in_days))

        # Push max_date to XCom for downstream task consumption
        kwargs['ti'].xcom_push(key='max_date', value=max_date)
        kwargs['ti'].xcom_push(key='enable_delete', value=enable_delete)

        logging.info(f"Configuration: max_db_entry_age_in_days={max_db_entry_age_in_days}, enable_delete={enable_delete}, max_date={max_date}")

    def cleanup_airflow_db(**kwargs):
        """
        Performs the actual database cleanup based on the calculated max_date.
        """
        from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel
        from airflow.models import TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError, Job
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker

        # Retrieve max_date and enable_delete from XCom
        max_date = kwargs['ti'].xcom_pull(task_ids='print_configuration', key='max_date')
        enable_delete = kwargs['ti'].xcom_pull(task_ids='print_configuration', key='enable_delete')

        # Database session setup
        engine = create_engine(Variable.get('sql_alchemy_conn'))
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define database objects configuration
        db_objects = [
            (DagRun, 'execution_date'),
            (TaskInstance, 'execution_date'),
            (Log, 'dttm'),
            (XCom, 'timestamp'),
            (SlaMiss, 'execution_date'),
            (DagModel, 'last_scheduler_run'),
            (TaskReschedule, 'execution_date'),
            (TaskFail, 'execution_date'),
            (RenderedTaskInstanceFields, 'execution_date'),
            (ImportError, 'timestamp'),
            (Job, 'latest_heartbeat')
        ]

        # Perform cleanup for each database object
        for model, date_field in db_objects:
            query = session.query(model).filter(getattr(model, date_field) < max_date)
            if enable_delete:
                query.delete(synchronize_session=False)
                session.commit()
                logging.info(f"Deleted {query.count()} records from {model.__tablename__}")
            else:
                logging.info(f"Would delete {query.count()} records from {model.__tablename__}")

        session.close()

    # Define tasks
    print_config_task = PythonOperator(
        task_id='print_configuration',
        python_callable=print_configuration,
        provide_context=True
    )

    cleanup_db_task = PythonOperator(
        task_id='cleanup_airflow_db',
        python_callable=cleanup_airflow_db,
        provide_context=True
    )

    # Define task dependencies
    print_config_task >> cleanup_db_task