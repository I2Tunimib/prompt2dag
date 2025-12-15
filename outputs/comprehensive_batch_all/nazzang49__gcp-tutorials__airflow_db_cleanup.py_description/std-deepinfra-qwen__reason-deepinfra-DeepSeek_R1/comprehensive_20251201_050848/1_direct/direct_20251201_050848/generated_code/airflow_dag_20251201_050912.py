from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.models import Variable
from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel
from airflow.models import TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError, Job
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='airflow_db_cleanup',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    start_date=days_ago(1),
    catchup=False,
    tags=['maintenance', 'database']
) as dag:

    def print_configuration(**kwargs):
        """
        Initializes cleanup parameters and calculates the max_date for data deletion.
        """
        # Load configuration
        max_db_entry_age_in_days = Variable.get('max_db_entry_age_in_days', default_var=30)
        enable_delete = Variable.get('enable_delete', default_var='False').lower() == 'true'

        # Calculate max_date
        max_date = days_ago(int(max_db_entry_age_in_days))

        # Push max_date to XCom
        kwargs['ti'].xcom_push(key='max_date', value=max_date)
        kwargs['ti'].xcom_push(key='enable_delete', value=enable_delete)

        logging.info(f"Configuration: max_db_entry_age_in_days={max_db_entry_age_in_days}, enable_delete={enable_delete}, max_date={max_date}")

    def cleanup_airflow_db(**kwargs):
        """
        Performs the actual database cleanup based on the max_date and enable_delete settings.
        """
        # Retrieve max_date and enable_delete from XCom
        max_date = kwargs['ti'].xcom_pull(task_ids='print_configuration', key='max_date')
        enable_delete = kwargs['ti'].xcom_pull(task_ids='print_configuration', key='enable_delete')

        # Database session setup
        engine = create_engine('sqlite:///airflow.db')  # Replace with actual DB URI
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define database objects to clean
        models = [
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

        for model, date_field in models:
            # Build and execute SQL query
            query = session.query(model).filter(getattr(model, date_field) < max_date)
            if enable_delete:
                query.delete(synchronize_session=False)
                session.commit()
                logging.info(f"Deleted {query.count()} records from {model.__tablename__}")
            else:
                logging.info(f"Would delete {query.count()} records from {model.__tablename__} (dry run)")

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