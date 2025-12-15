from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'catchup': False,
}

# Define the DAG
with DAG(
    dag_id='database_replication_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    tags=['replication', 'database', 'fanout'],
) as dag:

    # Task to create a CSV snapshot from the production database
    dump_prod_csv = BashOperator(
        task_id='dump_prod_csv',
        bash_command='pg_dump -U user -h host -d prod_db -F c -b -v -f /tmp/prod_db_{{ ds_nodash }}.csv',
    )

    # Task to load the CSV snapshot into the Development database environment
    copy_dev = BashOperator(
        task_id='copy_dev',
        bash_command='psql -U user -h host -d dev_db -c "COPY table FROM \'/tmp/prod_db_{{ ds_nodash }}.csv\' WITH (FORMAT csv);"',
    )

    # Task to load the CSV snapshot into the Staging database environment
    copy_staging = BashOperator(
        task_id='copy_staging',
        bash_command='psql -U user -h host -d staging_db -c "COPY table FROM \'/tmp/prod_db_{{ ds_nodash }}.csv\' WITH (FORMAT csv);"',
    )

    # Task to load the CSV snapshot into the QA database environment
    copy_qa = BashOperator(
        task_id='copy_qa',
        bash_command='psql -U user -h host -d qa_db -c "COPY table FROM \'/tmp/prod_db_{{ ds_nodash }}.csv\' WITH (FORMAT csv);"',
    )

    # Define the task dependencies
    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]