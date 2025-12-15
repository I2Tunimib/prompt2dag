from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='database_replication_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['replication', 'database', 'fanout'],
) as dag:

    def create_dump_task():
        """Task to create a CSV snapshot from the production database."""
        return BashOperator(
            task_id='dump_prod_csv',
            bash_command='mkdir -p /tmp/db_snapshot && pg_dump -U user -d prod_db -F c -b -v -f /tmp/db_snapshot/prod_db_$(date +%%Y%%m%%d).csv'
        )

    def create_copy_task(env_name, target_db):
        """Task to load the CSV snapshot into a target database environment."""
        return BashOperator(
            task_id=f'copy_{env_name}',
            bash_command=f'pg_restore -U user -d {target_db} -v /tmp/db_snapshot/prod_db_$(date +%%Y%%m%%d).csv'
        )

    dump_prod_csv = create_dump_task()

    copy_dev = create_copy_task('dev', 'dev_db')
    copy_staging = create_copy_task('staging', 'staging_db')
    copy_qa = create_copy_task('qa', 'qa_db')

    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]