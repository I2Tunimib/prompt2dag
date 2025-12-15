from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='db_replication_pipeline',
    default_args=default_args,
    description='Daily production database snapshot replication to downstream environments',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['replication', 'database', 'fanout'],
) as dag:

    # Extract production data to dated CSV file in /tmp
    dump_prod_csv = BashOperator(
        task_id='dump_prod_csv',
        bash_command=(
            "psql -h prod-db.example.com -U prod_user -d production -c "
            "\"\\COPY (SELECT * FROM important_table) TO '/tmp/prod_snapshot_{{ ds_nodash }}.csv' WITH CSV HEADER;\""
        ),
    )

    # Load snapshot into Development database
    copy_dev = BashOperator(
        task_id='copy_dev',
        bash_command=(
            "psql -h dev-db.example.com -U dev_user -d development -c "
            "\"\\COPY important_table FROM '/tmp/prod_snapshot_{{ ds_nodash }}.csv' WITH CSV HEADER;\""
        ),
    )

    # Load snapshot into Staging database
    copy_staging = BashOperator(
        task_id='copy_staging',
        bash_command=(
            "psql -h staging-db.example.com -U staging_user -d staging -c "
            "\"\\COPY important_table FROM '/tmp/prod_snapshot_{{ ds_nodash }}.csv' WITH CSV HEADER;\""
        ),
    )

    # Load snapshot into QA database
    copy_qa = BashOperator(
        task_id='copy_qa',
        bash_command=(
            "psql -h qa-db.example.com -U qa_user -d qa -c "
            "\"\\COPY important_table FROM '/tmp/prod_snapshot_{{ ds_nodash }}.csv' WITH CSV HEADER;\""
        ),
    )

    # Fan-out pattern: dump task triggers all three copy tasks in parallel
    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]