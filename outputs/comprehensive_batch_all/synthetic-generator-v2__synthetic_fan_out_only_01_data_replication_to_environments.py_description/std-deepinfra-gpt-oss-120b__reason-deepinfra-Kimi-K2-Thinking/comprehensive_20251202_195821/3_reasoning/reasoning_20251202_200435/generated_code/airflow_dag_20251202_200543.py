from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='db_replication_daily_snapshot',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['replication', 'database', 'fanout'],
) as dag:
    """
    Daily database replication pipeline.
    Creates a CSV snapshot from production and distributes it to
    Development, Staging, and QA environments in a fan-out pattern.
    """

    dump_prod_csv = BashOperator(
        task_id='dump_prod_csv',
        bash_command=(
            "set -e && "
            "echo 'Creating production DB CSV snapshot...' && "
            "SNAPSHOT_PATH='/tmp/prod_snapshot_{{ ds_nodash }}.csv' && "
            "psql \"$PROD_DB_CONN\" -c \"\\COPY (SELECT * FROM production_table) "
            "TO '$SNAPSHOT_PATH' CSV HEADER;\" && "
            "echo \"Snapshot created at $SNAPSHOT_PATH\""
        ),
    )

    copy_dev = BashOperator(
        task_id='copy_dev',
        bash_command=(
            "set -e && "
            "echo 'Loading snapshot to Development DB...' && "
            "SNAPSHOT_PATH='/tmp/prod_snapshot_{{ ds_nodash }}.csv' && "
            "psql \"$DEV_DB_CONN\" -c \"\\COPY dev_table FROM '$SNAPSHOT_PATH' "
            "CSV HEADER;\" && "
            "echo 'Development DB copy complete'"
        ),
    )

    copy_staging = BashOperator(
        task_id='copy_staging',
        bash_command=(
            "set -e && "
            "echo 'Loading snapshot to Staging DB...' && "
            "SNAPSHOT_PATH='/tmp/prod_snapshot_{{ ds_nodash }}.csv' && "
            "psql \"$STAGING_DB_CONN\" -c \"\\COPY staging_table FROM "
            "'$SNAPSHOT_PATH' CSV HEADER;\" && "
            "echo 'Staging DB copy complete'"
        ),
    )

    copy_qa = BashOperator(
        task_id='copy_qa',
        bash_command=(
            "set -e && "
            "echo 'Loading snapshot to QA DB...' && "
            "SNAPSHOT_PATH='/tmp/prod_snapshot_{{ ds_nodash }}.csv' && "
            "psql \"$QA_DB_CONN\" -c \"\\COPY qa_table FROM '$SNAPSHOT_PATH' "
            "CSV HEADER;\" && "
            "echo 'QA DB copy complete'"
        ),
    )

    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]