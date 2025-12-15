"""Database replication DAG that creates a daily CSV snapshot of the production
database and distributes it to Development, Staging, and QA environments.

The workflow follows a fan‑out pattern:
    dump_prod_csv → [copy_dev, copy_staging, copy_qa]

Each copy task runs independently after the dump completes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# DAG definition
with DAG(
    dag_id="db_replication_fanout",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["replication", "database", "fanout"],
    concurrency=3,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    # Create a CSV snapshot from the production database.
    # Expected environment variables:
    #   PROD_DB_URL – connection string for the production database
    dump_prod_csv = BashOperator(
        task_id="dump_prod_csv",
        bash_command=(
            "psql $PROD_DB_URL -c \"\\copy (SELECT * FROM my_table) TO "
            "'/tmp/prod_snapshot_{{ ds_nodash }}.csv' CSV HEADER\""
        ),
    )

    # Load the snapshot into the Development environment.
    # Expected environment variable: DEV_DB_URL
    copy_dev = BashOperator(
        task_id="copy_dev",
        bash_command=(
            "psql $DEV_DB_URL -c \"\\copy my_table FROM "
            "'/tmp/prod_snapshot_{{ ds_nodash }}.csv' CSV HEADER\""
        ),
    )

    # Load the snapshot into the Staging environment.
    # Expected environment variable: STAGING_DB_URL
    copy_staging = BashOperator(
        task_id="copy_staging",
        bash_command=(
            "psql $STAGING_DB_URL -c \"\\copy my_table FROM "
            "'/tmp/prod_snapshot_{{ ds_nodash }}.csv' CSV HEADER\""
        ),
    )

    # Load the snapshot into the QA environment.
    # Expected environment variable: QA_DB_URL
    copy_qa = BashOperator(
        task_id="copy_qa",
        bash_command=(
            "psql $QA_DB_URL -c \"\\copy my_table FROM "
            "'/tmp/prod_snapshot_{{ ds_nodash }}.csv' CSV HEADER\""
        ),
    )

    # Define fan‑out dependencies
    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]