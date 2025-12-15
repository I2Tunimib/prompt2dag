"""Database replication DAG that creates a daily CSV snapshot of the production
database and distributes it to Development, Staging, and QA environments.

The workflow follows a fan‑out pattern:
    dump_prod_csv → [copy_dev, copy_staging, copy_qa]
All copy tasks run in parallel and complete independently.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="db_replication_fanout",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["replication", "database", "fanout"],
    concurrency=3,
    max_active_tasks=3,
    doc_md=__doc__,
) as dag:
    # Create a CSV snapshot of the production database.
    dump_prod_csv = BashOperator(
        task_id="dump_prod_csv",
        bash_command=(
            "python /opt/scripts/dump_prod_to_csv.py "
            "--output /tmp/prod_snapshot_{{ ds_nodash }}.csv"
        ),
    )

    # Load the snapshot into the Development environment.
    copy_dev = BashOperator(
        task_id="copy_dev",
        bash_command=(
            "python /opt/scripts/load_csv_to_dev.py "
            "--input /tmp/prod_snapshot_{{ ds_nodash }}.csv"
        ),
    )

    # Load the snapshot into the Staging environment.
    copy_staging = BashOperator(
        task_id="copy_staging",
        bash_command=(
            "python /opt/scripts/load_csv_to_staging.py "
            "--input /tmp/prod_snapshot_{{ ds_nodash }}.csv"
        ),
    )

    # Load the snapshot into the QA environment.
    copy_qa = BashOperator(
        task_id="copy_qa",
        bash_command=(
            "python /opt/scripts/load_csv_to_qa.py "
            "--input /tmp/prod_snapshot_{{ ds_nodash }}.csv"
        ),
    )

    # Define fan‑out dependencies.
    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]