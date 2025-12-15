"""Database replication DAG that creates a daily CSV snapshot and distributes it.

The DAG performs the following steps:
1. Dump the production database to a dated CSV file.
2. In parallel, load the CSV into Development, Staging, and QA environments.

The tasks are implemented with BashOperator commands that can be replaced with
real extraction / load commands.
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
    # Path for the daily CSV snapshot
    snapshot_path = "/tmp/prod_snapshot_{{ ds_nodash }}.csv"

    dump_prod_csv = BashOperator(
        task_id="dump_prod_csv",
        bash_command=(
            f"echo 'Dumping production DB to {snapshot_path}' && "
            f"touch {snapshot_path}"
        ),
    )

    copy_dev = BashOperator(
        task_id="copy_dev",
        bash_command=(
            f"echo 'Loading {snapshot_path} into Development DB' && "
            f"sleep 5"
        ),
    )

    copy_staging = BashOperator(
        task_id="copy_staging",
        bash_command=(
            f"echo 'Loading {snapshot_path} into Staging DB' && "
            f"sleep 5"
        ),
    )

    copy_qa = BashOperator(
        task_id="copy_qa",
        bash_command=(
            f"echo 'Loading {snapshot_path} into QA DB' && "
            f"sleep 5"
        ),
    )

    # Define fan‑out dependencies
    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]