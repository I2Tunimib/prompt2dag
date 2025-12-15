from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="global_dag",
    default_args=default_args,
    description=(
        "ETL pipeline processes French government death records and power plant data "
        "using a staged ETL pattern with mixed topology."
    ),
    schedule_interval=None,  # disabled schedule
    catchup=False,
    is_paused_upon_creation=True,
    tags=["etl"],
) as dag:
    # No actual processing tasks defined; placeholder to keep DAG valid
    start = DummyOperator(task_id="start")
    # No downstream dependencies because the pipeline pattern is empty.