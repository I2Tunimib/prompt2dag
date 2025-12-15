from datetime import datetime

from airflow import DAG

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

# DAG definition
with DAG(
    dag_id="unnamed_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval=None,  # Schedule disabled
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["empty"],
) as dag:
    # No tasks defined for this pipeline (empty pattern)
    pass