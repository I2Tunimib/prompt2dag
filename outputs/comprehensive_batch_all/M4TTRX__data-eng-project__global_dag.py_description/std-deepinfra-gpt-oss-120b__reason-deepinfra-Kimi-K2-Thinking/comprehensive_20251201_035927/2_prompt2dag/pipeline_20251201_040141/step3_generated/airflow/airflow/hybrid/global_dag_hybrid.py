from airflow import DAG
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

# DAG definition
with DAG(
    dag_id="global_dag",
    default_args=default_args,
    description=(
        "ETL pipeline that processes French government death records and power plant data "
        "using a staged ETL pattern with fan‑out/fan‑in parallelism, conditional branching, "
        "and PostgreSQL loading."
    ),
    schedule_interval=None,  # Disabled schedule
    start_date=days_ago(1),
    catchup=False,
    tags=["etl"],
    is_paused_upon_creation=True,
) as dag:
    # No tasks defined (empty pattern)
    pass