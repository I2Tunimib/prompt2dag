import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import BranchPythonOperator


def decide_path(**context):
    """
    Placeholder branching logic.
    In a real scenario, this would inspect the result of `quality_check`
    (e.g., via XCom) and decide whether to proceed to production_load
    or quarantine_and_alert.
    """
    # Example: always go to production_load; replace with real logic as needed.
    return "production_load"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="data_quality_gate",
    description="Comprehensive Pipeline Description",
    schedule_interval="@daily",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fanout_fanin"],
) as dag:

    ingest_csv = DockerOperator(
        task_id='ingest_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quality_check = DockerOperator(
        task_id='quality_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quality_check_branch = BranchPythonOperator(
        task_id='quality_check_branch',
        python_callable=decide_path,
        provide_context=True,
    )

    production_load = DockerOperator(
        task_id='production_load',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quarantine_and_alert = DockerOperator(
        task_id='quarantine_and_alert',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_alert_email = DockerOperator(
        task_id='send_alert_email',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanup = DockerOperator(
        task_id='cleanup',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies (fanout_fanin pattern)
    ingest_csv >> quality_check >> quality_check_branch
    quality_check_branch >> [production_load, quarantine_and_alert]
    quarantine_and_alert >> send_alert_email
    [production_load, send_alert_email] >> cleanup