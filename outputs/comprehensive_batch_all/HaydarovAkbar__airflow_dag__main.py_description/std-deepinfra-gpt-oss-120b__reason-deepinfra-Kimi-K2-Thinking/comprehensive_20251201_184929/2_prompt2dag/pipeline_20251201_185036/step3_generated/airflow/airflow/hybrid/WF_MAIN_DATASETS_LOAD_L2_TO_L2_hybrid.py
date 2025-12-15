from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="WF_MAIN_DATASETS_LOAD_L2_TO_L2",
    default_args=default_args,
    description="Comprehensive Pipeline Description",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["fanout"],
) as dag:

    # ---- Task Definitions (provided) ----
    wait_for_l2_full_load = DockerOperator(
        task_id="wait_for_l2_full_load",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    get_load_id = DockerOperator(
        task_id="get_load_id",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    workflow_registration = DockerOperator(
        task_id="workflow_registration",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    wait_for_success_end = DockerOperator(
        task_id="wait_for_success_end",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    run_sys_kill_all_session_pg = DockerOperator(
        task_id="run_sys_kill_all_session_pg",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    run_wf_data_preparation_for_reports = DockerOperator(
        task_id="run_wf_data_preparation_for_reports",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    load_ds_client_segmentation = DockerOperator(
        task_id="load_ds_client_segmentation",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    send_flg_to_sap = DockerOperator(
        task_id="send_flg_to_sap",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    email_on_failure = DockerOperator(
        task_id="email_on_failure",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    end = DockerOperator(
        task_id="end",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # ---- Fan‑out / Join Helpers ----
    join_parallel = DummyOperator(task_id="join_parallel")

    # ---- Dependencies ----
    wait_for_l2_full_load >> get_load_id
    get_load_id >> workflow_registration
    workflow_registration >> wait_for_success_end
    wait_for_success_end >> run_sys_kill_all_session_pg

    # Fan‑out
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, load_ds_client_segmentation]

    # Branch specific dependency
    load_ds_client_segmentation >> send_flg_to_sap

    # Join before final steps
    [run_wf_data_preparation_for_reports, send_flg_to_sap] >> join_parallel
    join_parallel >> end
    end >> email_on_failure