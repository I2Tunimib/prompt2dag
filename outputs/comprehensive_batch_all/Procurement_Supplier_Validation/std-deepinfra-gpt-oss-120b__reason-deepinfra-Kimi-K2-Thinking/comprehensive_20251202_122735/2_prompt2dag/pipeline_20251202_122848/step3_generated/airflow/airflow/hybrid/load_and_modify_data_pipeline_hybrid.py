from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_and_modify_data_pipeline",
    default_args=default_args,
    description="No description provided.",
    schedule_interval=None,
    catchup=False,
    tags=["sequential"],
    is_paused_upon_creation=True,
) as dag:

    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
            "DATA_DIR": "${DATA_DIR}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    reconcile_supplier_names = DockerOperator(
        task_id='reconcile_supplier_names',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={
            "PRIMARY_COLUMN": "supplier_name",
            "RECONCILIATOR_ID": "wikidataEntity",
            "DATASET_ID": "2",
            "DATA_DIR": "${DATA_DIR}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_validated_data = DockerOperator(
        task_id='save_validated_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            "DATASET_ID": "2",
            "DATA_DIR": "${DATA_DIR}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set sequential dependencies
    load_and_modify_data >> reconcile_supplier_names >> save_validated_data