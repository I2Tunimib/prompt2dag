import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Retrieve the shared data directory from the environment or use a fallback
DATA_DIR_HOST = os.getenv("DATA_DIR", "/tmp/data")
DATA_DIR_CONTAINER = "/data"

# Common Docker settings
DOCKER_NETWORK = "app_network"
MOUNTS = [Mount(source=DATA_DIR_HOST, target=DATA_DIR_CONTAINER, type="bind")]

with DAG(
    dag_id="procurement_supplier_validation",
    default_args=default_args,
    description="Validate and standardize supplier data using Dockerized services",
    schedule_interval=None,
    catchup=False,
    tags=["procurement", "validation", "docker"],
) as dag:

    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        command=[
            "--input",
            f"{DATA_DIR_CONTAINER}/suppliers.csv",
            "--output",
            f"{DATA_DIR_CONTAINER}/table_data_2.json",
        ],
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
        },
        network_mode=DOCKER_NETWORK,
        mounts=MOUNTS,
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    entity_reconciliation = DockerOperator(
        task_id="entity_reconciliation",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        command=[
            "--input",
            f"{DATA_DIR_CONTAINER}/table_data_2.json",
            "--output",
            f"{DATA_DIR_CONTAINER}/reconciled_table_2.json",
        ],
        environment={
            "PRIMARY_COLUMN": "supplier_name",
            "RECONCILIATOR_ID": "wikidataEntity",
            "DATASET_ID": "2",
        },
        network_mode=DOCKER_NETWORK,
        mounts=MOUNTS,
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        command=[
            "--input",
            f"{DATA_DIR_CONTAINER}/reconciled_table_2.json",
            "--output",
            f"{DATA_DIR_CONTAINER}/enriched_data_2.csv",
        ],
        environment={
            "DATASET_ID": "2",
        },
        network_mode=DOCKER_NETWORK,
        mounts=MOUNTS,
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    # Define task dependencies
    load_and_modify >> entity_reconciliation >> save_final_data