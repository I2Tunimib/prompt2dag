import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Retrieve the shared data directory from the environment or use a fallback
DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")
# Ensure the directory exists on the host (Airflow worker)
if not os.path.isdir(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

# Common Docker settings
DOCKER_NETWORK = "app_network"
VOLUME_BINDING = f"{DATA_DIR}:/data"

with DAG(
    dag_id="procurement_supplier_validation",
    default_args=default_args,
    description="Validate and enrich supplier data using Wikidata reconciliation.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["procurement", "validation", "wikidata"],
) as dag:

    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        command=None,  # Use container's default entrypoint
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
            "DATA_DIR": "/data",
        },
        network_mode=DOCKER_NETWORK,
        volumes=[VOLUME_BINDING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    reconcile_wikidata = DockerOperator(
        task_id="reconcile_wikidata",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "PRIMARY_COLUMN": "supplier_name",
            "RECONCILIATOR_ID": "wikidataEntity",
            "DATASET_ID": "2",
            "DATA_DIR": "/data",
        },
        network_mode=DOCKER_NETWORK,
        volumes=[VOLUME_BINDING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "DATASET_ID": "2",
            "DATA_DIR": "/data",
        },
        network_mode=DOCKER_NETWORK,
        volumes=[VOLUME_BINDING],
        docker_url="unix://var/run/docker.sock",
        tty=True,
    )

    # Define task dependencies
    load_and_modify >> reconcile_wikidata >> save_final_data