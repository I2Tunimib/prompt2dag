from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["pipeline", "docker"],
    doc_md="""
## Data Processing Pipeline

Sequential Docker‑containerized tasks that:

1. Load CSV files and convert them to JSON.
2. Reconcile city names via HERE geocoding.
3. Enrich data with OpenMeteo weather information.
4. Extend columns with additional properties.
5. Save the final enriched dataset as CSV.
""",
) as dag:
    # Shared configuration
    data_dir = "/app/data"
    docker_network = "app_network"
    volume_binding = f"{data_dir}:{data_dir}"

    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "DATA_DIR": data_dir,
            "DATASET_ID": "2",
            "DATE_COLUMN": "Fecha_id",
            "TABLE_PREFIX": "JOT_",
        },
        network_mode=docker_network,
        volumes=[volume_binding],
    )

    data_reconciliation = DockerOperator(
        task_id="data_reconciliation",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "DATA_DIR": data_dir,
            "RECONCILIATOR_ID": "geocodingHere",
            "PRIMARY_COLUMN": "City",
            "OPTIONAL_COLUMNS": "County,Country",
        },
        network_mode=docker_network,
        volumes=[volume_binding],
    )

    openmeteo_extension = DockerOperator(
        task_id="openmeteo_extension",
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "DATA_DIR": data_dir,
            "DATE_SEPARATOR": "-",
        },
        network_mode=docker_network,
        volumes=[volume_binding],
    )

    column_extension = DockerOperator(
        task_id="column_extension",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "DATA_DIR": data_dir,
            "EXTENDER_ID": "reconciledColumnExt",
        },
        network_mode=docker_network,
        volumes=[volume_binding],
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        command=None,
        environment={
            "DATA_DIR": data_dir,
        },
        network_mode=docker_network,
        volumes=[volume_binding],
    )

    # Define sequential dependencies
    load_and_modify >> data_reconciliation >> openmeteo_extension >> column_extension >> save_final_data