from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='load_and_modify_data_pipeline',
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task definitions
    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "Fecha_id",
            "TABLE_NAMING_CONVENTION": "JOT_{}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    reconcile_data = DockerOperator(
        task_id='reconcile_data',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={
            "PRIMARY_COLUMN": "City",
            "OPTIONAL_COLUMNS": "County,Country",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": "your_api_token"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_with_weather_data = DockerOperator(
        task_id='extend_with_weather_data',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        environment={
            "DATE_FORMAT": "your_date_format"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_columns = DockerOperator(
        task_id='extend_columns',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            "EXTENDER_ID": "reconciledColumnExt"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    load_and_modify_data >> reconcile_data >> extend_with_weather_data >> extend_columns >> save_final_data