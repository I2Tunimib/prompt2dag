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

    data_reconciliation = DockerOperator(
        task_id='data_reconciliation',
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

    openmeteo_data_extension = DockerOperator(
        task_id='openmeteo_data_extension',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        environment={
            "WEATHER_ATTRIBUTES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_FORMAT": "your_date_format"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    column_extension = DockerOperator(
        task_id='column_extension',
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
        environment={
            "DATA_DIR": "/app/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    load_and_modify_data >> data_reconciliation >> openmeteo_data_extension >> column_extension >> save_final_data