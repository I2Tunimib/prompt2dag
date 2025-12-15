from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="load_and_modify_data_pipeline",
    default_args=default_args,
    description="Sequential pipeline to load, enrich, and save data.",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:

    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    geocode_reconciliation = DockerOperator(
        task_id='geocode_reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": "[HERE API token]",
            "DATASET_ID": "2",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    openmeteo_extension = DockerOperator(
        task_id='openmeteo_extension',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "DATE_COLUMN": "installation_date",
            "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    land_use_extension = DockerOperator(
        task_id='land_use_extension',
        image='geoapify-land-use:latest',
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "land_use_type",
            "API_KEY": "[Geoapify API key]",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    population_density_extension = DockerOperator(
        task_id='population_density_extension',
        image='worldpop-density:latest',
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "population_density",
            "RADIUS": "5000",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    calculate_environmental_risk = DockerOperator(
        task_id='calculate_environmental_risk',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            "EXTENDER_ID": "environmentalRiskCalculator",
            "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
            "OUTPUT_COLUMN": "risk_score",
            "CALCULATION_FORMULA": "[risk calculation parameters]",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            "DATASET_ID": "2",
            "DATA_DIR": "/data"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    load_and_modify_data >> geocode_reconciliation >> openmeteo_extension >> land_use_extension >> population_density_extension >> calculate_environmental_risk >> save_final_data