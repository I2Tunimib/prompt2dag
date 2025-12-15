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
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    reconcile_geocoding = DockerOperator(
        task_id='reconcile_geocoding',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": "[HERE API token]",
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_openmeteo_data = DockerOperator(
        task_id='extend_openmeteo_data',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "DATE_COLUMN": "installation_date",
            "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR_FORMAT": "YYYYMMDD"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_land_use = DockerOperator(
        task_id='extend_land_use',
        image='geoapify-land-use:latest',
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "land_use_type",
            "API_KEY": "[Geoapify API key]"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_population_density = DockerOperator(
        task_id='extend_population_density',
        image='worldpop-density:latest',
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "population_density",
            "RADIUS": "5000"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_environmental_risk = DockerOperator(
        task_id='extend_environmental_risk',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            "EXTENDER_ID": "environmentalRiskCalculator",
            "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
            "OUTPUT_COLUMN": "risk_score",
            "CALCULATION_FORMULA": "[risk calculation parameters]"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    load_and_modify_data >> reconcile_geocoding >> extend_openmeteo_data >> extend_land_use >> extend_population_density >> extend_environmental_risk >> save_final_data