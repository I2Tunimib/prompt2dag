"""
Environmental Monitoring Network Pipeline DAG.
Processes station data through geocoding, weather, land use, demographic,
and risk calculation stages to produce enriched environmental datasets.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="environmental_monitoring_pipeline",
    default_args=default_args,
    description="Environmental monitoring network pipeline for risk analysis",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["environmental", "monitoring"],
) as dag:

    DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

    docker_defaults = {
        "api_version": "auto",
        "auto_remove": True,
        "network_mode": "app_network",
        "docker_url": "unix://var/run/docker.sock",
        "mounts": [Mount(source=DATA_DIR, target=DATA_DIR, type="bind")],
    }

    load_and_modify = DockerOperator(
        task_id="load_and_modify_data",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_",
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    wait_for_table_data = FileSensor(
        task_id="wait_for_table_data",
        filepath=f"{DATA_DIR}/table_data_2.json",
        poke_interval=30,
        timeout=600,
    )

    reconciliation = DockerOperator(
        task_id="reconciliation_geocoding",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        environment={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": os.getenv("HERE_API_TOKEN", "your_here_api_token"),
            "DATASET_ID": "2",
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    wait_for_reconciled_data = FileSensor(
        task_id="wait_for_reconciled_data",
        filepath=f"{DATA_DIR}/reconciled_table_2.json",
        poke_interval=30,
        timeout=600,
    )

    openmeteo_extension = DockerOperator(
        task_id="openmeteo_data_extension",
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "DATE_COLUMN": "installation_date",
            "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    wait_for_openmeteo_data = FileSensor(
        task_id="wait_for_openmeteo_data",
        filepath=f"{DATA_DIR}/open_meteo_2.json",
        poke_interval=30,
        timeout=600,
    )

    land_use_extension = DockerOperator(
        task_id="land_use_extension",
        image="geoapify-land-use:latest",
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "land_use_type",
            "API_KEY": os.getenv("GEOAPIFY_API_KEY", "your_geoapify_api_key"),
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    wait_for_land_use_data = FileSensor(
        task_id="wait_for_land_use_data",
        filepath=f"{DATA_DIR}/land_use_2.json",
        poke_interval=30,
        timeout=600,
    )

    population_density_extension = DockerOperator(
        task_id="population_density_extension",
        image="worldpop-density:latest",
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "population_density",
            "RADIUS": "5000",
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    wait_for_pop_density_data = FileSensor(
        task_id="wait_for_pop_density_data",
        filepath=f"{DATA_DIR}/pop_density_2.json",
        poke_interval=30,
        timeout=600,
    )

    environmental_calculation = DockerOperator(
        task_id="environmental_risk_calculation",
        image="i2t-backendwithintertwino6-column-extension:latest",
        environment={
            "EXTENDER_ID": "environmentalRiskCalculator",
            "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
            "OUTPUT_COLUMN": "risk_score",
            "CALCULATION_FORMULA": os.getenv("RISK_CALCULATION_FORMULA", "default_risk_calculation"),
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    wait_for_column_extended_data = FileSensor(
        task_id="wait_for_column_extended_data",
        filepath=f"{DATA_DIR}/column_extended_2.json",
        poke_interval=30,
        timeout=600,
    )

    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        environment={
            "DATASET_ID": "2",
            "DATA_DIR": DATA_DIR,
        },
        **docker_defaults,
    )

    load_and_modify >> wait_for_table_data >> reconciliation >> wait_for_reconciled_data
    wait_for_reconciled_data >> openmeteo_extension >> wait_for_openmeteo_data
    wait_for_openmeteo_data >> land_use_extension >> wait_for_land_use_data
    wait_for_land_use_data >> population_density_extension >> wait_for_pop_density_data
    wait_for_pop_density_data >> environmental_calculation >> wait_for_column_extended_data
    wait_for_column_extended_data >> save_final_data