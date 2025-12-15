# ==============================================================================
# Generated Airflow DAG
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T01:01:38.507519
# ==============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.helpers import chain
from docker.types import Mount

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/airflow/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Default Arguments ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id='load_and_modify_data_pipeline',
    default_args=default_args,
    description='Environmental Monitoring Network Pipeline Configuration',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: load_and_modify_data
    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        command=['python', 'load_and_modify.py'],
        environment={
    'DATASET_ID': '2',
    'DATE_COLUMN': 'installation_date',
    'TABLE_NAME_PREFIX': 'JOT_',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: reconcile_geocoding
    reconcile_geocoding = DockerOperator(
        task_id='reconcile_geocoding',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        command=['python', 'reconcile_geocoding.py'],
        environment={
    'PRIMARY_COLUMN': 'location',
    'RECONCILIATOR_ID': 'geocodingHere',
    'API_TOKEN': '[HERE API token]',
    'DATASET_ID': '2',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: extend_openmeteo_data
    extend_openmeteo_data = DockerOperator(
        task_id='extend_openmeteo_data',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        command=['python', 'extend_openmeteo.py'],
        environment={
    'LAT_COLUMN': 'latitude',
    'LON_COLUMN': 'longitude',
    'DATE_COLUMN': 'installation_date',
    'WEATHER_VARIABLES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
    'DATE_SEPARATOR_FORMAT': 'YYYYMMDD',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: extend_land_use
    extend_land_use = DockerOperator(
        task_id='extend_land_use',
        image='geoapify-land-use:latest',
        command=['python', 'extend_land_use.py'],
        environment={
    'LAT_COLUMN': 'latitude',
    'LON_COLUMN': 'longitude',
    'OUTPUT_COLUMN': 'land_use_type',
    'API_KEY': '[Geoapify API key]',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: extend_population_density
    extend_population_density = DockerOperator(
        task_id='extend_population_density',
        image='worldpop-density:latest',
        command=['python', 'extend_population_density.py'],
        environment={
    'LAT_COLUMN': 'latitude',
    'LON_COLUMN': 'longitude',
    'OUTPUT_COLUMN': 'population_density',
    'RADIUS': '5000',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: extend_environmental_risk
    extend_environmental_risk = DockerOperator(
        task_id='extend_environmental_risk',
        image='i2t-backendwithintertwino6-column-extension:latest',
        command=['python', 'extend_environmental_risk.py'],
        environment={
    'EXTENDER_ID': 'environmentalRiskCalculator',
    'INPUT_COLUMNS': 'precipitation_sum,population_density,land_use_type',
    'OUTPUT_COLUMN': 'risk_score',
    'CALCULATION_FORMULA': '[risk calculation parameters]',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: save_final_data
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        command=['python', 'save_final_data.py'],
        environment={
    'DATASET_ID': '2',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    load_and_modify_data >> reconcile_geocoding
    reconcile_geocoding >> extend_openmeteo_data
    extend_openmeteo_data >> extend_land_use
    extend_land_use >> extend_population_density
    extend_population_density >> extend_environmental_risk
    extend_environmental_risk >> save_final_data
