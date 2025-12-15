# ==============================================================================
# Generated Airflow DAG
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T10:30:53.360587
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
    description='No description provided.',
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
    'PRIMARY_COLUMN': 'address',
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

    # Task: calculate_distance_pt
    calculate_distance_pt = DockerOperator(
        task_id='calculate_distance_pt',
        image='i2t-backendwithintertwino6-column-extension:latest',
        command=['python', 'calculate_distance.py'],
        environment={
    'EXTENDER_ID': 'spatialDistanceCalculator',
    'LAT_COLUMN': 'latitude',
    'LON_COLUMN': 'longitude',
    'TARGET_LAYER': 'public_transport',
    'TARGET_DATA_SOURCE': '/app/data/transport_stops.geojson',
    'OUTPUT_COLUMN': 'distance_to_pt',
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

    # Task: calculate_distance_residential
    calculate_distance_residential = DockerOperator(
        task_id='calculate_distance_residential',
        image='i2t-backendwithintertwino6-column-extension:latest',
        command=['python', 'calculate_distance.py'],
        environment={
    'EXTENDER_ID': 'spatialDistanceCalculator',
    'LAT_COLUMN': 'latitude',
    'LON_COLUMN': 'longitude',
    'TARGET_LAYER': 'residential_areas',
    'TARGET_DATA_SOURCE': '/app/data/residential_areas.geojson',
    'OUTPUT_COLUMN': 'distance_to_residential',
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

    # Task: save_final_data
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        command=['python', 'save_data.py'],
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
    reconcile_geocoding >> calculate_distance_pt
    calculate_distance_pt >> calculate_distance_residential
    calculate_distance_residential >> save_final_data
