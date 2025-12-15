# ==============================================================================
# Generated Airflow DAG
# Pipeline: load_modify_facilities_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T10:46:33.934901
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
    dag_id='load_modify_facilities_pipeline',
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

    # Task: load_modify_facilities
    load_modify_facilities = DockerOperator(
        task_id='load_modify_facilities',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
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

    # Task: geocode_facilities
    geocode_facilities = DockerOperator(
        task_id='geocode_facilities',
        image='i2t-backendwithintertwino6-reconciliation:latest',
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

    # Task: calculate_distance_to_public_transport
    calculate_distance_to_public_transport = DockerOperator(
        task_id='calculate_distance_to_public_transport',
        image='i2t-backendwithintertwino6-column-extension:latest',
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

    # Task: calculate_distance_to_residential_areas
    calculate_distance_to_residential_areas = DockerOperator(
        task_id='calculate_distance_to_residential_areas',
        image='i2t-backendwithintertwino6-column-extension:latest',
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

    # Task: save_facilities_accessibility
    save_facilities_accessibility = DockerOperator(
        task_id='save_facilities_accessibility',
        image='i2t-backendwithintertwino6-save:latest',
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
    load_modify_facilities >> geocode_facilities
    geocode_facilities >> calculate_distance_to_public_transport
    calculate_distance_to_public_transport >> calculate_distance_to_residential_areas
    calculate_distance_to_residential_areas >> save_facilities_accessibility
