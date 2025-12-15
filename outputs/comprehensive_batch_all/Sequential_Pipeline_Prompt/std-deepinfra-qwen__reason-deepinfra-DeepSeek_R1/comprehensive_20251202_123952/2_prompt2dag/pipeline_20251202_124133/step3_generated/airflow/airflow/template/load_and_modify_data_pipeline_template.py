# ==============================================================================
# Generated Airflow DAG
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T12:49:43.172098
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
    'DATE_COLUMN': 'Fecha_id',
    'TABLE_NAMING_CONVENTION': 'JOT_{}',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: data_reconciliation
    data_reconciliation = DockerOperator(
        task_id='data_reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        command=['python', 'reconcile_data.py'],
        environment={
    'PRIMARY_COLUMN': 'City',
    'OPTIONAL_COLUMNS': 'County,Country',
    'RECONCILIATOR_ID': 'geocodingHere',
    'API_TOKEN': 'your_api_token',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: openmeteo_data_extension
    openmeteo_data_extension = DockerOperator(
        task_id='openmeteo_data_extension',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        command=['python', 'extend_data.py'],
        environment={
    'WEATHER_ATTRIBUTES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
    'DATE_FORMAT': 'your_date_format',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: column_extension
    column_extension = DockerOperator(
        task_id='column_extension',
        image='i2t-backendwithintertwino6-column-extension:latest',
        command=['python', 'extend_columns.py'],
        environment={
    'EXTENDER_ID': 'reconciledColumnExt',
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
    'DATA_DIR': '/app/data',
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
    load_and_modify_data >> data_reconciliation
    data_reconciliation >> openmeteo_data_extension
    openmeteo_data_extension >> column_extension
    column_extension >> save_final_data
