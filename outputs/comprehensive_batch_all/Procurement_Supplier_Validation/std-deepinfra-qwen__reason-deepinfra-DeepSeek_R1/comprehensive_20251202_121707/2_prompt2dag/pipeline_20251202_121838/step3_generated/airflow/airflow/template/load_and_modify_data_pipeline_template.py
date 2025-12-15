# ==============================================================================
# Generated Airflow DAG
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T12:21:41.527128
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
    'DATA_DIR': '/path/to/data',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: entity_reconciliation
    entity_reconciliation = DockerOperator(
        task_id='entity_reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        command=['python', 'reconcile.py'],
        environment={
    'PRIMARY_COLUMN': 'supplier_name',
    'RECONCILIATOR_ID': 'wikidataEntity',
    'DATASET_ID': '2',
    'DATA_DIR': '/path/to/data',
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
        command=['python', 'save.py'],
        environment={
    'DATASET_ID': '2',
    'DATA_DIR': '/path/to/data',
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
    load_and_modify_data >> entity_reconciliation
    entity_reconciliation >> save_final_data
