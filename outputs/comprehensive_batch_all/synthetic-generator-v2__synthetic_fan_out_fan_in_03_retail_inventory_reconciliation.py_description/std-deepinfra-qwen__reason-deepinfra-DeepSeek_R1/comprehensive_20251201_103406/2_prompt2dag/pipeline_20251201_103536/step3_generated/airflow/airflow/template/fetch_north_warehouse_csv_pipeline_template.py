# ==============================================================================
# Generated Airflow DAG - Fan-Out/Fan-In Pattern
# Pipeline: fetch_north_warehouse_csv_pipeline
# Pattern: fanin
# Strategy: template
# Generated: 2025-12-01T10:44:16.898644
# ==============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
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
    dag_id='fetch_north_warehouse_csv_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'fanin'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Identify fan-out and fan-in points

    # Task: fetch_north_warehouse_csv
    fetch_north_warehouse_csv = DockerOperator(
        task_id='fetch_north_warehouse_csv',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: fetch_south_warehouse_csv
    fetch_south_warehouse_csv = DockerOperator(
        task_id='fetch_south_warehouse_csv',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: fetch_east_warehouse_csv
    fetch_east_warehouse_csv = DockerOperator(
        task_id='fetch_east_warehouse_csv',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: fetch_west_warehouse_csv
    fetch_west_warehouse_csv = DockerOperator(
        task_id='fetch_west_warehouse_csv',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: normalize_north_skus
    normalize_north_skus = DockerOperator(
        task_id='normalize_north_skus',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: normalize_south_skus
    normalize_south_skus = DockerOperator(
        task_id='normalize_south_skus',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: normalize_east_skus
    normalize_east_skus = DockerOperator(
        task_id='normalize_east_skus',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: normalize_west_skus
    normalize_west_skus = DockerOperator(
        task_id='normalize_west_skus',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: reconcile_all_inventories
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=all_success)
    reconcile_all_inventories = DockerOperator(
        task_id='reconcile_all_inventories',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: generate_final_report
    generate_final_report = DockerOperator(
        task_id='generate_final_report',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )


    # ==========================================================================
    # Task Dependencies - Fan-Out/Fan-In Pattern
    # ==========================================================================
    # Fan-out points: None detected
    # Fan-in points: ['reconcile_all_inventories']

    fetch_north_warehouse_csv >> normalize_north_skus
    fetch_south_warehouse_csv >> normalize_south_skus
    fetch_east_warehouse_csv >> normalize_east_skus
    fetch_west_warehouse_csv >> normalize_west_skus
    normalize_north_skus >> reconcile_all_inventories
    normalize_south_skus >> reconcile_all_inventories
    normalize_east_skus >> reconcile_all_inventories
    normalize_west_skus >> reconcile_all_inventories
    reconcile_all_inventories >> generate_final_report
