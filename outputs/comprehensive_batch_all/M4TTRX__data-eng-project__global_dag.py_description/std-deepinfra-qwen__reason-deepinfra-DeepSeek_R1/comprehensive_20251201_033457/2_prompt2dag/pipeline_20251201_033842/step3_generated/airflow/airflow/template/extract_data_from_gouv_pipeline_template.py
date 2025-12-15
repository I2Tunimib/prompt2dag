# ==============================================================================
# Generated Airflow DAG - Fan-Out/Fan-In Pattern
# Pipeline: extract_data_from_gouv_pipeline
# Pattern: fanout_fanin
# Strategy: template
# Generated: 2025-12-01T03:49:59.572327
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
    dag_id='extract_data_from_gouv_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'fanout_fanin'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Identify fan-out and fan-in points

    # Task: extract_data_from_gouv
    extract_data_from_gouv = DockerOperator(
        task_id='extract_data_from_gouv',
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

    # Task: download_city_geo
    download_city_geo = DockerOperator(
        task_id='download_city_geo',
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

    # Task: fetch_nuclear_data
    fetch_nuclear_data = DockerOperator(
        task_id='fetch_nuclear_data',
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

    # Task: fetch_thermal_data
    fetch_thermal_data = DockerOperator(
        task_id='fetch_thermal_data',
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

    # Task: fetch_death_records
    fetch_death_records = DockerOperator(
        task_id='fetch_death_records',
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

    # Task: create_death_table
    create_death_table = DockerOperator(
        task_id='create_death_table',
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

    # Task: create_power_plants_table
    create_power_plants_table = DockerOperator(
        task_id='create_power_plants_table',
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

    # Task: load_death_records_to_redis
    load_death_records_to_redis = DockerOperator(
        task_id='load_death_records_to_redis',
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

    # Task: cleanse_death_data
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=all_success)
    cleanse_death_data = DockerOperator(
        task_id='cleanse_death_data',
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

    # Task: cleanse_power_plant_data
    cleanse_power_plant_data = DockerOperator(
        task_id='cleanse_power_plant_data',
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

    # Task: generate_plant_persist_sql
    generate_plant_persist_sql = DockerOperator(
        task_id='generate_plant_persist_sql',
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

    # Task: check_death_data_emptiness
    # ⚡ FAN-OUT POINT: Multiple downstream tasks
    check_death_data_emptiness = DockerOperator(
        task_id='check_death_data_emptiness',
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

    # Task: store_deaths_in_postgres
    store_deaths_in_postgres = DockerOperator(
        task_id='store_deaths_in_postgres',
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

    # Task: staging_end
    staging_end = DockerOperator(
        task_id='staging_end',
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

    # Task: store_plants_in_postgres
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=all_success)
    store_plants_in_postgres = DockerOperator(
        task_id='store_plants_in_postgres',
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

    # Task: clean_tmp_death_files
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=all_success)
    clean_tmp_death_files = DockerOperator(
        task_id='clean_tmp_death_files',
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
    # Fan-out points: ['check_death_data_emptiness']
    # Fan-in points: ['cleanse_death_data', 'store_plants_in_postgres', 'clean_tmp_death_files']

    ingestion_pipeline_start >> extract_data_from_gouv
    ingestion_pipeline_start >> download_city_geo
    ingestion_pipeline_start >> fetch_nuclear_data
    ingestion_pipeline_start >> fetch_thermal_data
    ingestion_pipeline_start >> fetch_death_records
    staging_pipeline_start >> create_death_table
    staging_pipeline_start >> create_power_plants_table
    staging_pipeline_start >> load_death_records_to_redis
    create_death_table >> cleanse_death_data
    load_death_records_to_redis >> cleanse_death_data
    create_power_plants_table >> cleanse_power_plant_data
    cleanse_power_plant_data >> generate_plant_persist_sql
    cleanse_death_data >> check_death_data_emptiness
    check_death_data_emptiness >> store_deaths_in_postgres
    check_death_data_emptiness >> staging_end
    generate_plant_persist_sql >> store_plants_in_postgres
    staging_end >> store_plants_in_postgres
    store_deaths_in_postgres >> clean_tmp_death_files
    store_plants_in_postgres >> clean_tmp_death_files
