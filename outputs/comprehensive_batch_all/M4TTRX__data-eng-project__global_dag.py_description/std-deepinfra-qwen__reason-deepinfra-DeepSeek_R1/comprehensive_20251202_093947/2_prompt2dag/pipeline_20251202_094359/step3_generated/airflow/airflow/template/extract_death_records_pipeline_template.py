# ==============================================================================
# Generated Airflow DAG - Fan-Out/Fan-In Pattern
# Pipeline: extract_death_records_pipeline
# Pattern: fanout
# Strategy: template
# Generated: 2025-12-02T09:52:32.345289
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
    dag_id='extract_death_records_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'fanout'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Identify fan-out and fan-in points

    # Task: extract_death_records
    extract_death_records = DockerOperator(
        task_id='extract_death_records',
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

    # Task: extract_nuclear_plants
    extract_nuclear_plants = DockerOperator(
        task_id='extract_nuclear_plants',
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

    # Task: extract_thermal_plants
    extract_thermal_plants = DockerOperator(
        task_id='extract_thermal_plants',
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

    # Task: extract_city_geo_data
    extract_city_geo_data = DockerOperator(
        task_id='extract_city_geo_data',
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

    # Task: create_power_plant_table
    create_power_plant_table = DockerOperator(
        task_id='create_power_plant_table',
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

    # Task: generate_plant_insert_queries
    generate_plant_insert_queries = DockerOperator(
        task_id='generate_plant_insert_queries',
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

    # Task: store_plants_in_postgres
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
    # Fan-out points: None detected
    # Fan-in points: None detected

    ingestion_pipeline_start >> extract_death_records
    ingestion_pipeline_start >> extract_nuclear_plants
    ingestion_pipeline_start >> extract_thermal_plants
    ingestion_pipeline_start >> extract_city_geo_data
    staging_pipeline_start >> create_death_table
    staging_pipeline_start >> create_power_plant_table
    staging_pipeline_start >> load_death_records_to_redis
    load_death_records_to_redis >> cleanse_death_data
    cleanse_death_data >> check_death_data_emptiness
    check_death_data_emptiness >> store_deaths_in_postgres
    staging_pipeline_end >> store_plants_in_postgres
    store_plants_in_postgres >> clean_tmp_death_files
