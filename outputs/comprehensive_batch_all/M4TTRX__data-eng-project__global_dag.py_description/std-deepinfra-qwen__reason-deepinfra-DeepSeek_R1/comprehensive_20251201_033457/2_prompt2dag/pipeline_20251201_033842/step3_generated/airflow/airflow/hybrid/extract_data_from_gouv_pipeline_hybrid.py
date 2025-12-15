from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='extract_data_from_gouv_pipeline',
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task definitions
    check_death_data_emptiness = DockerOperator(
        task_id='check_death_data_emptiness',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    staging_end = DockerOperator(
        task_id='staging_end',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    store_deaths_in_postgres = DockerOperator(
        task_id='store_deaths_in_postgres',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    fetch_nuclear_data = DockerOperator(
        task_id='fetch_nuclear_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    clean_tmp_death_files = DockerOperator(
        task_id='clean_tmp_death_files',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanse_power_plant_data = DockerOperator(
        task_id='cleanse_power_plant_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_plant_persist_sql = DockerOperator(
        task_id='generate_plant_persist_sql',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    store_plants_in_postgres = DockerOperator(
        task_id='store_plants_in_postgres',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanse_death_data = DockerOperator(
        task_id='cleanse_death_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_death_table = DockerOperator(
        task_id='create_death_table',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    fetch_death_records = DockerOperator(
        task_id='fetch_death_records',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_death_records_to_redis = DockerOperator(
        task_id='load_death_records_to_redis',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_data_from_gouv = DockerOperator(
        task_id='extract_data_from_gouv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_power_plants_table = DockerOperator(
        task_id='create_power_plants_table',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    fetch_thermal_data = DockerOperator(
        task_id='fetch_thermal_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_city_geo = DockerOperator(
        task_id='download_city_geo',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    ingestion_pipeline_start = extract_data_from_gouv
    staging_pipeline_start = create_death_table

    extract_data_from_gouv >> download_city_geo
    extract_data_from_gouv >> fetch_nuclear_data
    extract_data_from_gouv >> fetch_thermal_data
    extract_data_from_gouv >> fetch_death_records

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