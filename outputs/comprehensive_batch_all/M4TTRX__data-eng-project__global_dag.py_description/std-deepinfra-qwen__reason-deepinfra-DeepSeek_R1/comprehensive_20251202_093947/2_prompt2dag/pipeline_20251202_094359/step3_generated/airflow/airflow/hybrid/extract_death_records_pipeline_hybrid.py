from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='extract_death_records_pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task definitions
    cleanse_power_plant_data = DockerOperator(
        task_id='cleanse_power_plant_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_plant_insert_queries = DockerOperator(
        task_id='generate_plant_insert_queries',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_power_plant_table = DockerOperator(
        task_id='create_power_plant_table',
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

    cleanse_death_data = DockerOperator(
        task_id='cleanse_death_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    check_death_data_emptiness = DockerOperator(
        task_id='check_death_data_emptiness',
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

    clean_tmp_death_files = DockerOperator(
        task_id='clean_tmp_death_files',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_death_records = DockerOperator(
        task_id='extract_death_records',
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

    extract_nuclear_plants = DockerOperator(
        task_id='extract_nuclear_plants',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_thermal_plants = DockerOperator(
        task_id='extract_thermal_plants',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_city_geo_data = DockerOperator(
        task_id='extract_city_geo_data',
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

    # Task dependencies
    extract_death_records >> cleanse_death_data
    extract_nuclear_plants >> cleanse_power_plant_data
    extract_thermal_plants >> cleanse_power_plant_data
    extract_city_geo_data >> cleanse_power_plant_data
    create_death_table >> load_death_records_to_redis
    create_power_plant_table >> cleanse_power_plant_data
    load_death_records_to_redis >> cleanse_death_data
    cleanse_death_data >> check_death_data_emptiness
    check_death_data_emptiness >> store_deaths_in_postgres
    store_deaths_in_postgres >> clean_tmp_death_files
    cleanse_power_plant_data >> generate_plant_insert_queries
    generate_plant_insert_queries >> store_plants_in_postgres
    store_plants_in_postgres >> clean_tmp_death_files