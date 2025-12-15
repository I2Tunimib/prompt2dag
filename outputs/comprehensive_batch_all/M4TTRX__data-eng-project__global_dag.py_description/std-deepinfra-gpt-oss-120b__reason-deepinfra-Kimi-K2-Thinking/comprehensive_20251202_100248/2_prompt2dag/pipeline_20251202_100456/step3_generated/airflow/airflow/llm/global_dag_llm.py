# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: global_dag
Description: ETL pipeline processes French government death records and power plant data using a staged ETL pattern with mixed topology.
Generation Timestamp: 2024-06-13T12:00:00Z
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.providers.docker.operators.docker import DockerOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

# DAG definition
with DAG(
    dag_id="global_dag",
    description="ETL pipeline processes French government death records and power plant data using a staged ETL pattern with mixed topology.",
    schedule_interval=None,  # Disabled schedule
    catchup=False,
    default_args=default_args,
    tags=["etl", "french-government", "power-plants", "death-records"],
    is_paused_upon_creation=True,  # Ensure the DAG is not auto‑triggered
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    # -------------------------------------------------------------------------
    # Extraction tasks – pull raw data from public APIs
    # -------------------------------------------------------------------------
    extract_city_geo = DockerOperator(
        task_id="extract_city_geo",
        image="myorg/extract_city_geo:latest",
        api_version="auto",
        auto_remove=True,
        command="python extract.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "API_CONN_ID": "data_gouv_fr_city_geo_api",
            "OUTPUT_PATH": "/data/ingestion/city_geo.json",
        },
        mounts=[
            "/opt/airflow/data/ingestion:/data/ingestion",
        ],
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    extract_nuclear = DockerOperator(
        task_id="extract_nuclear",
        image="myorg/extract_nuclear:latest",
        api_version="auto",
        auto_remove=True,
        command="python extract.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "API_CONN_ID": "data_gouv_fr_nuclear_api",
            "OUTPUT_PATH": "/data/ingestion/nuclear.json",
        },
        mounts=[
            "/opt/airflow/data/ingestion:/data/ingestion",
        ],
    )

    extract_thermal = DockerOperator(
        task_id="extract_thermal",
        image="myorg/extract_thermal:latest",
        api_version="auto",
        auto_remove=True,
        command="python extract.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "API_CONN_ID": "data_gouv_fr_thermal_api",
            "OUTPUT_PATH": "/data/ingestion/thermal.json",
        },
        mounts=[
            "/opt/airflow/data/ingestion:/data/ingestion",
        ],
    )

    extract_death = DockerOperator(
        task_id="extract_death",
        image="myorg/extract_death:latest",
        api_version="auto",
        auto_remove=True,
        command="python extract.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "API_CONN_ID": "data_gouv_fr_death_api",
            "OUTPUT_PATH": "/data/ingestion/death_records.json",
        },
        mounts=[
            "/opt/airflow/data/ingestion:/data/ingestion",
        ],
    )

    # -------------------------------------------------------------------------
    # Staging task – move raw files to the staging area
    # -------------------------------------------------------------------------
    stage_raw_data = DockerOperator(
        task_id="stage_raw_data",
        image="myorg/stage_raw:latest",
        api_version="auto",
        auto_remove=True,
        command="python stage.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "INGESTION_PATH": "/data/ingestion",
            "STAGING_PATH": "/data/staging",
        },
        mounts=[
            "/opt/airflow/data/ingestion:/data/ingestion",
            "/opt/airflow/data/staging:/data/staging",
        ],
    )

    # -------------------------------------------------------------------------
    # Transformation task – apply SQL scripts to staged data
    # -------------------------------------------------------------------------
    transform_data = DockerOperator(
        task_id="transform_data",
        image="myorg/transform_sql:latest",
        api_version="auto",
        auto_remove=True,
        command="python transform.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "STAGING_PATH": "/data/staging",
            "SQL_SCRIPTS_PATH": "/sql/scripts",
            "REDIS_CONN_ID": "redis_intermediate",
        },
        mounts=[
            "/opt/airflow/data/staging:/data/staging",
            "/opt/airflow/sql_scripts:/sql/scripts",
        ],
    )

    # -------------------------------------------------------------------------
    # Cache task – store intermediate results in Redis
    # -------------------------------------------------------------------------
    cache_intermediate = DockerOperator(
        task_id="cache_intermediate",
        image="myorg/cache_redis:latest",
        api_version="auto",
        auto_remove=True,
        command="python cache.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "STAGING_PATH": "/data/staging",
            "REDIS_CONN_ID": "redis_intermediate",
        },
        mounts=[
            "/opt/airflow/data/staging:/data/staging",
        ],
    )

    # -------------------------------------------------------------------------
    # Load task – ingest transformed data into PostgreSQL
    # -------------------------------------------------------------------------
    load_to_postgres = DockerOperator(
        task_id="load_to_postgres",
        image="myorg/load_postgres:latest",
        api_version="auto",
        auto_remove=True,
        command="python load.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "STAGING_PATH": "/data/staging",
            "POSTGRES_CONN_ID": "postgres_target",
        },
        mounts=[
            "/opt/airflow/data/staging:/data/staging",
        ],
    )

    # -------------------------------------------------------------------------
    # Define task dependencies (mixed topology)
    # -------------------------------------------------------------------------
    # Extraction tasks run in parallel
    extract_tasks = [extract_city_geo, extract_nuclear, extract_thermal, extract_death]

    # All extraction tasks must finish before staging
    extract_tasks >> stage_raw_data

    # Staging -> transformation
    stage_raw_data >> transform_data

    # Transformation branches:
    #   - cache intermediate results
    #   - load final data to PostgreSQL
    transform_data >> [cache_intermediate, load_to_postgres]

    # No downstream dependencies for cache_intermediate (it ends the branch)

    # End of DAG definition. All tasks are now linked.