# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for ETL pipeline: global_dag
Description: ETL pipeline that processes French government death records and power plant data
using a staged ETL pattern with fan‑out/fan‑in parallelism, conditional branching,
and PostgreSQL loading.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="global_dag",
    description="ETL pipeline that processes French government death records and power plant data using a staged ETL pattern with fan‑out/fan‑in parallelism, conditional branching, and PostgreSQL loading.",
    schedule_interval=None,               # Disabled schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "french-data", "postgres"],
    is_paused_upon_creation=True,        # Ensure the DAG starts in a paused state
) as dag:

    # -----------------------------------------------------------------
    # Start / End markers
    # -----------------------------------------------------------------
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # -----------------------------------------------------------------
    # Ingestion tasks (fan‑out)
    # -----------------------------------------------------------------
    with TaskGroup("ingestion") as ingestion_group:
        ingest_city_geo = DockerOperator(
            task_id="ingest_city_geo",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="ingest_city_geo",
            environment={
                "API_ENDPOINT": "{{ conn.city_geo_api.host }}",
                "TARGET_PATH": "/data/ingestion/city_geo.json",
            },
            mounts=[
                # Mount the local ingestion filesystem into the container
                # Adjust the source path according to your Airflow worker setup
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
            ],
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        ingest_nuclear_plants = DockerOperator(
            task_id="ingest_nuclear_plants",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="ingest_nuclear_plants",
            environment={
                "API_ENDPOINT": "{{ conn.nuclear_plants_api.host }}",
                "TARGET_PATH": "/data/ingestion/nuclear_plants.json",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
            ],
        )

        ingest_thermal_plants = DockerOperator(
            task_id="ingest_thermal_plants",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="ingest_thermal_plants",
            environment={
                "API_ENDPOINT": "{{ conn.thermal_plants_api.host }}",
                "TARGET_PATH": "/data/ingestion/thermal_plants.json",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
            ],
        )

        ingest_death_records = DockerOperator(
            task_id="ingest_death_records",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="ingest_death_records",
            environment={
                "API_ENDPOINT": "{{ conn.death_records_api.host }}",
                "TARGET_PATH": "/data/ingestion/death_records.json",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
            ],
        )

    # -----------------------------------------------------------------
    # Staging tasks (fan‑out after ingestion)
    # -----------------------------------------------------------------
    with TaskGroup("staging") as staging_group:
        stage_city_geo = DockerOperator(
            task_id="stage_city_geo",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="stage_file",
            environment={
                "SOURCE_PATH": "/data/ingestion/city_geo.json",
                "TARGET_PATH": "/data/staging/city_geo.parquet",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
            ],
        )

        stage_nuclear_plants = DockerOperator(
            task_id="stage_nuclear_plants",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="stage_file",
            environment={
                "SOURCE_PATH": "/data/ingestion/nuclear_plants.json",
                "TARGET_PATH": "/data/staging/nuclear_plants.parquet",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
            ],
        )

        stage_thermal_plants = DockerOperator(
            task_id="stage_thermal_plants",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="stage_file",
            environment={
                "SOURCE_PATH": "/data/ingestion/thermal_plants.json",
                "TARGET_PATH": "/data/staging/thermal_plants.parquet",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
            ],
        )

        stage_death_records = DockerOperator(
            task_id="stage_death_records",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="stage_file",
            environment={
                "SOURCE_PATH": "/data/ingestion/death_records.json",
                "TARGET_PATH": "/data/staging/death_records.parquet",
            },
            mounts=[
                {"source": "/opt/airflow/data/ingestion", "target": "/data/ingestion", "type": "bind"},
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
            ],
        )

    # -----------------------------------------------------------------
    # Transformation tasks (fan‑out after staging)
    # -----------------------------------------------------------------
    with TaskGroup("transformation") as transformation_group:
        transform_city_geo = DockerOperator(
            task_id="transform_city_geo",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="transform_city_geo",
            environment={
                "INPUT_PATH": "/data/staging/city_geo.parquet",
                "OUTPUT_PATH": "/data/sql_tmp/city_geo.sql",
            },
            mounts=[
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
                {"source": "/opt/airflow/data/sql_tmp", "target": "/data/sql_tmp", "type": "bind"},
            ],
        )

        transform_nuclear_plants = DockerOperator(
            task_id="transform_nuclear_plants",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="transform_nuclear_plants",
            environment={
                "INPUT_PATH": "/data/staging/nuclear_plants.parquet",
                "OUTPUT_PATH": "/data/sql_tmp/nuclear_plants.sql",
            },
            mounts=[
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
                {"source": "/opt/airflow/data/sql_tmp", "target": "/data/sql_tmp", "type": "bind"},
            ],
        )

        transform_thermal_plants = DockerOperator(
            task_id="transform_thermal_plants",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="transform_thermal_plants",
            environment={
                "INPUT_PATH": "/data/staging/thermal_plants.parquet",
                "OUTPUT_PATH": "/data/sql_tmp/thermal_plants.sql",
            },
            mounts=[
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
                {"source": "/opt/airflow/data/sql_tmp", "target": "/data/sql_tmp", "type": "bind"},
            ],
        )

        transform_death_records = DockerOperator(
            task_id="transform_death_records",
            image="mycompany/etl-task:latest",
            api_version="auto",
            auto_remove=True,
            command="transform_death_records",
            environment={
                "INPUT_PATH": "/data/staging/death_records.parquet",
                "OUTPUT_PATH": "/data/sql_tmp/death_records.sql",
            },
            mounts=[
                {"source": "/opt/airflow/data/staging", "target": "/data/staging", "type": "bind"},
                {"source": "/opt/airflow/data/sql_tmp", "target": "/data/sql_tmp", "type": "bind"},
            ],
        )

    # -----------------------------------------------------------------
    # Conditional branching based on death records volume
    # -----------------------------------------------------------------
    def decide_extra_processing(**context):
        """
        Simple branching logic: if the death records file size exceeds 100 MB,
        run extra processing; otherwise skip.
        """
        import os

        sql_tmp_path = "/opt/airflow/data/sql_tmp/death_records.sql"
        try:
            size_bytes = os.path.getsize(sql_tmp_path)
        except OSError:
            # If the file does not exist, skip extra processing
            return "skip_extra_processing"

        # 100 MB threshold
        if size_bytes > 100 * 1024 * 1024:
            return "extra_death_processing"
        return "skip_extra_processing"

    branch_death_records = BranchPythonOperator(
        task_id="branch_death_records",
        python_callable=decide_extra_processing,
        provide_context=True,
    )

    extra_death_processing = DockerOperator(
        task_id="extra_death_processing",
        image="mycompany/etl-task:latest",
        api_version="auto",
        auto_remove=True,
        command="extra_death_processing",
        environment={
            "INPUT_SQL": "/data/sql_tmp/death_records.sql",
            "OUTPUT_SQL": "/data/sql_tmp/death_records_enriched.sql",
        },
        mounts=[
            {"source": "/opt/airflow/data/sql_tmp", "target": "/data/sql_tmp", "type": "bind"},
        ],
    )

    skip_extra_processing = DummyOperator(task_id="skip_extra_processing")

    # -----------------------------------------------------------------
    # Load into PostgreSQL (fan‑in)
    # -----------------------------------------------------------------
    with TaskGroup("load") as load_group:
        load_city_geo = PostgresOperator(
            task_id="load_city_geo",
            postgres_conn_id="postgres_db",
            sql="/data/sql_tmp/city_geo.sql",
        )

        load_nuclear_plants = PostgresOperator(
            task_id="load_nuclear_plants",
            postgres_conn_id="postgres_db",
            sql="/data/sql_tmp/nuclear_plants.sql",
        )

        load_thermal_plants = PostgresOperator(
            task_id="load_thermal_plants",
            postgres_conn_id="postgres_db",
            sql="/data/sql_tmp/thermal_plants.sql",
        )

        # Use the enriched file if extra processing was executed, otherwise the original
        load_death_records = PostgresOperator(
            task_id="load_death_records",
            postgres_conn_id="postgres_db",
            sql="{{ '/data/sql_tmp/death_records_enriched.sql' if task_instance.xcom_pull(task_ids='extra_death_processing') else '/data/sql_tmp/death_records.sql' }}",
        )

    # -----------------------------------------------------------------
    # Define task dependencies
    # -----------------------------------------------------------------
    start >> ingestion_group
    ingestion_group >> staging_group
    staging_group >> transformation_group
    transformation_group >> branch_death_records

    branch_death_records >> extra_death_processing >> load_group
    branch_death_records >> skip_extra_processing >> load_group

    load_group >> end

    # Ensure all transformation tasks complete before branching
    [
        transform_city_geo,
        transform_nuclear_plants,
        transform_thermal_plants,
        transform_death_records,
    ] >> branch_death_records

    # Ensure all load tasks finish before DAG ends
    [
        load_city_geo,
        load_nuclear_plants,
        load_thermal_plants,
        load_death_records,
    ] >> end