# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for load_and_modify_data_pipeline
Author: Automated DAG Generator
Date: 2024-06-28
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # You can add more default args such as email_on_failure, etc.
}

# DAG definition
with DAG(
    dag_id="load_and_modify_data_pipeline",
    description="No description provided.",
    schedule_interval=None,  # Disabled schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["load_and_modify", "sequential"],
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
) as dag:

    # -------------------------------------------------------------------------
    # Common DockerOperator configuration
    # -------------------------------------------------------------------------
    DOCKER_URL = os.getenv("DOCKER_HOST", "unix://var/run/docker.sock")
    # Shared volume name defined in the resources section
    DATA_DIR_VOLUME = "data_dir_volume"
    # Custom Docker network defined in the resources section
    DOCKER_NETWORK = "app_network"

    # -------------------------------------------------------------------------
    # Task: Load and Modify Station Data
    # -------------------------------------------------------------------------
    load_and_modify_data = DockerOperator(
        task_id="load_and_modify_data",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        # Mount the shared data directory volume into the container at /data
        mounts=[
            # DockerMount(source=DATA_DIR_VOLUME, target="/data", type="volume")
            # Using the legacy `volumes` parameter for simplicity
            f"{DATA_DIR_VOLUME}:/data"
        ],
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Task: Geocode Reconciliation
    # -------------------------------------------------------------------------
    geocode_reconciliation = DockerOperator(
        task_id="geocode_reconciliation",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mounts=[f"{DATA_DIR_VOLUME}:/data"],
        environment={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": "[HERE API token]",
            "DATASET_ID": "2",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Task: OpenMeteo Weather Data Extension
    # -------------------------------------------------------------------------
    openmeteo_extension = DockerOperator(
        task_id="openmeteo_extension",
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mounts=[f"{DATA_DIR_VOLUME}:/data"],
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "DATE_COLUMN": "installation_date",
            "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Task: Land Use Classification Extension
    # -------------------------------------------------------------------------
    land_use_extension = DockerOperator(
        task_id="land_use_extension",
        image="geoapify-land-use:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mounts=[f"{DATA_DIR_VOLUME}:/data"],
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "land_use_type",
            "API_KEY": "[Geoapify API key]",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Task: Population Density Extension
    # -------------------------------------------------------------------------
    population_density_extension = DockerOperator(
        task_id="population_density_extension",
        image="worldpop-density:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mounts=[f"{DATA_DIR_VOLUME}:/data"],
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "population_density",
            "RADIUS": "5000",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Task: Environmental Risk Calculation
    # -------------------------------------------------------------------------
    environmental_risk_calculation = DockerOperator(
        task_id="environmental_risk_calculation",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mounts=[f"{DATA_DIR_VOLUME}:/data"],
        environment={
            "EXTENDER_ID": "environmentalRiskCalculator",
            "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
            "OUTPUT_COLUMN": "risk_score",
            "CALCULATION_FORMULA": "[risk calculation parameters]",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Task: Save Final Enriched Dataset
    # -------------------------------------------------------------------------
    save_final_data = DockerOperator(
        task_id="save_final_data",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mounts=[f"{DATA_DIR_VOLUME}:/data"],
        environment={
            "DATASET_ID": "2",
        },
        command=None,
        tty=True,
    )

    # -------------------------------------------------------------------------
    # Set task dependencies (sequential execution)
    # -------------------------------------------------------------------------
    load_and_modify_data >> geocode_reconciliation >> openmeteo_extension \
        >> land_use_extension >> population_density_extension \
        >> environmental_risk_calculation >> save_final_data

# End of DAG definition.