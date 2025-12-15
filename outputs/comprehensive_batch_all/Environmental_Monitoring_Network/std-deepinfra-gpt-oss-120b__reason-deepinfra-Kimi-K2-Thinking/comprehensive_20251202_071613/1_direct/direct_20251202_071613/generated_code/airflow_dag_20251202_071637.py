import os
import subprocess
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------


def _run_docker(image: str, env: dict, network: str = "app_network") -> None:
    """
    Execute a Docker container with the given image and environment variables.

    The container is run with:
    - ``--rm`` to clean up after execution
    - the custom network ``app_network``
    - a bind mount of the ``DATA_DIR`` environment variable to the same path
    - all provided environment variables passed with ``-e``

    Parameters
    ----------
    image: str
        Docker image to run (including tag).
    env: dict
        Environment variables for the container.
    network: str, optional
        Docker network name. Defaults to ``app_network``.
    """
    data_dir = os.getenv("DATA_DIR", "/tmp/data")
    volume_binding = f"{data_dir}:{data_dir}"
    env_options = sum([["-e", f"{k}={v}"] for k, v in env.items()], [])
    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-v",
        volume_binding,
    ] + env_options + [image]

    logging.info("Running Docker command: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


# ----------------------------------------------------------------------
# Task implementations
# ----------------------------------------------------------------------


def load_and_modify(**context):
    """Ingest stations CSV, parse dates, standardize names, and output JSON."""
    env = {
        "DATASET_ID": "2",
        "DATE_COLUMN": "installation_date",
        "TABLE_NAME_PREFIX": "JOT_",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        env=env,
    )


def reconciliation(**context):
    """Geocode station locations using the HERE API."""
    env = {
        "PRIMARY_COLUMN": "location",
        "RECONCILIATOR_ID": "geocodingHere",
        "API_TOKEN": os.getenv("HERE_API_TOKEN", "YOUR_HERE_TOKEN"),
        "DATASET_ID": "2",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-reconciliation:latest",
        env=env,
    )


def open_meteo_extension(**context):
    """Add historical weather data from OpenMeteo."""
    env = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "DATE_COLUMN": "installation_date",
        "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
        "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        env=env,
    )


def land_use_extension(**context):
    """Add land use classification using Geoapify."""
    env = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "OUTPUT_COLUMN": "land_use_type",
        "API_KEY": os.getenv("GEOAPIFY_API_KEY", "YOUR_GEOAPIFY_KEY"),
    }
    _run_docker(
        image="geoapify-land-use:latest",
        env=env,
    )


def population_density_extension(**context):
    """Add population density data for the surrounding area."""
    env = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "OUTPUT_COLUMN": "population_density",
        "RADIUS": "5000",
    }
    _run_docker(
        image="worldpop-density:latest",
        env=env,
    )


def column_extension(**context):
    """Compute environmental risk score based on combined data."""
    env = {
        "EXTENDER_ID": "environmentalRiskCalculator",
        "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
        "OUTPUT_COLUMN": "risk_score",
        "CALCULATION_FORMULA": "DEFAULT",  # Placeholder for actual formula
    }
    _run_docker(
        image="i2t-backendwithintertwino6-column-extension:latest",
        env=env,
    )


def save_final(**context):
    """Export the enriched dataset to CSV."""
    env = {
        "DATASET_ID": "2",
    }
    _run_docker(
        image="i2t-backendwithintertwino6-save:latest",
        env=env,
    )


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="environmental_monitoring_network",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["environment", "data_enrichment"],
) as dag:

    load_task = PythonOperator(
        task_id="load_and_modify",
        python_callable=load_and_modify,
        provide_context=True,
    )

    reconcile_task = PythonOperator(
        task_id="reconciliation",
        python_callable=reconciliation,
        provide_context=True,
    )

    open_meteo_task = PythonOperator(
        task_id="open_meteo_extension",
        python_callable=open_meteo_extension,
        provide_context=True,
    )

    land_use_task = PythonOperator(
        task_id="land_use_extension",
        python_callable=land_use_extension,
        provide_context=True,
    )

    pop_density_task = PythonOperator(
        task_id="population_density_extension",
        python_callable=population_density_extension,
        provide_context=True,
    )

    column_ext_task = PythonOperator(
        task_id="column_extension",
        python_callable=column_extension,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id="save_final",
        python_callable=save_final,
        provide_context=True,
    )

    # Define linear workflow
    load_task >> reconcile_task >> open_meteo_task >> land_use_task >> pop_density_task >> column_ext_task >> save_task