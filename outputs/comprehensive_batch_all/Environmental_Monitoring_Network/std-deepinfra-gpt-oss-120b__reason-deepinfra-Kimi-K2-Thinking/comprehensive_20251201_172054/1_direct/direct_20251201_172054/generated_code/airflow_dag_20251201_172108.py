import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_docker(image: str, env_vars: dict, extra_args: list | None = None) -> None:
    """
    Execute a Docker container with the given image, environment variables,
    and optional extra arguments. The container is run with ``--rm`` to
    automatically clean up after execution and attached to the custom
    ``app_network`` network.

    Parameters
    ----------
    image: str
        Docker image to run.
    env_vars: dict
        Environment variables to pass to the container.
    extra_args: list | None
        Additional command‑line arguments for the container.
    """
    data_dir = os.getenv("DATA_DIR", "/tmp/data")
    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        "app_network",
        "-v",
        f"{data_dir}:{data_dir}",
    ]

    for key, value in env_vars.items():
        cmd.extend(["-e", f"{key}={value}"])

    cmd.append(image)

    if extra_args:
        cmd.extend(extra_args)

    subprocess.run(cmd, check=True)


def load_and_modify(**context):
    """Ingest stations CSV, parse dates, standardise names and output JSON."""
    image = "i2t-backendwithintertwino6-load-and-modify:latest"
    env = {
        "DATASET_ID": "2",
        "DATE_COLUMN": "installation_date",
        "TABLE_NAME_PREFIX": "JOT_",
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/stations.csv",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/table_data_2.json",
    }
    _run_docker(image, env)


def reconcile_geocode(**context):
    """Geocode station locations using the HERE API."""
    image = "i2t-backendwithintertwino6-reconciliation:latest"
    env = {
        "PRIMARY_COLUMN": "location",
        "RECONCILIATOR_ID": "geocodingHere",
        "API_TOKEN": os.getenv("HERE_API_TOKEN", "dummy-token"),
        "DATASET_ID": "2",
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/table_data_2.json",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/reconciled_table_2.json",
    }
    _run_docker(image, env)


def open_meteo_extension(**context):
    """Add historical weather data from OpenMeteo."""
    image = "i2t-backendwithintertwino6-openmeteo-extension:latest"
    env = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "DATE_COLUMN": "installation_date",
        "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
        "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/reconciled_table_2.json",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/open_meteo_2.json",
    }
    _run_docker(image, env)


def land_use_extension(**context):
    """Add land‑use classification using Geoapify."""
    image = "geoapify-land-use:latest"
    env = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "OUTPUT_COLUMN": "land_use_type",
        "API_KEY": os.getenv("GEOAPIFY_API_KEY", "dummy-key"),
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/open_meteo_2.json",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/land_use_2.json",
    }
    _run_docker(image, env)


def population_density_extension(**context):
    """Add population density data around each station."""
    image = "worldpop-density:latest"
    env = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "OUTPUT_COLUMN": "population_density",
        "RADIUS": "5000",
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/land_use_2.json",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/pop_density_2.json",
    }
    _run_docker(image, env)


def environmental_calculation(**context):
    """Compute environmental risk score from combined data."""
    image = "i2t-backendwithintertwino6-column-extension:latest"
    env = {
        "EXTENDER_ID": "environmentalRiskCalculator",
        "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
        "OUTPUT_COLUMN": "risk_score",
        "CALCULATION_FORMULA": "dummy-formula",
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/pop_density_2.json",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/column_extended_2.json",
    }
    _run_docker(image, env)


def save_final_data(**context):
    """Export the enriched dataset to CSV."""
    image = "i2t-backendwithintertwino6-save:latest"
    env = {
        "DATASET_ID": "2",
        "INPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/column_extended_2.json",
        "OUTPUT_FILE": f"{os.getenv('DATA_DIR', '/tmp/data')}/enriched_data_2.csv",
    }
    _run_docker(image, env)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="environmental_monitoring_network",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["environment", "data_engineering"],
) as dag:
    load_modify_task = PythonOperator(
        task_id="load_and_modify",
        python_callable=load_and_modify,
    )

    geocode_task = PythonOperator(
        task_id="reconcile_geocode",
        python_callable=reconcile_geocode,
    )

    open_meteo_task = PythonOperator(
        task_id="open_meteo_extension",
        python_callable=open_meteo_extension,
    )

    land_use_task = PythonOperator(
        task_id="land_use_extension",
        python_callable=land_use_extension,
    )

    pop_density_task = PythonOperator(
        task_id="population_density_extension",
        python_callable=population_density_extension,
    )

    env_calc_task = PythonOperator(
        task_id="environmental_calculation",
        python_callable=environmental_calculation,
    )

    save_task = PythonOperator(
        task_id="save_final_data",
        python_callable=save_final_data,
    )

    # Define task dependencies
    load_modify_task >> geocode_task >> open_meteo_task >> land_use_task >> pop_density_task >> env_calc_task >> save_task