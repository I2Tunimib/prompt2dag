"""Environmental Monitoring Network DAG.

This DAG orchestrates a series of Docker‑based services to ingest,
enrich, and export environmental monitoring station data.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# ----------------------------------------------------------------------
# Default arguments for the DAG
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# ----------------------------------------------------------------------
# Helper to retrieve the shared data directory
# ----------------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")  # fallback for local testing

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="environmental_monitoring_network",
    default_args=DEFAULT_ARGS,
    description="Enrich station data with geocoding, weather, land use, and demographics",
    schedule_interval=None,
    catchup=False,
    tags=["environment", "etl"],
) as dag:

    # ------------------------------------------------------------------
    # 1. Load & Modify Data
    # ------------------------------------------------------------------
    load_and_modify = DockerOperator(
        task_id="load_and_modify",
        image="i2t-backendwithintertwino6-load-and-modify:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "DATASET_ID": "2",
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_",
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",  # adjust if entrypoint differs
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # 2. Reconciliation (Geocoding)
    # ------------------------------------------------------------------
    geocode_reconciliation = DockerOperator(
        task_id="geocode_reconciliation",
        image="i2t-backendwithintertwino6-reconciliation:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": os.getenv("HERE_API_TOKEN", "replace-with-token"),
            "DATASET_ID": "2",
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # 3. OpenMeteo Data Extension
    # ------------------------------------------------------------------
    open_meteo_extension = DockerOperator(
        task_id="open_meteo_extension",
        image="i2t-backendwithintertwino6-openmeteo-extension:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "DATE_COLUMN": "installation_date",
            "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # 4. Land Use Extension
    # ------------------------------------------------------------------
    land_use_extension = DockerOperator(
        task_id="land_use_extension",
        image="geoapify-land-use:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "land_use_type",
            "API_KEY": os.getenv("GEOAPIFY_API_KEY", "replace-with-key"),
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # 5. Population Density Extension
    # ------------------------------------------------------------------
    pop_density_extension = DockerOperator(
        task_id="pop_density_extension",
        image="worldpop-density:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "population_density",
            "RADIUS": "5000",
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # 6. Environmental Calculation (Column Extension)
    # ------------------------------------------------------------------
    column_extension = DockerOperator(
        task_id="column_extension",
        image="i2t-backendwithintertwino6-column-extension:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "EXTENDER_ID": "environmentalRiskCalculator",
            "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
            "OUTPUT_COLUMN": "risk_score",
            "CALCULATION_FORMULA": "replace-with-formula",
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # 7. Save Final Data
    # ------------------------------------------------------------------
    save_final = DockerOperator(
        task_id="save_final",
        image="i2t-backendwithintertwino6-save:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="app_network",
        environment={
            "DATASET_ID": "2",
            "DATA_DIR": DATA_DIR,
        },
        command="python /app/main.py",
        mounts=[
            {
                "source": DATA_DIR,
                "target": "/data",
                "type": "bind",
                "read_only": False,
            }
        ],
    )

    # ------------------------------------------------------------------
    # Define task dependencies
    # ------------------------------------------------------------------
    load_and_modify >> geocode_reconciliation >> open_meteo_extension
    open_meteo_extension >> land_use_extension >> pop_density_extension
    pop_density_extension >> column_extension >> save_final