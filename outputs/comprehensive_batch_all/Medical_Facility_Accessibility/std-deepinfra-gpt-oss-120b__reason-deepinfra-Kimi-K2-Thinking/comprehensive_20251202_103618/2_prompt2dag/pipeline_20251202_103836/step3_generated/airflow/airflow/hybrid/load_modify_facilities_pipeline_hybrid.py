from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_modify_facilities_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["pipeline"],
    description="No description provided.",
) as dag:

    load_modify_facilities = DockerOperator(
        task_id='load_modify_facilities',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    geocode_facilities = DockerOperator(
        task_id='geocode_facilities',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={
            "PRIMARY_COLUMN": "address",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": "[HERE API token]",
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    calculate_distance_to_public_transport = DockerOperator(
        task_id='calculate_distance_to_public_transport',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "public_transport",
            "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
            "OUTPUT_COLUMN": "distance_to_pt",
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    calculate_distance_to_residential_areas = DockerOperator(
        task_id='calculate_distance_to_residential_areas',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "residential_areas",
            "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
            "OUTPUT_COLUMN": "distance_to_residential",
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_facilities_accessibility = DockerOperator(
        task_id='save_facilities_accessibility',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            "DATASET_ID": "2"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    load_modify_facilities >> geocode_facilities >> calculate_distance_to_public_transport >> calculate_distance_to_residential_areas >> save_facilities_accessibility