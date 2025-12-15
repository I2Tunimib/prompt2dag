from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

"""
Medical Facility Accessibility Pipeline DAG.

Processes medical facility location data through containerized tasks to assess
accessibility by calculating distances to public transport and residential areas.
"""

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration - In production, use Airflow Variables/Connections
DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data/medical_facilities')
HERE_API_TOKEN = os.getenv('HERE_API_TOKEN', 'your_here_api_token_placeholder')

# Docker volume mount for shared data directory
data_volume_mount = Mount(
    source=DATA_DIR,
    target='/app/data',
    type='bind'
)

with DAG(
    dag_id='medical_facility_accessibility_pipeline',
    default_args=default_args,
    description='Assesses medical facility accessibility via geocoding and distance calculations',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['medical', 'accessibility', 'geospatial', 'docker'],
) as dag:
    
    # Task 1: Ingest and preprocess facility data from CSV to JSON
    task_load_modify = DockerOperator(
        task_id='load_and_modify_facility_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[data_volume_mount],
        environment={
            'DATASET_ID': '2',
            'TABLE_NAME_PREFIX': 'JOT_',
        },
    )
    
    # Task 2: Geocode facility addresses using HERE API
    task_geocode = DockerOperator(
        task_id='geocode_with_here_api',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[data_volume_mount],
        environment={
            'PRIMARY_COLUMN': 'address',
            'RECONCILIATOR_ID': 'geocodingHere',
            'API_TOKEN': HERE_API_TOKEN,
            'DATASET_ID': '2',
        },
    )
    
    # Task 3: Calculate distance to nearest public transport stop
    task_distance_pt = DockerOperator(
        task_id='calculate_distance_to_public_transport',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[data_volume_mount],
        environment={
            'EXTENDER_ID': 'spatialDistanceCalculator',
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'TARGET_LAYER': 'public_transport',
            'TARGET_DATA_SOURCE': '/app/data/transport_stops.geojson',
            'OUTPUT_COLUMN': 'distance_to_pt',
            'DATASET_ID': '2',
        },
    )
    
    # Task 4: Calculate distance to nearest residential area
    task_distance_residential = DockerOperator(
        task_id='calculate_distance_to_residential_areas',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[data_volume_mount],
        environment={
            'EXTENDER_ID': 'spatialDistanceCalculator',
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'TARGET_LAYER': 'residential_areas',
            'TARGET_DATA_SOURCE': '/app/data/residential_areas.geojson',
            'OUTPUT_COLUMN': 'distance_to_residential',
            'DATASET_ID': '2',
        },
    )
    
    # Task 5: Export final enriched dataset to CSV
    task_save = DockerOperator(
        task_id='save_enriched_dataset',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[data_volume_mount],
        environment={
            'DATASET_ID': '2',
        },
    )
    
    # Define sequential pipeline execution
    task_load_modify >> task_geocode >> task_distance_pt >> task_distance_residential >> task_save