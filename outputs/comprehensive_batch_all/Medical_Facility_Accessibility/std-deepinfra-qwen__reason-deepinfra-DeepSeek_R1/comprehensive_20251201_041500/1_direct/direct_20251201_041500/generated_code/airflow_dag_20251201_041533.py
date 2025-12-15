from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'medical_facility_accessibility_pipeline',
    default_args=default_args,
    description='Pipeline to assess medical facility accessibility',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def load_and_modify_data():
        # Placeholder for the actual load and modify data function
        pass

    def reconcile_geocoding():
        # Placeholder for the actual geocoding function
        pass

    def calculate_distance_to_public_transport():
        # Placeholder for the actual distance calculation to public transport function
        pass

    def calculate_distance_to_residential_areas():
        # Placeholder for the actual distance calculation to residential areas function
        pass

    def save_final_data():
        # Placeholder for the actual save final data function
        pass

    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'TABLE_NAME_PREFIX': 'JOT_',
            'INPUT_FILE': '/app/data/facilities.csv',
            'OUTPUT_FILE': '/app/data/table_data_2.json'
        },
        network_mode='app_network',
        volumes=['{{ var.value.DATA_DIR }}:/app/data'],
        docker_url='tcp://docker-socket:2375',
        command='python3 load_and_modify.py',
    )

    reconcile_geocoding = DockerOperator(
        task_id='reconcile_geocoding',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'PRIMARY_COLUMN': 'address',
            'RECONCILIATOR_ID': 'geocodingHere',
            'API_TOKEN': '{{ var.value.HERE_API_TOKEN }}',
            'INPUT_FILE': '/app/data/table_data_2.json',
            'OUTPUT_FILE': '/app/data/reconciled_table_2.json'
        },
        network_mode='app_network',
        volumes=['{{ var.value.DATA_DIR }}:/app/data'],
        docker_url='tcp://docker-socket:2375',
        command='python3 reconcile_geocoding.py',
    )

    calculate_distance_to_public_transport = DockerOperator(
        task_id='calculate_distance_to_public_transport',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'EXTENDER_ID': 'spatialDistanceCalculator',
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'TARGET_LAYER': 'public_transport',
            'TARGET_DATA_SOURCE': '/app/data/transport_stops.geojson',
            'OUTPUT_COLUMN': 'distance_to_pt',
            'INPUT_FILE': '/app/data/reconciled_table_2.json',
            'OUTPUT_FILE': '/app/data/distance_pt_2.json'
        },
        network_mode='app_network',
        volumes=['{{ var.value.DATA_DIR }}:/app/data'],
        docker_url='tcp://docker-socket:2375',
        command='python3 calculate_distance.py',
    )

    calculate_distance_to_residential_areas = DockerOperator(
        task_id='calculate_distance_to_residential_areas',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'EXTENDER_ID': 'spatialDistanceCalculator',
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'TARGET_LAYER': 'residential_areas',
            'TARGET_DATA_SOURCE': '/app/data/residential_areas.geojson',
            'OUTPUT_COLUMN': 'distance_to_residential',
            'INPUT_FILE': '/app/data/distance_pt_2.json',
            'OUTPUT_FILE': '/app/data/column_extended_2.json'
        },
        network_mode='app_network',
        volumes=['{{ var.value.DATA_DIR }}:/app/data'],
        docker_url='tcp://docker-socket:2375',
        command='python3 calculate_distance.py',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'INPUT_FILE': '/app/data/column_extended_2.json',
            'OUTPUT_FILE': '/app/data/enriched_data_2.csv'
        },
        network_mode='app_network',
        volumes=['{{ var.value.DATA_DIR }}:/app/data'],
        docker_url='tcp://docker-socket:2375',
        command='python3 save_data.py',
    )

    load_and_modify >> reconcile_geocoding >> calculate_distance_to_public_transport >> calculate_distance_to_residential_areas >> save_final_data