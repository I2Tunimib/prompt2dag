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
    'environmental_monitoring_pipeline',
    default_args=default_args,
    description='A pipeline to create a comprehensive environmental dataset',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def load_and_modify_data():
        # Placeholder for actual implementation
        pass

    def reconcile_geocoding():
        # Placeholder for actual implementation
        pass

    def extend_openmeteo_data():
        # Placeholder for actual implementation
        pass

    def extend_land_use_data():
        # Placeholder for actual implementation
        pass

    def extend_population_density():
        # Placeholder for actual implementation
        pass

    def compute_environmental_risk():
        # Placeholder for actual implementation
        pass

    def save_final_data():
        # Placeholder for actual implementation
        pass

    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'DATE_COLUMN': 'installation_date',
            'TABLE_NAME_PREFIX': 'JOT_',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 load_and_modify.py',
    )

    reconcile_geocoding = DockerOperator(
        task_id='reconcile_geocoding',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'PRIMARY_COLUMN': 'location',
            'RECONCILIATOR_ID': 'geocodingHere',
            'API_TOKEN': '[HERE API token]',
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 reconcile_geocoding.py',
    )

    extend_openmeteo = DockerOperator(
        task_id='extend_openmeteo_data',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'DATE_COLUMN': 'installation_date',
            'WEATHER_VARIABLES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
            'DATE_SEPARATOR_FORMAT': 'YYYYMMDD',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 extend_openmeteo.py',
    )

    extend_land_use = DockerOperator(
        task_id='extend_land_use_data',
        image='geoapify-land-use:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'OUTPUT_COLUMN': 'land_use_type',
            'API_KEY': '[Geoapify API key]',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 extend_land_use.py',
    )

    extend_population_density = DockerOperator(
        task_id='extend_population_density',
        image='worldpop-density:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'OUTPUT_COLUMN': 'population_density',
            'RADIUS': '5000',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 extend_population_density.py',
    )

    compute_risk = DockerOperator(
        task_id='compute_environmental_risk',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'EXTENDER_ID': 'environmentalRiskCalculator',
            'INPUT_COLUMNS': 'precipitation_sum,population_density,land_use_type',
            'OUTPUT_COLUMN': 'risk_score',
            'CALCULATION_FORMULA': '[risk calculation parameters]',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 compute_risk.py',
    )

    save_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        docker_url='tcp://docker-socket:2375',
        command='python3 save_data.py',
    )

    load_and_modify >> reconcile_geocoding >> extend_openmeteo >> extend_land_use >> extend_population_density >> compute_risk >> save_data