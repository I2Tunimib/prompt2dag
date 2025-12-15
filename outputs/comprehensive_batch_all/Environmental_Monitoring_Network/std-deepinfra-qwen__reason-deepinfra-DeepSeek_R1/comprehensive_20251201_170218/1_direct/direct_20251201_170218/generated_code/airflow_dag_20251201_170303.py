from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_DIR = os.environ.get('DATA_DIR', '/data')

with DAG(
    'environmental_monitoring_pipeline',
    default_args=default_args,
    description='A pipeline to create a comprehensive environmental dataset',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def load_and_modify(**kwargs):
        # Placeholder for the actual load and modify logic
        pass

    load_and_modify_task = DockerOperator(
        task_id='load_and_modify',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': 2,
            'DATE_COLUMN': 'installation_date',
            'TABLE_NAME_PREFIX': 'JOT_',
            'INPUT_FILE': f'{DATA_DIR}/stations.csv',
            'OUTPUT_FILE': f'{DATA_DIR}/table_data_2.json'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 load_and_modify.py',
    )

    def reconcile_geocoding(**kwargs):
        # Placeholder for the actual geocoding logic
        pass

    reconcile_geocoding_task = DockerOperator(
        task_id='reconcile_geocoding',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'PRIMARY_COLUMN': 'location',
            'RECONCILIATOR_ID': 'geocodingHere',
            'API_TOKEN': '[HERE API token]',
            'DATASET_ID': 2,
            'INPUT_FILE': f'{DATA_DIR}/table_data_2.json',
            'OUTPUT_FILE': f'{DATA_DIR}/reconciled_table_2.json'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 reconcile_geocoding.py',
    )

    def extend_openmeteo(**kwargs):
        # Placeholder for the actual OpenMeteo extension logic
        pass

    extend_openmeteo_task = DockerOperator(
        task_id='extend_openmeteo',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'DATE_COLUMN': 'installation_date',
            'WEATHER_VARIABLES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
            'DATE_SEPARATOR_FORMAT': 'YYYYMMDD',
            'INPUT_FILE': f'{DATA_DIR}/reconciled_table_2.json',
            'OUTPUT_FILE': f'{DATA_DIR}/open_meteo_2.json'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 extend_openmeteo.py',
    )

    def extend_land_use(**kwargs):
        # Placeholder for the actual land use extension logic
        pass

    extend_land_use_task = DockerOperator(
        task_id='extend_land_use',
        image='geoapify-land-use:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'OUTPUT_COLUMN': 'land_use_type',
            'API_KEY': '[Geoapify API key]',
            'INPUT_FILE': f'{DATA_DIR}/open_meteo_2.json',
            'OUTPUT_FILE': f'{DATA_DIR}/land_use_2.json'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 extend_land_use.py',
    )

    def extend_population_density(**kwargs):
        # Placeholder for the actual population density extension logic
        pass

    extend_population_density_task = DockerOperator(
        task_id='extend_population_density',
        image='worldpop-density:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'OUTPUT_COLUMN': 'population_density',
            'RADIUS': 5000,
            'INPUT_FILE': f'{DATA_DIR}/land_use_2.json',
            'OUTPUT_FILE': f'{DATA_DIR}/pop_density_2.json'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 extend_population_density.py',
    )

    def extend_environmental_risk(**kwargs):
        # Placeholder for the actual environmental risk extension logic
        pass

    extend_environmental_risk_task = DockerOperator(
        task_id='extend_environmental_risk',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'EXTENDER_ID': 'environmentalRiskCalculator',
            'INPUT_COLUMNS': 'precipitation_sum,population_density,land_use_type',
            'OUTPUT_COLUMN': 'risk_score',
            'CALCULATION_FORMULA': '[risk calculation parameters]',
            'INPUT_FILE': f'{DATA_DIR}/pop_density_2.json',
            'OUTPUT_FILE': f'{DATA_DIR}/column_extended_2.json'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 extend_environmental_risk.py',
    )

    def save_final_data(**kwargs):
        # Placeholder for the actual save logic
        pass

    save_final_data_task = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': 2,
            'INPUT_FILE': f'{DATA_DIR}/column_extended_2.json',
            'OUTPUT_FILE': f'{DATA_DIR}/enriched_data_2.csv'
        },
        network_mode='app_network',
        docker_url='tcp://docker-proxy:2375',
        command='python3 save_final_data.py',
    )

    load_and_modify_task >> reconcile_geocoding_task >> extend_openmeteo_task >> extend_land_use_task >> extend_population_density_task >> extend_environmental_risk_task >> save_final_data_task