from datetime import datetime, timedelta
import logging
import os
import requests
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'environmental-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data')
LOAD_MODIFY_URL = os.getenv('LOAD_MODIFY_URL', 'http://host.docker.internal:3003/load-and-modify')
RECONCILIATION_URL = os.getenv('RECONCILIATION_URL', 'http://host.docker.internal:3003/reconciliation')
OPENMETEO_URL = os.getenv('OPENMETEO_URL', 'http://host.docker.internal:3004/openmeteo-extension')
LAND_USE_URL = os.getenv('LAND_USE_URL', 'http://host.docker.internal:3005/land-use-extension')
POP_DENSITY_URL = os.getenv('POP_DENSITY_URL', 'http://host.docker.internal:3006/population-density-extension')
COLUMN_EXT_URL = os.getenv('COLUMN_EXT_URL', 'http://host.docker.internal:3003/column-extension')
SAVE_URL = os.getenv('SAVE_URL', 'http://host.docker.internal:3003/save')
HERE_API_TOKEN = os.getenv('HERE_API_TOKEN', 'your_here_api_token')
GEOAPIFY_API_KEY = os.getenv('GEOAPIFY_API_KEY', 'your_geoapify_api_key')


def _make_api_request(url, payload, timeout=300):
    """Helper to POST JSON payloads and raise on errors."""
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


with DAG(
    'environmental_monitoring_pipeline',
    default_args=default_args,
    description='Environmental monitoring network data enrichment pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['environmental', 'monitoring'],
) as dag:

    wait_for_stations_csv = FileSensor(
        task_id='wait_for_stations_csv',
        filepath=f'{DATA_DIR}/stations.csv',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    def load_and_modify_data(**kwargs):
        payload = {
            'dataset_id': 2,
            'date_column': 'installation_date',
            'table_name_prefix': 'JOT_',
            'input_file': f'{DATA_DIR}/stations.csv',
            'output_file': f'{DATA_DIR}/table_data_2.json',
        }
        return _make_api_request(LOAD_MODIFY_URL, payload)

    load_and_modify_task = PythonOperator(
        task_id='load_and_modify_data',
        python_callable=load_and_modify_data,
    )

    def reconcile_geocoding(**kwargs):
        payload = {
            'primary_column': 'location',
            'reconciliator_id': 'geocodingHere',
            'api_token': HERE_API_TOKEN,
            'dataset_id': 2,
            'input_file': f'{DATA_DIR}/table_data_2.json',
            'output_file': f'{DATA_DIR}/reconciled_table_2.json',
        }
        return _make_api_request(RECONCILIATION_URL, payload)

    reconcile_geocoding_task = PythonOperator(
        task_id='reconcile_geocoding',
        python_callable=reconcile_geocoding,
    )

    def extend_openmeteo(**kwargs):
        payload = {
            'lat_column': 'latitude',
            'lon_column': 'longitude',
            'date_column': 'installation_date',
            'weather_variables': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
            'date_separator_format': 'YYYYMMDD',
            'input_file': f'{DATA_DIR}/reconciled_table_2.json',
            'output_file': f'{DATA_DIR}/open_meteo_2.json',
        }
        return _make_api_request(OPENMETEO_URL, payload)

    extend_openmeteo_task = PythonOperator(
        task_id='extend_openmeteo_data',
        python_callable=extend_openmeteo,
    )

    def extend_land_use(**kwargs):
        payload = {
            'lat_column': 'latitude',
            'lon_column': 'longitude',
            'output_column': 'land_use_type',
            'api_key': GEOAPIFY_API_KEY,
            'input_file': f'{DATA_DIR}/open_meteo_2.json',
            'output_file': f'{DATA_DIR}/land_use_2.json',
        }
        return _make_api_request(LAND_USE_URL, payload)

    extend_land_use_task = PythonOperator(
        task_id='extend_land_use',
        python_callable=extend_land_use,
    )

    def extend_population_density(**kwargs):
        payload = {
            'lat_column': 'latitude',
            'lon_column': 'longitude',
            'output_column': 'population_density',
            'radius': 5000,
            'input_file': f'{DATA_DIR}/land_use_2.json',
            'output_file': f'{DATA_DIR}/pop_density_2.json',
        }
        return _make_api_request(POP_DENSITY_URL, payload)

    extend_population_density_task = PythonOperator(
        task_id='extend_population_density',
        python_callable=extend_population_density,
    )

    def calculate_environmental_risk(**kwargs):
        payload = {
            'extender_id': 'environmentalRiskCalculator',
            'input_columns': 'precipitation_sum,population_density,land_use_type',
            'output_column': 'risk_score',
            'calculation_formula': 'weighted_risk_score',
            'input_file': f'{DATA_DIR}/pop_density_2.json',
            'output_file': f'{DATA_DIR}/column_extended_2.json',
        }
        return _make_api_request(COLUMN_EXT_URL, payload)

    calculate_risk_task = PythonOperator(
        task_id='calculate_environmental_risk',
        python_callable=calculate_environmental_risk,
    )

    def save_final_data(**kwargs):
        payload = {
            'dataset_id': 2,
            'input_file': f'{DATA_DIR}/column_extended_2.json',
            'output_file': f'{DATA_DIR}/enriched_data_2.csv',
        }
        return _make_api_request(SAVE_URL, payload)

    save_final_data_task = PythonOperator(
        task_id='save_final_data',
        python_callable=save_final_data,
    )

    wait_for_stations_csv >> load_and_modify_task >> reconcile_geocoding_task >> extend_openmeteo_task >> extend_land_use_task >> extend_population_density_task >> calculate_risk_task >> save_final_data_task