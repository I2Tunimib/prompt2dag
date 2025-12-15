import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'environmental-data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants
DATA_DIR = os.getenv('DATA_DIR', '/opt/data')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:3003')
HERE_API_TOKEN = Variable.get('here_api_token', default_var='your_here_api_token')
GEOAPIFY_API_KEY = Variable.get('geoapify_api_key', default_var='your_geoapify_api_key')

# Helper function to make API calls
def call_api_service(endpoint, payload, headers=None):
    url = f"{API_BASE_URL}{endpoint}"
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

# Task functions
def load_and_modify_data(**context):
    payload = {
        'dataset_id': 2,
        'date_column': 'installation_date',
        'table_name_prefix': 'JOT_',
        'input_file': f"{DATA_DIR}/stations.csv",
        'output_file': f"{DATA_DIR}/table_data_2.json"
    }
    call_api_service('/load-and-modify', payload)

def reconciliation_geocoding(**context):
    payload = {
        'dataset_id': 2,
        'primary_column': 'location',
        'reconciliator_id': 'geocodingHere',
        'api_token': HERE_API_TOKEN,
        'input_file': f"{DATA_DIR}/table_data_2.json",
        'output_file': f"{DATA_DIR}/reconciled_table_2.json"
    }
    call_api_service('/reconciliation', payload)

def openmeteo_extension(**context):
    payload = {
        'lat_column': 'latitude',
        'lon_column': 'longitude',
        'date_column': 'installation_date',
        'weather_variables': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
        'date_separator_format': 'YYYYMMDD',
        'input_file': f"{DATA_DIR}/reconciled_table_2.json",
        'output_file': f"{DATA_DIR}/open_meteo_2.json'
    }
    call_api_service('/openmeteo-extension', payload)

def land_use_extension(**context):
    payload = {
        'lat_column': 'latitude',
        'lon_column': 'longitude',
        'output_column': 'land_use_type',
        'api_key': GEOAPIFY_API_KEY,
        'input_file': f"{DATA_DIR}/open_meteo_2.json",
        'output_file': f"{DATA_DIR}/land_use_2.json'
    }
    call_api_service('/land-use-extension', payload)

def population_density_extension(**context):
    payload = {
        'lat_column': 'latitude',
        'lon_column': 'longitude',
        'output_column': 'population_density',
        'radius': 5000,
        'input_file': f"{DATA_DIR}/land_use_2.json",
        'output_file': f"{DATA_DIR}/pop_density_2.json'
    }
    call_api_service('/population-density-extension', payload)

def environmental_calculation(**context):
    payload = {
        'extender_id': 'environmentalRiskCalculator',
        'input_columns': 'precipitation_sum,population_density,land_use_type',
        'output_column': 'risk_score',
        'calculation_formula': 'default_risk_calculation',  # Placeholder
        'input_file': f"{DATA_DIR}/pop_density_2.json",
        'output_file': f"{DATA_DIR}/column_extended_2.json'
    }
    call_api_service('/column-extension', payload)

def save_final_data(**context):
    payload = {
        'dataset_id': 2,
        'input_file': f"{DATA_DIR}/column_extended_2.json",
        'output_file': f"{DATA_DIR}/enriched_data_2.csv'
    }
    call_api_service('/save', payload)

# DAG definition
with DAG(
    'environmental_monitoring_pipeline',
    default_args=default_args,
    description='Environmental monitoring network pipeline for risk analysis',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['environmental', 'monitoring', 'risk-analysis'],
) as dag:

    # Wait for initial input file
    wait_for_stations_csv = FileSensor(
        task_id='wait_for_stations_csv',
        filepath=f"{DATA_DIR}/stations.csv",
        poke_interval=30,
        timeout=600,
    )

    # Step 1: Load & Modify Data
    load_and_modify = PythonOperator(
        task_id='load_and_modify_data',
        python_callable=load_and_modify_data,
    )

    wait_for_table_data_json = FileSensor(
        task_id='wait_for_table_data_json',
        filepath=f"{DATA_DIR}/table_data_2.json",
        poke_interval=30,
        timeout=600,
    )

    # Step 2: Reconciliation (Geocoding)
    reconciliation = PythonOperator(
        task_id='reconciliation_geocoding',
        python_callable=reconciliation_geocoding,
    )

    wait_for_reconciled_table_json = FileSensor(
        task_id='wait_for_reconciled_table_json',
        filepath=f"{DATA_DIR}/reconciled_table_2.json",
        poke_interval=30,
        timeout=600,
    )

    # Step 3: OpenMeteo Data Extension
    openmeteo = PythonOperator(
        task_id='openmeteo_extension',
        python_callable=openmeteo_extension,
    )

    wait_for_open_meteo_json = FileSensor(
        task_id='wait_for_open_meteo_json',
        filepath=f"{DATA_DIR}/open_meteo_2.json",
        poke_interval=30,
        timeout=600,
    )

    # Step 4: Land Use Extension
    land_use = PythonOperator(
        task_id='land_use_extension',
        python_callable=land_use_extension,
    )

    wait_for_land_use_json = FileSensor(
        task_id='wait_for_land_use_json',
        filepath=f"{DATA_DIR}/land_use_2.json",
        poke_interval=30,
        timeout=600,
    )

    # Step 5: Population Density Extension
    population_density = PythonOperator(
        task_id='population_density_extension',
        python_callable=population_density_extension,
    )

    wait_for_pop_density_json = FileSensor(
        task_id='wait_for_pop_density_json',
        filepath=f"{DATA_DIR}/pop_density_2.json",
        poke_interval=30,
        timeout=600,
    )

    # Step 6: Environmental Calculation
    environmental_calc = PythonOperator(
        task_id='environmental_calculation',
        python_callable=environmental_calculation,
    )

    wait_for_column_extended_json = FileSensor(
        task_id='wait_for_column_extended_json',
        filepath=f"{DATA_DIR}/column_extended_2.json",
        poke_interval=30,
        timeout=600,
    )

    # Step 7: Save Final Data
    save_final = PythonOperator(
        task_id='save_final_data',
        python_callable=save_final_data,
    )

    wait_for_enriched_data_csv = FileSensor(
        task_id='wait_for_enriched_data_csv',
        filepath=f"{DATA_DIR}/enriched_data_2.csv",
        poke_interval=30,
        timeout=600,
    )

    # Define dependencies
    wait_for_stations_csv >> load_and_modify >> wait_for_table_data_json
    wait_for_table_data_json >> reconciliation >> wait_for_reconciled_table_json
    wait_for_reconciled_table_json >> openmeteo >> wait_for_open_meteo_json
    wait_for_open_meteo_json >> land_use >> wait_for_land_use_json
    wait_for_land_use_json >> population_density >> wait_for_pop_density_json
    wait_for_pop_density_json >> environmental_calc >> wait_for_column_extended_json
    wait_for_column_extended_json >> save_final >> wait_for_enriched_data_csv