import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


# Default DAG arguments with error handling configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Environment configuration with safe defaults
DATA_DIR = os.environ.get('DATA_DIR', '/opt/airflow/data')
DATASET_ID = os.environ.get('DATASET_ID', '2')
DATE_COLUMN = os.environ.get('DATE_COLUMN', 'Fecha_id')
TABLE_NAMING_CONVENTION = os.environ.get('TABLE_NAMING_CONVENTION', 'JOT_{}')
HERE_API_TOKEN = os.environ.get('HERE_API_TOKEN', 'your_here_api_token')
RECONCILIATOR_ID = os.environ.get('RECONCILIATOR_ID', 'geocodingHere')
PRIMARY_COLUMN = os.environ.get('PRIMARY_COLUMN', 'City')
OPTIONAL_COLUMNS = os.environ.get('OPTIONAL_COLUMNS', 'County,Country')
WEATHER_ATTRIBUTES = os.environ.get(
    'WEATHER_ATTRIBUTES',
    'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours'
)
DATE_SEPARATOR_FORMAT = os.environ.get('DATE_SEPARATOR_FORMAT', '-')
EXTENDER_ID = os.environ.get('EXTENDER_ID', 'reconciledColumnExt')

# Common Docker operator parameters for all tasks
docker_common_params = {
    'api_version': 'auto',
    'auto_remove': True,
    'network_mode': 'app_network',
    'mount_tmp_dir': False,
    'tty': True,
}

# DAG definition
with DAG(
    dag_id='csv_enrichment_pipeline',
    default_args=default_args,
    description='Sequential CSV data enrichment pipeline using Docker containers',
    schedule_interval=None,
    catchup=False,
    tags=['data-processing', 'docker', 'csv-enrichment'],
) as dag:

    # Task 1: Load and Modify Data
    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        volumes=[f'{DATA_DIR}:/app/data'],
        environment={
            'DATA_DIR': '/app/data',
            'DATASET_ID': DATASET_ID,
            'DATE_COLUMN': DATE_COLUMN,
            'TABLE_NAMING_CONVENTION': TABLE_NAMING_CONVENTION,
        },
        **docker_common_params,
    )

    # Task 2: Data Reconciliation
    data_reconciliation = DockerOperator(
        task_id='data_reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        volumes=[f'{DATA_DIR}:/app/data'],
        environment={
            'DATA_DIR': '/app/data',
            'HERE_API_TOKEN': HERE_API_TOKEN,
            'RECONCILIATOR_ID': RECONCILIATOR_ID,
            'PRIMARY_COLUMN': PRIMARY_COLUMN,
            'OPTIONAL_COLUMNS': OPTIONAL_COLUMNS,
        },
        **docker_common_params,
    )

    # Task 3: OpenMeteo Data Extension
    openmeteo_extension = DockerOperator(
        task_id='openmeteo_data_extension',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        volumes=[f'{DATA_DIR}:/app/data'],
        environment={
            'DATA_DIR': '/app/data',
            'WEATHER_ATTRIBUTES': WEATHER_ATTRIBUTES,
            'DATE_SEPARATOR_FORMAT': DATE_SEPARATOR_FORMAT,
        },
        **docker_common_params,
    )

    # Task 4: Column Extension
    column_extension = DockerOperator(
        task_id='column_extension',
        image='i2t-backendwithintertwino6-column-extension:latest',
        volumes=[f'{DATA_DIR}:/app/data'],
        environment={
            'DATA_DIR': '/app/data',
            'EXTENDER_ID': EXTENDER_ID,
        },
        **docker_common_params,
    )

    # Task 5: Save Final Data
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        volumes=[f'{DATA_DIR}:/app/data'],
        environment={
            'DATA_DIR': '/app/data',
        },
        **docker_common_params,
    )

    # Define sequential task dependencies
    load_and_modify >> data_reconciliation >> openmeteo_extension >> column_extension >> save_final_data