from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='A DAG to process and enrich CSV data through a series of Docker-containerized tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def load_and_modify_task():
        return DockerOperator(
            task_id='load_and_modify_data',
            image='i2t-backendwithintertwino6-load-and-modify:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'DATA_DIR': '/app/data',
                'DATASET_ID': '2',
                'DATE_COLUMN': 'Fecha_id',
                'TABLE_NAMING_CONVENTION': 'JOT_{}',
            },
            network_mode='app_network',
            mounts=['/app/data:/app/data'],
            docker_url='tcp://docker-proxy:2375',
            command='python load_and_modify.py',
        )

    def data_reconciliation_task():
        return DockerOperator(
            task_id='data_reconciliation',
            image='i2t-backendwithintertwino6-reconciliation:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'DATA_DIR': '/app/data',
                'PRIMARY_COLUMN': 'City',
                'OPTIONAL_COLUMNS': 'County,Country',
                'RECONCILIATOR_ID': 'geocodingHere',
                'API_TOKEN': 'your_api_token_here',
            },
            network_mode='app_network',
            mounts=['/app/data:/app/data'],
            docker_url='tcp://docker-proxy:2375',
            command='python reconcile_data.py',
        )

    def openmeteo_extension_task():
        return DockerOperator(
            task_id='openmeteo_data_extension',
            image='i2t-backendwithintertwino6-openmeteo-extension:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'DATA_DIR': '/app/data',
                'WEATHER_ATTRIBUTES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
                'DATE_FORMAT_SEPARATOR': '-',
            },
            network_mode='app_network',
            mounts=['/app/data:/app/data'],
            docker_url='tcp://docker-proxy:2375',
            command='python extend_with_weather.py',
        )

    def column_extension_task():
        return DockerOperator(
            task_id='column_extension',
            image='i2t-backendwithintertwino6-column-extension:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'DATA_DIR': '/app/data',
                'EXTENDER_ID': 'reconciledColumnExt',
            },
            network_mode='app_network',
            mounts=['/app/data:/app/data'],
            docker_url='tcp://docker-proxy:2375',
            command='python extend_columns.py',
        )

    def save_final_data_task():
        return DockerOperator(
            task_id='save_final_data',
            image='i2t-backendwithintertwino6-save:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'DATA_DIR': '/app/data',
            },
            network_mode='app_network',
            mounts=['/app/data:/app/data'],
            docker_url='tcp://docker-proxy:2375',
            command='python save_final_data.py',
        )

    load_and_modify = load_and_modify_task()
    data_reconciliation = data_reconciliation_task()
    openmeteo_extension = openmeteo_extension_task()
    column_extension = column_extension_task()
    save_final_data = save_final_data_task()

    load_and_modify >> data_reconciliation >> openmeteo_extension >> column_extension >> save_final_data