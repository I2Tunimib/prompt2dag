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
    description='A DAG to process and enrich raw CSV data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def load_and_modify_data():
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
            mounts=['/host/data:/app/data'],
            mount_tmp_dir=False,
        )

    def reconcile_data():
        return DockerOperator(
            task_id='reconcile_data',
            image='i2t-backendwithintertwino6-reconciliation:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'INPUT_FILES': '/app/data/table_data_*.json',
                'PRIMARY_COLUMN': 'City',
                'OPTIONAL_COLUMNS': 'County,Country',
                'RECONCILIATOR_ID': 'geocodingHere',
                'API_TOKEN': 'your_api_token_here',
            },
            network_mode='app_network',
            mounts=['/host/data:/app/data'],
            mount_tmp_dir=False,
        )

    def extend_with_openmeteo_data():
        return DockerOperator(
            task_id='extend_with_openmeteo_data',
            image='i2t-backendwithintertwino6-openmeteo-extension:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'INPUT_FILES': '/app/data/reconciled_table_*.json',
                'WEATHER_ATTRIBUTES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
                'DATE_FORMAT_SEPARATOR': '-',
            },
            network_mode='app_network',
            mounts=['/host/data:/app/data'],
            mount_tmp_dir=False,
        )

    def extend_columns():
        return DockerOperator(
            task_id='extend_columns',
            image='i2t-backendwithintertwino6-column-extension:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'INPUT_FILES': '/app/data/open_meteo_*.json',
                'EXTENDER_ID': 'reconciledColumnExt',
            },
            network_mode='app_network',
            mounts=['/host/data:/app/data'],
            mount_tmp_dir=False,
        )

    def save_final_data():
        return DockerOperator(
            task_id='save_final_data',
            image='i2t-backendwithintertwino6-save:latest',
            api_version='auto',
            auto_remove=True,
            environment={
                'INPUT_FILES': '/app/data/column_extended_*.json',
                'OUTPUT_DIR': '/app/data',
                'OUTPUT_FILE_NAME': 'enriched_data_{}.csv',
            },
            network_mode='app_network',
            mounts=['/host/data:/app/data'],
            mount_tmp_dir=False,
        )

    load_and_modify = load_and_modify_data()
    reconcile = reconcile_data()
    extend_openmeteo = extend_with_openmeteo_data()
    extend_columns_task = extend_columns()
    save_final = save_final_data()

    load_and_modify >> reconcile >> extend_openmeteo >> extend_columns_task >> save_final