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
    'procurement_supplier_validation',
    default_args=default_args,
    description='A pipeline to validate and standardize supplier data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def load_and_modify_data():
        # Placeholder for actual data loading and modification logic
        pass

    def reconcile_entities():
        # Placeholder for actual entity reconciliation logic
        pass

    def save_final_data():
        # Placeholder for actual data saving logic
        pass

    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'TABLE_NAME_PREFIX': 'JOT_',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        command='python load_and_modify.py',
    )

    reconcile_entities = DockerOperator(
        task_id='reconcile_entities',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'PRIMARY_COLUMN': 'supplier_name',
            'RECONCILIATOR_ID': 'wikidataEntity',
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        volumes=['/path/to/data:/data'],
        network_mode='app_network',
        command='python reconcile_entities.py',
    )

    save_final_data = DockerOperator(
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
        command='python save_final_data.py',
    )

    load_and_modify >> reconcile_entities >> save_final_data