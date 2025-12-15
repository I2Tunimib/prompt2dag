import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'procurement-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='procurement_supplier_validation',
    description='Validates and standardizes supplier data by reconciling names against Wikidata',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['procurement', 'validation', 'wikidata', 'supplier'],
) as dag:
    
    DATA_DIR = os.getenv('DATA_DIR', '/opt/data')
    
    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[Mount(source=DATA_DIR, target='/data', type='bind')],
        environment={
            'DATASET_ID': '2',
            'TABLE_NAME_PREFIX': 'JOT_',
            'DATA_DIR': '/data',
        },
        docker_url='unix://var/run/docker.sock',
        tty=False,
    )
    
    reconcile_entities = DockerOperator(
        task_id='reconcile_entities',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[Mount(source=DATA_DIR, target='/data', type='bind')],
        environment={
            'DATASET_ID': '2',
            'PRIMARY_COLUMN': 'supplier_name',
            'RECONCILIATOR_ID': 'wikidataEntity',
            'DATA_DIR': '/data',
        },
        docker_url='unix://var/run/docker.sock',
        tty=False,
    )
    
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='app_network',
        mounts=[Mount(source=DATA_DIR, target='/data', type='bind')],
        environment={
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        docker_url='unix://var/run/docker.sock',
        tty=False,
    )
    
    load_and_modify >> reconcile_entities >> save_final_data