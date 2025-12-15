"""
Procurement Supplier Validation Pipeline DAG.
Validates and standardizes supplier data by reconciling names against Wikidata.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from docker.types import Mount
import os

default_args = {
    'owner': 'procurement-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data')

with DAG(
    dag_id='procurement_supplier_validation',
    default_args=default_args,
    description='Validates and standardizes supplier data by reconciling names against Wikidata',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['procurement', 'validation', 'wikidata'],
) as dag:
    
    # Wait for the input supplier CSV file to be available
    wait_for_supplier_file = FileSensor(
        task_id='wait_for_supplier_file',
        filepath=f'{DATA_DIR}/suppliers.csv',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )
    
    # Step 1: Load and modify supplier data
    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        networks=['app_network'],
        environment={
            'DATASET_ID': '2',
            'TABLE_NAME_PREFIX': 'JOT_',
            'DATA_DIR': '/data',
        },
        mounts=[Mount(source=DATA_DIR, target='/data', type='bind')],
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
    )
    
    # Step 2: Reconcile entities against Wikidata
    reconcile_entities = DockerOperator(
        task_id='reconcile_entities',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        networks=['app_network'],
        environment={
            'PRIMARY_COLUMN': 'supplier_name',
            'RECONCILIATOR_ID': 'wikidataEntity',
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        mounts=[Mount(source=DATA_DIR, target='/data', type='bind')],
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
    )
    
    # Step 3: Save final enriched data
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        networks=['app_network'],
        environment={
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        mounts=[Mount(source=DATA_DIR, target='/data', type='bind')],
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
    )
    
    # Define pipeline flow
    wait_for_supplier_file >> load_and_modify >> reconcile_entities >> save_final_data