# ==============================================================================
# Generated Airflow DAG
# Pipeline: load_and_modify_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T04:52:43.535486
# ==============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.helpers import chain
from docker.types import Mount

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/airflow/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Default Arguments ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id='load_and_modify_data_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: load_and_modify_data
    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        command=['python', 'load_and_modify.py'],
        environment={
    'DATASET_ID': '2',
    'DATE_COLUMN': 'submission_date',
    'TABLE_NAME_PREFIX': 'JOT_',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: language_detection
    language_detection = DockerOperator(
        task_id='language_detection',
        image='jmockit/language-detection',
        command=['python', 'detect_language.py'],
        environment={
    'TEXT_COLUMN': 'review_text',
    'LANG_CODE_COLUMN': 'language_code',
    'OUTPUT_FILE': 'lang_detected_2.json',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: sentiment_analysis
    sentiment_analysis = DockerOperator(
        task_id='sentiment_analysis',
        image='huggingface/transformers-inference',
        command=['python', 'sentiment_analysis.py'],
        environment={
    'MODEL_NAME': 'distilbert-base-uncased-finetuned-sst-2-english',
    'TEXT_COLUMN': 'review_text',
    'OUTPUT_COLUMN': 'sentiment_score',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: category_extraction
    category_extraction = DockerOperator(
        task_id='category_extraction',
        image='i2t-backendwithintertwino6-column-extension:latest',
        command=['python', 'category_extraction.py'],
        environment={
    'EXTENDER_ID': 'featureExtractor',
    'TEXT_COLUMN': 'review_text',
    'OUTPUT_COLUMN': 'mentioned_features',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: save_final_data
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        command=['python', 'save_data.py'],
        environment={
    'DATASET_ID': '2',
},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    load_and_modify_data >> language_detection
    language_detection >> sentiment_analysis
    sentiment_analysis >> category_extraction
    category_extraction >> save_final_data
