# ==============================================================================
# Generated Airflow DAG
# Pipeline: Multilingual Product Review Analysis Pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T11:18:13.045049
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
    dag_id='Multilingual Product Review Analysis Pipeline',
    default_args=default_args,
    description='Enriches product reviews with language verification, sentiment, and key feature extraction using LLM capabilities for deeper customer insight.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: load_and_modify_reviews
    load_and_modify_reviews = DockerOperator(
        task_id='load_and_modify_reviews',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: detect_review_language
    detect_review_language = DockerOperator(
        task_id='detect_review_language',
        image='jmockit/language-detection',
        environment={},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: analyze_sentiment
    analyze_sentiment = DockerOperator(
        task_id='analyze_sentiment',
        image='huggingface/transformers-inference',
        environment={},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: extract_review_features
    extract_review_features = DockerOperator(
        task_id='extract_review_features',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={},
        network_mode='app_network',
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: save_enriched_reviews
    save_enriched_reviews = DockerOperator(
        task_id='save_enriched_reviews',
        image='i2t-backendwithintertwino6-save:latest',
        environment={},
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
    load_and_modify_reviews >> detect_review_language
    detect_review_language >> analyze_sentiment
    analyze_sentiment >> extract_review_features
    extract_review_features >> save_enriched_reviews
