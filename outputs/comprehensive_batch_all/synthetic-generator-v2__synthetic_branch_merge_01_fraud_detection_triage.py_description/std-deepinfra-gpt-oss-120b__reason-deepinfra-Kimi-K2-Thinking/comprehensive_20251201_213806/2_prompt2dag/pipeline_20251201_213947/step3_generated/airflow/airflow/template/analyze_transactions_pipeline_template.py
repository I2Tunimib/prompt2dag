# ==============================================================================
# Generated Airflow DAG - Fan-Out/Fan-In Pattern
# Pipeline: analyze_transactions_pipeline
# Pattern: fanout_fanin
# Strategy: template
# Generated: 2025-12-01T21:43:20.594963
# ==============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
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
    dag_id='analyze_transactions_pipeline',
    default_args=default_args,
    description='Comprehensive Pipeline Description',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'fanout_fanin'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Identify fan-out and fan-in points

    # Task: analyze_transactions
    analyze_transactions = DockerOperator(
        task_id='analyze_transactions',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: route_transaction
    # ⚡ FAN-OUT POINT: Multiple downstream tasks
    route_transaction = DockerOperator(
        task_id='route_transaction',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: route_to_manual_review
    route_to_manual_review = DockerOperator(
        task_id='route_to_manual_review',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: route_to_auto_approve
    route_to_auto_approve = DockerOperator(
        task_id='route_to_auto_approve',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: send_notification
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=none_failed)
    send_notification = DockerOperator(
        task_id='send_notification',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        trigger_rule='none_failed',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )


    # ==========================================================================
    # Task Dependencies - Fan-Out/Fan-In Pattern
    # ==========================================================================
    # Fan-out points: ['route_transaction']
    # Fan-in points: ['send_notification']

    analyze_transactions >> route_transaction
    route_transaction >> route_to_manual_review
    route_transaction >> route_to_auto_approve
    route_to_manual_review >> send_notification
    route_to_auto_approve >> send_notification
