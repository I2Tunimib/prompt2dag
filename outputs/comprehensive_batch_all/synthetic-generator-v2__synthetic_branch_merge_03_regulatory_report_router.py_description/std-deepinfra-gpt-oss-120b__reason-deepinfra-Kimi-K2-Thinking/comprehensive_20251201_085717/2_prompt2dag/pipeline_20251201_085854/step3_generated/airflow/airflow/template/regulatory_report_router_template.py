# ==============================================================================
# Generated Airflow DAG - Fan-Out/Fan-In Pattern
# Pipeline: regulatory_report_router
# Pattern: fanout_fanin
# Strategy: template
# Generated: 2025-12-01T09:03:41.014105
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
    dag_id='regulatory_report_router',
    default_args=default_args,
    description='This regulatory reporting pipeline processes financial transaction data and routes it to appropriate regulatory systems based on account type, following a branch‑merge pattern. It extracts CSV data, branches processing for international (FATCA) and domestic (IRS) accounts, runs parallel reporting workflows, and merges results for archival.',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'fanout_fanin'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Identify fan-out and fan-in points

    # Task: extract_transaction_csv
    extract_transaction_csv = DockerOperator(
        task_id='extract_transaction_csv',
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

    # Task: determine_account_routing
    determine_account_routing = DockerOperator(
        task_id='determine_account_routing',
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

    # Task: generate_fatca_report
    generate_fatca_report = DockerOperator(
        task_id='generate_fatca_report',
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

    # Task: generate_irs_report
    generate_irs_report = DockerOperator(
        task_id='generate_irs_report',
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

    # Task: archive_regulatory_reports
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=all_done)
    archive_regulatory_reports = DockerOperator(
        task_id='archive_regulatory_reports',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        trigger_rule='all_done',
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )


    # ==========================================================================
    # Task Dependencies - Fan-Out/Fan-In Pattern
    # ==========================================================================
    # Fan-out points: None detected
    # Fan-in points: ['archive_regulatory_reports']

    extract_transaction_csv >> determine_account_routing
    routing_branch >> generate_fatca_report
    routing_branch >> generate_irs_report
    generate_fatca_report >> archive_regulatory_reports
    generate_irs_report >> archive_regulatory_reports
