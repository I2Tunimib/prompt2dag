from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="procurement_supplier_validation_pipeline",
    description="Validates and standardizes supplier data by reconciling names against a known database (Wikidata) for improved data quality in procurement systems. Ingests a CSV of basic supplier information, converts it to JSON, reconciles supplier names, and exports the enriched data to CSV.",
    schedule_interval=None,  # Pipeline disabled
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["procurement", "supplier", "validation"],
) as dag:

    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={
            "DATASET_ID": "2",
            "TABLE_NAME_PREFIX": "JOT_",
            "DATA_DIR": "${DATA_DIR}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    reconcile_supplier_names = DockerOperator(
        task_id='reconcile_supplier_names',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={
            "PRIMARY_COLUMN": "supplier_name",
            "RECONCILIATOR_ID": "wikidataEntity",
            "DATASET_ID": "2",
            "DATA_DIR": "${DATA_DIR}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={
            "DATASET_ID": "2",
            "DATA_DIR": "${DATA_DIR}"
        },
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies (sequential pattern)
    load_and_modify_data >> reconcile_supplier_names >> save_final_data