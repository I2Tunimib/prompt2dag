from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="load_and_modify_data_pipeline",
    default_args=default_args,
    description="Sequential pipeline to load, modify, reconcile, enrich, extend and save data.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sequential"],
) as dag:

    load_and_modify_data = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    reconcile_city_names = DockerOperator(
        task_id='reconcile_city_names',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    enrich_with_openmeteo = DockerOperator(
        task_id='enrich_with_openmeteo',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extend_columns = DockerOperator(
        task_id='extend_columns',
        image='i2t-backendwithintertwino6-column-extension:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        environment={},
        network_mode='app_network',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set sequential dependencies
    load_and_modify_data >> reconcile_city_names >> enrich_with_openmeteo >> extend_columns >> save_final_data