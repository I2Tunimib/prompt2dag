from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_enrichment_pipeline',
    default_args=default_args,
    description='Sequential Docker-based data enrichment pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['data-pipeline', 'docker', 'enrichment'],
) as dag:
    
    DATA_MOUNT = '{{ var.value.host_data_dir }}:/app/data:rw'
    
    load_and_modify = DockerOperator(
        task_id='load_and_modify_data',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='app_network',
        mounts=[DATA_MOUNT],
        environment={
            'DATA_DIR': '/app/data',
            'DATASET_ID': '2',
            'DATE_COLUMN': 'Fecha_id',
            'TABLE_PREFIX': 'JOT_',
        },
        container_name='load_and_modify_{{ ts_nodash }}',
        tty=True,
        mount_tmp_dir=False,
    )
    
    reconciliation = DockerOperator(
        task_id='data_reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='app_network',
        mounts=[DATA_MOUNT],
        environment={
            'DATA_DIR': '/app/data',
            'PRIMARY_COLUMN': 'City',
            'OPTIONAL_COLUMNS': 'County,Country',
            'RECONCILIATOR_ID': 'geocodingHere',
            'API_TOKEN': '{{ var.value.here_api_token }}',
        },
        container_name='reconciliation_{{ ts_nodash }}',
        tty=True,
        mount_tmp_dir=False,
    )
    
    openmeteo_extension = DockerOperator(
        task_id='openmeteo_data_extension',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='app_network',
        mounts=[DATA_MOUNT],
        environment={
            'DATA_DIR': '/app/data',
            'WEATHER_ATTRIBUTES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
            'DATE_SEPARATOR_FORMAT': '-',
        },
        container_name='openmeteo_extension_{{ ts_nodash }}',
        tty=True,
        mount_tmp_dir=False,
    )
    
    column_extension = DockerOperator(
        task_id='column_extension',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='app_network',
        mounts=[DATA_MOUNT],
        environment={
            'DATA_DIR': '/app/data',
            'EXTENDER_ID': 'reconciledColumnExt',
        },
        container_name='column_extension_{{ ts_nodash }}',
        tty=True,
        mount_tmp_dir=False,
    )
    
    save_final_data = DockerOperator(
        task_id='save_final_data',
        image='i2t-backendwithintertwino6-save:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='app_network',
        mounts=[DATA_MOUNT],
        environment={
            'DATA_DIR': '/app/data',
            'OUTPUT_DIR': '/app/data',
        },
        container_name='save_final_data_{{ ts_nodash }}',
        tty=True,
        mount_tmp_dir=False,
    )
    
    load_and_modify >> reconciliation >> openmeteo_extension >> column_extension >> save_final_data