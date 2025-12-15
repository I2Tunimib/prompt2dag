from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    'environmental_monitoring_pipeline',
    default_args=default_args,
    description='A pipeline to create a comprehensive environmental dataset',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def _load_and_modify(**kwargs):
        # Placeholder for the actual load and modify logic
        pass

    def _reconciliation(**kwargs):
        # Placeholder for the actual reconciliation logic
        pass

    def _openmeteo_extension(**kwargs):
        # Placeholder for the actual OpenMeteo extension logic
        pass

    def _land_use_extension(**kwargs):
        # Placeholder for the actual land use extension logic
        pass

    def _population_density_extension(**kwargs):
        # Placeholder for the actual population density extension logic
        pass

    def _environmental_calculation(**kwargs):
        # Placeholder for the actual environmental calculation logic
        pass

    def _save_final_data(**kwargs):
        # Placeholder for the actual save final data logic
        pass

    load_and_modify = DockerOperator(
        task_id='load_and_modify',
        image='i2t-backendwithintertwino6-load-and-modify:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'DATASET_ID': '2',
            'DATE_COLUMN': 'installation_date',
            'TABLE_NAME_PREFIX': 'JOT_',
            'DATA_DIR': '/data',
        },
        network_mode='app_network',
        command='python3 -m load_and_modify_service',
    )

    reconciliation = DockerOperator(
        task_id='reconciliation',
        image='i2t-backendwithintertwino6-reconciliation:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'PRIMARY_COLUMN': 'location',
            'RECONCILIATOR_ID': 'geocodingHere',
            'API_TOKEN': '[HERE API token]',
            'DATASET_ID': '2',
            'DATA_DIR': '/data',
        },
        network_mode='app_network',
        command='python3 -m reconciliation_service',
    )

    openmeteo_extension = DockerOperator(
        task_id='openmeteo_extension',
        image='i2t-backendwithintertwino6-openmeteo-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'DATE_COLUMN': 'installation_date',
            'WEATHER_VARIABLES': 'apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours',
            'DATE_SEPARATOR_FORMAT': 'YYYYMMDD',
            'DATA_DIR': '/data',
        },
        network_mode='app_network',
        command='python3 -m openmeteo_extension_service',
    )

    land_use_extension = DockerOperator(
        task_id='land_use_extension',
        image='geoapify-land-use:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'OUTPUT_COLUMN': 'land_use_type',
            'API_KEY': '[Geoapify API key]',
            'DATA_DIR': '/data',
        },
        network_mode='app_network',
        command='python3 -m land_use_extension_service',
    )

    population_density_extension = DockerOperator(
        task_id='population_density_extension',
        image='worldpop-density:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'LAT_COLUMN': 'latitude',
            'LON_COLUMN': 'longitude',
            'OUTPUT_COLUMN': 'population_density',
            'RADIUS': '5000',
            'DATA_DIR': '/data',
        },
        network_mode='app_network',
        command='python3 -m population_density_extension_service',
    )

    environmental_calculation = DockerOperator(
        task_id='environmental_calculation',
        image='i2t-backendwithintertwino6-column-extension:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'EXTENDER_ID': 'environmentalRiskCalculator',
            'INPUT_COLUMNS': 'precipitation_sum,population_density,land_use_type',
            'OUTPUT_COLUMN': 'risk_score',
            'CALCULATION_FORMULA': '[risk calculation parameters]',
            'DATA_DIR': '/data',
        },
        network_mode='app_network',
        command='python3 -m environmental_calculation_service',
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
        network_mode='app_network',
        command='python3 -m save_service',
    )

    load_and_modify >> reconciliation >> openmeteo_extension >> land_use_extension >> population_density_extension >> environmental_calculation >> save_final_data