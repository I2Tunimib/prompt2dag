from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': None,
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def parse_input_parameters(**kwargs):
    """
    Extracts and processes runtime parameters including logical date and description.
    Stores compilation configuration in XCom.
    """
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    params = dag_run.conf.get('parameters', {})
    logical_date = kwargs['logical_date']
    description = params.get('description', 'Default description')
    compilation_config = {
        'logical_date': logical_date.isoformat(),
        'description': description,
        'git_commitish': params.get('git_commitish', 'main')
    }
    ti.xcom_push(key='compilation_config', value=compilation_config)

def create_compilation_result(**kwargs):
    """
    Retrieves the configuration from XCom and executes Dataform compilation.
    """
    ti = kwargs['ti']
    compilation_config = ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
    # Example of how to use the configuration to create a compilation result
    # This is a placeholder for actual Dataform API calls
    compilation_result = {
        'project_id': 'your-project-id',
        'region': 'us-central1',
        'repository_id': 'your-repository-id',
        'compilation_config': compilation_config
    }
    ti.xcom_push(key='compilation_result', value=compilation_result)

with DAG(
    dag_id='dataform_data_transformation_pipeline',
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['dataform', 'data_transformation'],
) as dag:

    start = DummyOperator(task_id='start')

    parse_input_parameters_task = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
        provide_context=True
    )

    create_compilation_result_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
        provide_context=True
    )

    create_workflow_invocation_task = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='your-project-id',
        region='us-central1',
        repository_id='your-repository-id',
        compilation_result= "{{ ti.xcom_pull(task_ids='create_compilation_result', key='compilation_result') }}",
        gcp_conn_id='modelling_cloud_default'
    )

    is_workflow_invocation_done_task = DataformWorkflowInvocationStateSensor(
        task_id='is_workflow_invocation_done',
        project_id='your-project-id',
        region='us-central1',
        repository_id='your-repository-id',
        workflow_invocation_id="{{ ti.xcom_pull(task_ids='create_workflow_invocation', key='workflow_invocation_id') }}",
        expected_statuses=['SUCCEEDED', 'FAILED'],
        poke_interval=60,
        timeout=3600,
        gcp_conn_id='modelling_cloud_default'
    )

    end = DummyOperator(task_id='end')

    start >> parse_input_parameters_task >> create_compilation_result_task >> create_workflow_invocation_task >> is_workflow_invocation_done_task >> end