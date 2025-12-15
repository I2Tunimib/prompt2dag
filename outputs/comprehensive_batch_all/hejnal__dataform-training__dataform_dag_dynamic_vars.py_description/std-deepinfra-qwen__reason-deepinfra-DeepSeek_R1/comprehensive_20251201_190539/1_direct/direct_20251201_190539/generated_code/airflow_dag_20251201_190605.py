from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': 0,
    'catchup': False,
    'max_active_runs': 1,
}

# Function to parse input parameters and store in XCom
def parse_input_parameters(**context):
    logical_date = context['logical_date']
    description = context['dag_run'].conf.get('description', 'Default Description')
    compilation_config = {
        'logical_date': logical_date.isoformat(),
        'description': description,
    }
    context['ti'].xcom_push(key='compilation_config', value=compilation_config)

# Function to create compilation result using parsed parameters
def create_compilation_result(**context):
    compilation_config = context['ti'].xcom_pull(key='compilation_config')
    # Example of creating a compilation result (this would be replaced with actual Dataform API calls)
    compilation_result = {
        'project_id': 'your-project-id',
        'region': 'your-region',
        'repository_id': 'your-repository-id',
        'compilation_config': compilation_config,
    }
    context['ti'].xcom_push(key='compilation_result', value=compilation_result)

# Define the DAG
with DAG(
    dag_id='dataform_data_transformation_pipeline',
    default_args=default_args,
    schedule_interval='@once',  # Replace with actual dataset-based scheduling
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    parse_input_parameters_task = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
        provide_context=True,
    )

    create_compilation_result_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
        provide_context=True,
    )

    create_workflow_invocation_task = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='your-project-id',
        region='your-region',
        repository_id='your-repository-id',
        compilation_result= "{{ ti.xcom_pull(task_ids='create_compilation_result', key='compilation_result') }}",
        google_impersonation_chain='modelling_cloud_default',
    )

    is_workflow_invocation_done_task = DataformWorkflowInvocationStateSensor(
        task_id='is_workflow_invocation_done',
        project_id='your-project-id',
        region='your-region',
        repository_id='your-repository-id',
        workflow_invocation_id="{{ task_instance.xcom_pull(task_ids='create_workflow_invocation', key='return_value')['name'].split('/')[-1] }}",
        expected_statuses=['SUCCEEDED', 'FAILED'],
        poke_interval=60,
        timeout=3600,
        google_impersonation_chain='modelling_cloud_default',
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> parse_input_parameters_task >> create_compilation_result_task >> create_workflow_invocation_task >> is_workflow_invocation_done_task >> end