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

# Define the DAG
with DAG(
    dag_id='dataform_data_transformation_pipeline',
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
    tags=['dataform', 'data_transformation'],
) as dag:

    # Dummy operator to start the pipeline
    start = DummyOperator(task_id='start')

    # Task to parse input parameters
    def parse_input_parameters(**kwargs):
        ti = kwargs['ti']
        logical_date = kwargs['logical_date']
        description = kwargs['dag_run'].conf.get('description', 'Default Description')
        compilation_config = {
            'logical_date': logical_date,
            'description': description,
        }
        ti.xcom_push(key='compilation_config', value=compilation_config)

    parse_input_parameters_task = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
        provide_context=True,
    )

    # Task to create compilation result
    def create_compilation_result(**kwargs):
        ti = kwargs['ti']
        compilation_config = ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
        # Simulate Dataform compilation
        compilation_result = {
            'compilation_config': compilation_config,
            'status': 'compiled',
        }
        ti.xcom_push(key='compilation_result', value=compilation_result)

    create_compilation_result_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
        provide_context=True,
    )

    # Task to create workflow invocation
    create_workflow_invocation_task = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='your-project-id',
        region='your-region',
        repository_id='your-repository-id',
        compilation_result=DataformCreateWorkflowInvocationOperator.XCOM_RETURN_KEY,
        dag_run_conf_key='compilation_result',
        gcp_conn_id='modelling_cloud_default',
    )

    # Task to monitor workflow invocation state
    is_workflow_invocation_done_task = DataformWorkflowInvocationStateSensor(
        task_id='is_workflow_invocation_done',
        project_id='your-project-id',
        region='your-region',
        repository_id='your-repository-id',
        workflow_invocation_id=create_workflow_invocation_task.output,
        expected_statuses=['SUCCEEDED', 'FAILED'],
        gcp_conn_id='modelling_cloud_default',
    )

    # Dummy operator to end the pipeline
    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> parse_input_parameters_task >> create_compilation_result_task >> create_workflow_invocation_task >> is_workflow_invocation_done_task >> end