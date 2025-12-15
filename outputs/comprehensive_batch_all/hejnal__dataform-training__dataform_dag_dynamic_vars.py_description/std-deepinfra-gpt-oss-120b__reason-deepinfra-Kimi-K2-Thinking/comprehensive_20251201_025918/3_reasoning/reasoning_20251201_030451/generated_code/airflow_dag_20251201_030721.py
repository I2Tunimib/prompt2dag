from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.state import State

default_args = {
    'owner': 'dataform-team',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=0),
}

with DAG(
    dag_id='dataform_transformation_pipeline',
    default_args=default_args,
    description='Linear Dataform transformation pipeline triggered by upstream ingestion',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['dataform', 'transformation'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    wait_for_upstream_ingestion = ExternalTaskSensor(
        task_id='wait_for_upstream_ingestion',
        external_dag_id='dataform-training-data-ingestion',
        external_task_id=None,
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED, State.UPSTREAM_FAILED],
        timeout=3600,
        poke_interval=60,
        mode='poke',
    )
    
    def parse_input_parameters(**context):
        """Parse runtime parameters and store compilation config in XCom."""
        dag_run = context['dag_run']
        conf = dag_run.conf or {}
        
        logical_date = context['ds']
        description = conf.get('description', f'Dataform transformation for {logical_date}')
        git_commitish = conf.get('git_commitish', 'main')
        project_id = conf.get('project_id', 'your-gcp-project')
        region = conf.get('region', 'us-central1')
        repository_id = conf.get('repository_id', 'your-dataform-repo')
        
        compilation_config = {
            'git_commitish': git_commitish,
            'project_id': project_id,
            'region': region,
            'repository_id': repository_id,
            'logical_date': logical_date,
            'description': description,
        }
        
        context['task_instance'].xcom_push(key='compilation_config', value=compilation_config)
        return compilation_config
    
    parse_input_parameters_task = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
    )
    
    def create_compilation_result(**context):
        """Create Dataform compilation result and return its resource name."""
        ti = context['task_instance']
        config = ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
        
        # In production, create via Dataform API; here we construct the resource name
        compilation_result_name = (
            f"projects/{config['project_id']}/locations/{config['region']}/"
            f"repositories/{config['repository_id']}/compilationResults/{context['run_id']}"
        )
        
        ti.xcom_push(key='compilation_result_name', value=compilation_result_name)
        return compilation_result_name
    
    create_compilation_result_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
    )
    
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='{{ dag_run.conf.get("project_id", "your-gcp-project") }}',
        region='{{ dag_run.conf.get("region", "us-central1") }}',
        repository_id='{{ dag_run.conf.get("repository_id", "your-dataform-repo") }}',
        workflow_invocation={
            'compilation_result': (
                '{{ ti.xcom_pull(task_ids="create_compilation_result", key="compilation_result_name") }}'
            ),
            'invocation_config': {
                'included_tags': ['production'],
            }
        },
        wait_for_completion=False,
        gcp_conn_id='modelling_cloud_default',
    )
    
    wait_for_workflow_completion = DataformWorkflowInvocationStateSensor(
        task_id='wait_for_workflow_completion',
        project_id='{{ dag_run.conf.get("project_id", "your-gcp-project") }}',
        region='{{ dag_run.conf.get("region", "us-central1") }}',
        repository_id='{{ dag_run.conf.get("repository_id", "your-dataform-repo") }}',
        workflow_invocation_id='{{ ti.xcom_pull(task_ids="create_workflow_invocation", key="workflow_invocation_id") }}',
        expected_states={'SUCCEEDED'},
        failure_states={'FAILED', 'CANCELLED'},
        poke_interval=60,
        timeout=3600,
        gcp_conn_id='modelling_cloud_default',
    )
    
    end = DummyOperator(task_id='end')
    
    # Linear task dependencies
    start >> wait_for_upstream_ingestion >> parse_input_parameters_task >> \
        create_compilation_result_task >> create_workflow_invocation >> \
        wait_for_workflow_completion >> end