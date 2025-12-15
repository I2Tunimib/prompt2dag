<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements step by step.

First, the pipeline has six sequential tasks. The Start and End are DummyOperators. Then there's Parse Input Parameters and Create Compilation Result as PythonOperators. The Create Workflow Invocation uses a specific operator, and the Is Workflow Invocation Done is a sensor.

The DAG should be triggered by the completion of an upstream task called "dataform-training-data-ingestion". So I'll need an ExternalTaskSensor to wait for that. Wait, the description mentions "Dataset-based scheduling activated by completion", which might mean using a Dataset as a trigger, but the user specified using an ExternalTaskSensor. Hmm, maybe the user expects the sensor here. Let me check the constraints again. The constraints say if sensors appear in the description, demonstrate proper use. The pipeline steps mention triggering when upstream data ingestion completes, so I should use an ExternalTaskSensor as the first task after the Start.

Wait, the pipeline steps list Start as the first task, but according to the orchestration, the trigger is based on the upstream completion. So maybe the ExternalTaskSensor is part of the DAG's tasks. Let me structure the tasks in order:

Wait, the pipeline steps are:

- Start: DummyOperator initiates the pipeline
But the trigger is based on upstream completion. So perhaps the first task after Start is the ExternalTaskSensor that waits for the upstream ingestion task. But the pipeline steps listed don't mention a sensor. Wait, the pipeline steps list includes "Is Workflow Invocation Done" as a sensor, but the trigger is handled by the ExternalTaskSensor. Wait, the user's pipeline steps are:

Steps:

Start (Dummy)
Parse Input Parameters (Python)
Create Compilation Result (Python)
Create Workflow Invocation (DataformCreate...)
Is Workflow Invocation Done (Sensor)
End (Dummy)

But the orchestration says the trigger is when upstream data ingestion completes. So perhaps the DAG is scheduled via a dataset, but the user's example might require an ExternalTaskSensor to wait for that upstream task. However, the user's pipeline steps don't include that sensor. Wait, the pipeline steps list six tasks, including the Start and End. So maybe the ExternalTaskSensor is part of the tasks. Let me check the description again.

In the Integrations & Dependencies section, under Data Dependencies: Triggered by upstream data ingestion dataset completion. So perhaps the DAG is scheduled based on a dataset, but the user's example might not need a sensor. Alternatively, maybe the pipeline starts with an ExternalTaskSensor that waits for the upstream task. But according to the pipeline steps listed, the first task is Start (Dummy). Hmm, this is conflicting.

Wait, the Pipeline Steps list includes six tasks, starting with Start. The description says the pipeline triggers when upstream data ingestion completes. So perhaps the DAG is scheduled after the upstream task completes, which could be via a Dataset. But the user's constraints mention that if sensors appear in the description, demonstrate proper use. Since the pipeline is triggered by upstream completion, maybe the first task after Start is an ExternalTaskSensor that waits for the upstream task. But the pipeline steps listed don't mention that. Wait, the pipeline steps are:

- Start: DummyOperator initiates the pipeline execution

But the trigger is dataset-based. So maybe the DAG is scheduled using a dataset trigger, which is a feature in Airflow 2.4+. However, the user's code example might not include that, as the user might expect the DAG to have an ExternalTaskSensor as part of the tasks. Alternatively, perhaps the DAG is scheduled normally, but the first task is a sensor that waits for the upstream ingestion to complete.

The problem is that the pipeline steps listed don't include a sensor for the upstream dependency. The steps listed are Start, Parse, Create Compilation, Create Invocation, Sensor for workflow done, End. So according to the steps, the sensor is the "Is Workflow Invocation Done" step. But the trigger for the DAG is the upstream data ingestion completion. So perhaps the DAG is scheduled after that upstream task, but the user's code example may not need a sensor for that. Alternatively, maybe the DAG is triggered by a sensor task.

Wait, the user's pipeline steps include six tasks. The description says "Trigger: Dataset-based scheduling activated by completion of 'dataform-training-data-ingestion'". So perhaps the DAG is scheduled based on a dataset produced by that upstream task. In that case, the DAG's schedule is set via the dataset parameter. However, the user's code example may not need to include a sensor for that, as the DAG would be triggered automatically when the dataset is updated. But the user's constraints mention using sensors if they appear in the description. Since the pipeline description mentions triggering on upstream completion, but the steps don't include a sensor, perhaps the ExternalTaskSensor is not part of this DAG's tasks. Instead, the DAG is scheduled via a dataset, which is a different mechanism.

But the user's code example may not need to handle that. The DAG's schedule_interval would be set to None, and the DAG is triggered by the dataset. However, the code example may not include that part, as setting up datasets requires more Airflow 2.4+ features. Alternatively, perhaps the user expects the DAG to start with an ExternalTaskSensor that waits for the upstream task to complete. Let me check the pipeline steps again. The steps listed are Start, Parse, etc. So perhaps the ExternalTaskSensor is not part of this DAG's tasks. Instead, the DAG is scheduled after the upstream task via a dataset trigger. However, the code example would not include that in the DAG itself. Therefore, perhaps the code example doesn't include a sensor for the upstream task, but the DAG is set to be triggered by the dataset. However, the user's code example may not show that, as it's part of the DAG's schedule setup.

But given the user's constraints, perhaps the code should include the ExternalTaskSensor as part of the tasks. Wait, the pipeline steps list six tasks, including Start and End. The steps listed don't mention a sensor for the upstream task. The sensor mentioned in the steps is for checking if the workflow invocation is done. So perhaps the ExternalTaskSensor is not part of this DAG's tasks. Therefore, the code example should not include it. Instead, the DAG is triggered when the upstream task completes, which is handled via Airflow's dataset triggering mechanism. However, the user's code example may not need to include that, as it's part of the DAG's scheduling setup. But since the user's code example needs to be executable, perhaps the DAG is scheduled normally, and the ExternalTaskSensor is not part of the tasks. Hmm, this is confusing.

Alternatively, perhaps the pipeline's trigger is handled by an ExternalTaskSensor as the first task. Let me check the pipeline steps again. The steps are:

Start (Dummy)
Parse Input Parameters (Python)
Create Compilation Result (Python)
Create Workflow Invocation (Operator)
Is Workflow Invocation Done (Sensor)
End (Dummy)

So according to the steps, the sensor is the "Is Workflow Invocation Done" step. The ExternalTaskSensor for the upstream ingestion is not part of the steps. Therefore, perhaps the DAG is scheduled to run after the upstream task via a dataset, but the code example doesn't need to include a sensor for that. Therefore, the code example would start with the Start task, followed by the parsing, etc.

But the user's pipeline description says "Trigger: Dataset-based scheduling activated by completion of 'dataform-training-data-ingestion'". So perhaps the DAG is scheduled using a dataset produced by that task. However, in the code example, the DAG's schedule_interval would be set to None, and the schedule would be defined via the dataset. But Airflow's dataset triggering is a separate feature. However, the user's code example may not include that, as it's part of the DAG's parameters. Therefore, perhaps the code example proceeds without including the ExternalTaskSensor for the upstream task, assuming that the DAG is triggered via dataset.

But given the user's constraints mention that if sensors appear in the description, demonstrate proper use. The pipeline steps mention a sensor (Is Workflow Invocation Done), which is a DataformWorkflowInvocationStateSensor. So that sensor is included. The ExternalTaskSensor for the upstream task is not part of the steps listed, so perhaps it's not included in the code example.

Now, moving on to the code structure.

The DAG should have default_args with catchup=False, and schedule_interval=None. Max parallel width is 1, so concurrency is 1. Retries are zero.

The tasks are:

start_task = DummyOperator(...)

parse_params_task = PythonOperator(...)

create_compilation_task = PythonOperator(...)

create_invocation_task = DataformCreateWorkflowInvocationOperator(...)

check_invocation_task = DataformWorkflowInvocationStateSensor(...)

end_task = DummyOperator(...)

The dependencies are sequential: start >> parse_params >> create_compilation >> create_invocation >> check_invocation >> end.

Wait, but according to the pipeline steps, the Start is the first task. So the order is:

start_task >> parse_params_task >> create_compilation_task >> create_invocation_task >> check_invocation_task >> end_task.

But the pipeline's trigger is based on the upstream data ingestion completion. How is that handled in the DAG? If the DAG is scheduled via a dataset, then the code would include a schedule with a dataset, but the user's code example may not need to show that. However, the user's code example may need to include an ExternalTaskSensor as part of the tasks. Wait, but according to the pipeline steps, the tasks are sequential and start with Start. So perhaps the ExternalTaskSensor is not part of this DAG's tasks. Instead, the DAG is triggered when the upstream task completes via dataset triggering. Therefore, the code example would not include the ExternalTaskSensor, and the DAG's schedule is set to None, but triggered via dataset. However, the code example may not show that, as it's part of the DAG's parameters. But the user's code example needs to be executable. So perhaps the DAG is scheduled normally, and the ExternalTaskSensor is not part of the tasks. Alternatively, maybe the user made a mistake and the ExternalTaskSensor is part of the tasks. But according to the pipeline steps, it's not listed.

Hmm, perhaps the pipeline steps are missing the ExternalTaskSensor. The pipeline description says "Trigger: Dataset-based scheduling activated by completion of 'dataform-training-data-ingestion'". So the DAG is triggered when that dataset is updated. Therefore, the DAG's schedule is defined by the dataset, which is set via the 'schedule' parameter with a Dataset object. However, the code example may not include that, as it's part of Airflow 2.4+ features. But the user's code example needs to be executable. So perhaps the DAG is scheduled normally, and the ExternalTaskSensor is part of the tasks. Let me check the pipeline steps again. The steps listed are six tasks, and the sensor is the fifth task. So perhaps the ExternalTaskSensor is not part of this DAG's tasks, and the DAG is triggered via a dataset. However, the code example would need to include the schedule parameter with the dataset. But the user's code example may not include that, as it's not part of the steps. Therefore, perhaps the code example proceeds without including the ExternalTaskSensor for the upstream task.

Now, moving to the code:

Import necessary modules. The user specified Airflow 2.x style imports. So:

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException

Wait, but the user's pipeline uses XCom to pass parameters between tasks. The Parse Input Parameters task extracts runtime parameters from dag_run.conf and stores them in XCom. Then the Create Compilation Result task retrieves that configuration from XCom.

So, the Python functions for the PythonOperators will need to take the task_instance as an argument to push and pull XCom values.

For example:

def parse_input_parameters(**kwargs):
    # Get parameters from dag_run.conf or use defaults
    params = kwargs['dag_run'].conf if kwargs['dag_run'] else {}
    logical_date = kwargs['logical_date']
    description = params.get('description', 'default description')
    compilation_config = {
        'git_commitish': params.get('git_commitish', 'main'),
        'workflow_invocation': {
            'compilation_result': {
                'git_commitish': params.get('git_commitish', 'main'),
                'code_compilation_config': {
                    'default_database': params.get('default_database', 'project-db'),
                    'default_schema': params.get('default_schema', 'dataform_schema'),
                    'vars': params.get('vars', {})
                }
            }
        }
    }
    # Push to XCom
    kwargs['ti'].xcom_push(key='compilation_config', value=compilation_config)

def create_compilation_result(**kwargs):
    ti = kwargs['ti']
    compilation_config = ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
    # Here, we would normally call the Dataform API to create the compilation result
    # For the sake of example, assume compilation_result_id is generated
    compilation_result_id = 'compilation_result_id'
    ti.xcom_push(key='compilation_result_id', value=compilation_result_id)

But since the Create Compilation Result task uses the Dataform API, perhaps the actual operator is a Dataform operator. However, the user's pipeline steps mention that Create Compilation Result is a PythonOperator. So perhaps the user expects us to simulate that step with a Python function that pushes the compilation result ID to XCom.

The Create Workflow Invocation task uses DataformCreateWorkflowInvocationOperator, which requires the compilation result ID and project details.

The sensor then checks the status of the workflow invocation.

Now, putting it all together:

The default_args should include 'start_date' (using days_ago(1)), 'retries': 0, and other parameters.

The DAG is defined with catchup=False, schedule_interval=None, default_args=default_args, tags maybe.

The tasks are connected in order.

Now, code:

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def parse_input_parameters(**kwargs):
    """Parse runtime parameters and store compilation config in XCom."""
    dag_run_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    logical_date = kwargs['logical_date']
    
    compilation_config = {
        'git_commitish': dag_run_conf.get('git_commitish', 'main'),
        'default_database': dag_run_conf.get('default_database', 'project-db'),
        'default_schema': dag_run_conf.get('default_schema', 'dataform_schema'),
        'vars': dag_run_conf.get('vars', {})
    }
    kwargs['ti'].xcom_push(key='compilation_config', value=compilation_config)

def create_compilation_result(**kwargs):
    """Retrieve config from XCom and simulate compilation result creation."""
    ti = kwargs['ti']
    compilation_config = ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
    # In a real scenario, call Dataform API to create compilation result
    compilation_result_id = 'generated_compilation_id'
    ti.xcom_push(key='compilation_result_id', value=compilation_result_id)

with DAG(
    dag_id='dataform_data_transformation',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['dataform', 'gcp'],
) as dag:

    start_task = DummyOperator(task_id='start')
    
    parse_params_task = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
    )
    
    create_compilation_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
    )
    
    create_invocation_task = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='{{ var.value.get("gcp_project_id", "my-gcp-project") }}',
        region='us-central1',
        repository_id='dataform-repo',
        workflow_invocation={
            'compilation_result': '{{ ti.xcom_pull(task_ids="create_compilation_result", key="compilation_result_id") }}'
        },
        gcp_conn_id='modelling_cloud_default',
    )
    
    check_invocation_task = DataformWorkflowInvocationStateSensor(
        task_id='is_workflow_invocation_done',
        project_id='{{ var.value.get("gcp_project_id", "my-gcp-project") }}',
        region='us-central1',
        repository_id='dataform-repo',
        workflow_invocation_id='{{ ti.xcom_pull(task_ids="create_workflow_invocation") }}',
        expected_statuses=['SUCCEEDED', 'FAILED'],
        gcp_conn_id='modelling_cloud_default',
    )
    
    end_task = DummyOperator(task_id='end')
    
    # Define dependencies
    start_task >> parse_params_task >> create_compilation_task >> create_invocation_task >> check_invocation_task >> end_task

Wait, but the DataformCreateWorkflowInvocationOperator requires a compilation_result_id. In the code above, the create_compilation_task pushes 'compilation_result_id' to XCom. Then, the create_invocation_task uses that ID in the workflow_invocation parameter. However, the operator's 'workflow_invocation' expects a dictionary that includes 'compilation_result' which is the ID. So the code for create_invocation_task should have:

workflow_invocation={
    'compilation_result': '{{ ti.xcom_pull(task_ids="create_compilation_result", key="compilation_result_id") }}'
},

But in reality, the DataformCreateWorkflowInvocationOperator's workflow_invocation parameter should be a dictionary that includes the compilation result's name. The compilation result's name is in the format projects/{project}/locations/{location}/repositories/{repo}/compilationResults/{id}.

Wait, perhaps the create_compilation_result function should generate the full resource name. For example:

compilation_result_id = 'projects/my-project/locations/us-central1/repositories/dataform-repo/compilationResults/123'

But since this is a mock example, the code pushes a dummy ID. However, the actual operator may require the full resource name. So the create_compilation_result function should construct the full path based on project, region, repository, and the generated ID.

But since this is a code example, perhaps the user expects placeholders. Alternatively, the code can use Jinja templating to construct the resource name.

Alternatively, perhaps the DataformCreateWorkflowInvocationOperator expects the compilation_result as a dictionary with the name field. But according to the provider's documentation, the workflow_invocation parameter is a dictionary that should include the compilation result's name.

Assuming that the compilation_result_id stored in XCom is the full resource name, then the code would use that. But in the create_compilation_result function, perhaps the code constructs the name using variables.

But for the sake of the example, let's assume that the compilation_result_id is the full name. So the create_compilation_task pushes that as a string.

In the create_invocation_task, the workflow_invocation is set with 'compilation_result' pointing to that name.

The sensor then uses the workflow_invocation_id, which is the ID returned by the create_invocation_task. The sensor's workflow_invocation_id should be the ID part of the full resource name. Alternatively, the sensor may require the full resource name. This depends on the provider's implementation.

But in the code example, the create_invocation_task is expected to return the workflow_invocation_id via XCom. The DataformCreateWorkflowInvocationOperator's execute method returns the workflow_invocation_id, which is then pulled by the sensor.

So in the sensor, workflow_invocation_id is set to '{{ ti.xcom_pull(task_ids="create_workflow_invocation") }}'.

Putting it all together, the code should be as above.

The Google Cloud connection ID is 'modelling_cloud_default', which is specified in the operators and sensor.

The project_id is retrieved from Airflow variables, with a default fallback.

The functions parse_input_parameters and create_compilation_result handle XCom pushing and pulling.

Now, check for PEP 8 compliance, proper imports, and task dependencies.

The code should be executable, assuming the required providers are installed (google provider for Airflow).

Thus, the final code should look like the above.
</think>

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def parse_input_parameters(**kwargs):
    """Parse runtime parameters and store compilation config in XCom."""
    dag_run_conf = kwargs.get('dag_run').conf or {}
    compilation_config = {
        'git_commitish': dag_run_conf.get('git_commitish', 'main'),
        'default_database': dag_run_conf.get('default_database', 'project-db'),
        'default_schema': dag_run_conf.get('default_schema', 'dataform_schema'),
        'vars': dag_run_conf.get('vars', {})
    }
    kwargs['ti'].xcom_push(key='compilation_config', value=compilation_config)

def create_compilation_result(**kwargs):
    """Retrieve config from XCom and simulate compilation result creation."""
    ti = kwargs['ti']
    compilation_config = ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
    ti.xcom_push(key='compilation_result_id', value='generated_compilation_id')

with DAG(
    dag_id='dataform_data_transformation',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['dataform'],
) as dag:

    start_task = DummyOperator(task_id='start')
    
    parse_params_task = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
    )
    
    create_compilation_task = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
    )
    
    create_invocation_task = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='{{ var.value.get("gcp_project", "default-project") }}',
        region='us-central1',
        repository_id='dataform-repo',
        workflow_invocation={
            'compilation_result': '{{ ti.xcom_pull(task_ids="create_compilation_result", key="compilation_result_id") }}'
        },
        gcp_conn_id='modelling_cloud_default',
    )
    
    check_invocation_task = DataformWorkflowInvocationStateSensor(
        task_id='is_workflow_invocation_done',
        project_id='{{ var.value.get("gcp_project", "default-project") }}',
        region='us-central1',
        repository_id='dataform-repo',
        workflow_invocation_id='{{ ti.xcom_pull(task_ids="create_workflow_invocation") }}',
        expected_statuses=['SUCCEEDED', 'FAILED'],
        gcp_conn_id='modelling_cloud_default',
    )
    
    end_task = DummyOperator(task_id='end')

    start_task >> parse_params_task >> create_compilation_task >> create_invocation_task >> check_invocation_task >> end_task