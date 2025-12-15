<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has six sequential tasks. The trigger is dataset-based, so I should use an ExternalTaskSensor to wait for the "dataform-training-data-ingestion" to complete. Wait, the description says the pipeline triggers when upstream data ingestion completes. So the first task should be a sensor that checks for the completion of that external task. But in the steps listed, the first task is a DummyOperator named Start. Hmm, maybe the ExternalTaskSensor is part of the DAG's setup. Let me check the Pipeline Steps again.

Looking back, the steps are Start (Dummy), Parse Input Parameters, Create Compilation Result, Create Workflow Invocation, Is Workflow Invocation Done, End. But the Orchestration & Scheduling mentions the trigger is activated by the completion of the ingestion task. So perhaps the ExternalTaskSensor is the first task before the Start dummy. Wait, but the steps list Start as the first task. Maybe the user made a mistake, or perhaps the ExternalTaskSensor is part of the same DAG? Or maybe the DAG is scheduled to run after the ingestion, but the user wants to include the sensor in the DAG to wait for the upstream task. Let me re-read the description.

Under Integrations & Dependencies, it says "Triggered by upstream data ingestion dataset completion." So the DAG should have a sensor that waits for the upstream task. But in the Pipeline Steps, the first task is Start. So perhaps the user forgot to mention the sensor in the steps. But according to the constraints, if sensors appear in the description, demonstrate proper use. The pipeline description mentions that the trigger is based on dataset completion, so I need to include an ExternalTaskSensor as the first task.

Wait, the Pipeline Steps list six tasks, but the description says six sequential tasks. Let me count: Start, Parse, Create Compilation, Create Workflow Invocation, Is Done, End. That's six tasks. But the trigger is handled by an ExternalTaskSensor, which would be an additional task. So maybe the user intended the ExternalTaskSensor to be part of the DAG. Therefore, the DAG should start with the sensor, then the Start dummy task. But according to the Pipeline Steps, the Start is the first task. This is conflicting.

Wait, the Pipeline Summary says it's a linear pipeline with six tasks. The steps listed are six tasks. But the Orchestration & Scheduling mentions the trigger is activated by the completion of "dataform-training-data-ingestion". So perhaps the ExternalTaskSensor is part of the DAG's tasks. Therefore, the DAG should have the sensor as the first task, followed by the Start dummy. But according to the Pipeline Steps, the Start is the first task. So maybe the user made an error in the description. Alternatively, maybe the DAG is scheduled after the ingestion completes, and the sensor isn't needed. But the constraints say if sensors appear in the description, demonstrate proper use. Since the pipeline mentions the trigger is based on dataset completion, I should include an ExternalTaskSensor as the first task.

Wait, the Pipeline Steps list six tasks, but if I add the sensor, that would make seven tasks. But the user says six sequential tasks. So perhaps the sensor is not part of the steps, and the DAG is triggered by the completion of the ingestion task via schedule_interval or some other mechanism. Alternatively, maybe the ExternalTaskSensor is part of the DAG's tasks. But according to the Pipeline Steps, the first task is Start. Hmm. This is confusing.

Wait, the Pipeline Steps mention "Start: DummyOperator initiates the pipeline execution". So perhaps the ExternalTaskSensor is not part of the DAG's tasks, but the DAG is scheduled to run after the ingestion task. However, the user might have intended to include the sensor as part of the DAG. Let me check the constraints again. The constraints say: "If sensors appear in the description, demonstrate proper use". The pipeline description mentions that the trigger is dataset-based scheduling activated by completion of "dataform-training-data-ingestion". So I need to include an ExternalTaskSensor as the first task in the DAG. Therefore, the DAG's tasks would be:

1. ExternalTaskSensor (wait for ingestion)
2. Start (Dummy)
3. Parse Input Parameters
4. Create Compilation Result
5. Create Workflow Invocation
6. Is Workflow Invocation Done (sensor)
7. End (Dummy)

But the Pipeline Steps list six tasks. So perhaps the user didn't mention the sensor in the steps, but according to the constraints, I need to include it. Alternatively, maybe the sensor is part of the DAG's trigger, not a task. But Airflow doesn't work that way. The DAG runs when its schedule_interval is met, but if it needs to wait for another DAG's task, that's done via a sensor task.

Therefore, I should include the ExternalTaskSensor as the first task. But according to the Pipeline Steps, the first task is Start. So perhaps the user made a mistake, but I need to follow the constraints. The constraints say that if sensors appear in the description, demonstrate proper use. The description mentions the trigger is based on dataset completion, so I need to include the sensor. Therefore, the DAG will have seven tasks. But the user's Pipeline Steps list six. Hmm. Maybe the Start and End are DummyOperators, and the sensor is part of the steps. Let me check the Pipeline Steps again:

The steps are:

- Start: DummyOperator
- Parse Input Parameters: PythonOperator
- Create Compilation Result: PythonOperator
- Create Workflow Invocation: DataformCreateWorkflowInvocationOperator
- Is Workflow Invocation Done: DataformWorkflowInvocationStateSensor
- End: DummyOperator

That's six tasks. But according to the Orchestration & Scheduling, the trigger is activated by the completion of the ingestion task. So the DAG should start with a sensor that waits for that task. Therefore, the actual tasks would be:

1. wait_for_ingestion (ExternalTaskSensor)
2. start (Dummy)
3. parse_input_parameters
4. create_compilation_result
5. create_workflow_invocation
6. is_workflow_invocation_done
7. end

But the Pipeline Steps list six tasks. So perhaps the user intended the ExternalTaskSensor to be part of the DAG, but it's not listed in the steps. That's conflicting. Alternatively, maybe the DAG is scheduled after the ingestion task completes via Airflow's dataset mechanism, and thus the sensor isn't needed. But Airflow 2.4+ supports dataset-driven scheduling, but the user might not be using that. Alternatively, perhaps the sensor is part of the DAG's tasks. Given the constraints, I should include the ExternalTaskSensor as the first task.

But the Pipeline Steps mention six tasks. The user's Pipeline Steps list Start as the first task. So perhaps the sensor is not part of the DAG's tasks, but the DAG is triggered via dataset. However, the user's description says "Trigger: Dataset-based scheduling activated by completion of..." which in Airflow 2.4+ can be done using datasets. But if the user is using an older version, they might use a sensor. Since the problem statement doesn't specify Airflow version beyond 2.x, perhaps I should include the ExternalTaskSensor as the first task, even if it adds an extra task beyond the six mentioned. But the user's Pipeline Steps list six tasks. This is a conflict.

Alternatively, maybe the Start task is the ExternalTaskSensor. But the user says Start is a DummyOperator. So perhaps the user made a mistake in the description, and the first task should be the sensor. But according to the problem statement, I need to follow the Pipeline Steps as described. Let me re-examine the Pipeline Steps:

The Pipeline Steps are:

- Start: DummyOperator initiates the pipeline execution

So the first task is Start, a DummyOperator. Then the next steps follow. But according to the Orchestration & Scheduling, the trigger is based on the completion of an upstream task. Therefore, the DAG should have an ExternalTaskSensor as the first task, which then triggers the Start task. But according to the Pipeline Steps, Start is the first task. This is conflicting.

Wait, perhaps the DAG is scheduled to run after the upstream task completes via Airflow's Dataset mechanism. In that case, the DAG would be triggered when the upstream task produces a dataset, and the sensor isn't needed. But the problem statement says to include sensors if they appear in the description. The description mentions that the trigger is dataset-based, but doesn't explicitly mention a sensor. However, the user's pipeline steps don't include a sensor. So perhaps the ExternalTaskSensor is not part of this DAG's tasks. Instead, the DAG is scheduled via a schedule_interval that depends on the upstream task's completion. But Airflow's schedule_interval can't directly depend on another DAG's task. Therefore, the correct approach is to include an ExternalTaskSensor as the first task in this DAG to wait for the upstream task.

But according to the Pipeline Steps, the first task is Start. So this is a contradiction. The user may have made an error, but I need to reconcile this.

Alternatively, perhaps the DAG is triggered by the upstream task via a dataset, and the Start task is the first task. In that case, no sensor is needed. But the problem constraints say that if sensors appear in the description, demonstrate proper use. Since the description mentions the trigger is based on an upstream task, perhaps the sensor is required. Therefore, I'll include the ExternalTaskSensor as the first task, followed by the Start task. But this adds an extra task beyond the six mentioned. However, the problem statement's Pipeline Steps may have omitted the sensor. Given the constraints, I should include the sensor.

Wait, the Pipeline Steps list six tasks, but the user's description says the pipeline has six sequential tasks. If I include the sensor, that's seven tasks. But perhaps the sensor is part of the steps. Let me check the Pipeline Steps again:

The steps are:

1. Start
2. Parse Input Parameters
3. Create Compilation Result
4. Create Workflow Invocation
5. Is Workflow Invocation Done (which is a sensor)
6. End

So the fifth task is a sensor. The ExternalTaskSensor for the upstream ingestion is not listed. Therefore, perhaps the user intended that the DAG is scheduled after the upstream task completes, but the DAG itself doesn't have a sensor for that. However, the problem constraints mention that if sensors appear in the description, demonstrate proper use. The fifth task is a sensor (DataformWorkflowInvocationStateSensor), which is included. The ExternalTaskSensor for the upstream task is not mentioned in the steps, but the trigger is based on its completion. So perhaps the DAG is scheduled to run after the upstream task via Airflow's dataset triggering, and the sensor is not needed. But the problem statement says to use sensors if they appear in the description. Since the description mentions the trigger is based on an upstream task, perhaps I should include an ExternalTaskSensor as the first task.

But according to the Pipeline Steps, the first task is Start. So this is conflicting. The user may have intended that the DAG is triggered by the upstream task's completion, but the DAG itself doesn't need a sensor because it's scheduled via a dataset. However, the problem statement says to convert the pipeline description into a DAG. The description mentions that the trigger is dataset-based scheduling activated by the completion of "dataform-training-data-ingestion". Therefore, perhaps the DAG is scheduled using a Dataset, and the sensor is not part of the tasks. But Airflow's Dataset-based scheduling is a feature where a task in another DAG can produce a dataset, and this DAG is scheduled when that dataset is updated. In that case, the DAG doesn't need a sensor task because it's triggered automatically. However, the problem statement may not be considering that and expects an ExternalTaskSensor to be used.

This is a bit ambiguous. Given the constraints, I need to decide whether to include the ExternalTaskSensor or not. Since the Pipeline Steps list six tasks and the user didn't mention a sensor for the upstream task, perhaps the sensor is not part of this DAG's tasks. Instead, the DAG is triggered via some other mechanism (like schedule_interval or a dataset), and the tasks listed are the six steps. Therefore, I'll proceed with the six tasks as described, without adding an ExternalTaskSensor for the upstream task. But then, how does the DAG ensure it runs after the upstream task? Maybe the DAG's schedule_interval is None, and it's triggered manually or via a dataset. But the problem statement says the trigger is dataset-based, so perhaps the DAG is scheduled via a dataset produced by the upstream task. In that case, the DAG doesn't need a sensor task. However, the problem constraints mention that if sensors appear in the description, demonstrate proper use. Since the description mentions the trigger is based on an upstream task, but no sensor is listed in the steps, perhaps the sensor is not required here. Therefore, I'll proceed with the six tasks as listed.

Now, moving on to the code structure.

The DAG should have default_args with catchup=False, schedule_interval=None (or perhaps a schedule), but according to the Orchestration & Scheduling, catchup is disabled. The max parallel width is 1, so concurrency is 1. Retries are zero.

The tasks are sequential, no branching or parallelism.

The operators needed:

- DummyOperator for Start and End.
- PythonOperator for Parse Input Parameters and Create Compilation Result.
- DataformCreateWorkflowInvocationOperator (assuming this is a custom operator or part of a provider; perhaps from airflow.providers.google.cloud.operators.dataform).
- DataformWorkflowInvocationStateSensor (from airflow.providers.google.cloud.sensors.dataform).

The Dataform operators and sensors require a Google Cloud connection, which is given as "modelling_cloud_default".

The Parse Input Parameters task extracts runtime parameters from dag_run.conf, processes them, and stores the compilation config in XCom. The Create Compilation Result task retrieves that config and creates the compilation result in Dataform.

For the Python functions:

The parse_input_parameters function could take the logical date (execution_date) and a description from dag_run.conf, with defaults. It processes these and stores the compilation config in XCom.

The create_compilation_result function retrieves the config from XCom, then calls the Dataform API to create the compilation result. The DataformCreateWorkflowInvocationOperator then uses that compilation result's ID to trigger the workflow.

The DataformWorkflowInvocationStateSensor waits for the invocation to reach SUCCEEDED or FAILED state.

Now, putting this into code.

Imports:

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator, DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json

Wait, but the user's description mentions DataformCreateWorkflowInvocationOperator and DataformWorkflowInvocationStateSensor. Are these the correct imports? Let me check. In Airflow Google providers, the operators and sensors for Dataform are available. For example:

DataformCreateCompilationResultOperator: creates a compilation result.

DataformCreateWorkflowInvocationOperator: creates a workflow invocation.

DataformWorkflowInvocationStateSensor: waits for the invocation to reach a terminal state.

So the operators and sensors are correctly imported.

The Parse Input Parameters task is a PythonOperator that processes parameters and stores in XCom. The Create Compilation Result task uses DataformCreateCompilationResultOperator, which would take the config from XCom. Wait, but the user's steps say Create Compilation Result is a PythonOperator. Wait, no: the second step is "Create Compilation Result: PythonOperator retrieves the configuration from XCom and executes Dataform compilation using the parsed parameters". So perhaps the user expects a PythonOperator that uses the Dataform SDK to create the compilation result. Alternatively, maybe using the existing DataformCreateCompilationResultOperator.

But the problem statement says to use Airflow operators where possible. So perhaps the Create Compilation Result should use DataformCreateCompilationResultOperator, which requires parameters like project_id, region, repository_id, and compilation_result. But the compilation_result is built from the parameters parsed in the previous task.

Alternatively, if the DataformCreateCompilationResultOperator is not available, perhaps the user expects a PythonOperator that uses the Dataform client library. However, given the problem statement mentions "Google Cloud Dataform via dedicated operators and sensors", it's safe to assume that the correct operators are available.

Therefore, the Create Compilation Result task can be a DataformCreateCompilationResultOperator, which takes the config from XCom. But how to pass the parameters. The Parse Input Parameters task would need to generate the compilation_result object and push it to XCom. Then the DataformCreateCompilationResultOperator can use that.

Alternatively, the PythonOperator for Parse Input Parameters prepares the compilation config, pushes it to XCom, and the DataformCreateCompilationResultOperator uses that config to create the compilation result.

But DataformCreateCompilationResultOperator requires parameters like project_id, region, repository_id, and compilation_result. The compilation_result is a dictionary that includes git_commitish and other parameters.

So the Parse Input Parameters task would construct this compilation_result dictionary and push it to XCom. Then the Create Compilation Result task (using DataformCreateCompilationResultOperator) would take that compilation_result from XCom.

But how to pass the parameters between tasks. The DataformCreateCompilationResultOperator would need to pull the compilation_result from XCom. This can be done using Jinja templating, like task_instance.xcom_pull(...).

Alternatively, the PythonOperator can return the compilation_result, which is then pushed to XCom, and the next operator can pull it.

But operators like DataformCreateCompilationResultOperator may not support pulling from XCom directly in their parameters. Therefore, perhaps the PythonOperator for Create Compilation Result is needed to retrieve the config from XCom and call the Dataform API.

Wait, the user's step says: "Create Compilation Result: PythonOperator retrieves the configuration from XCom and executes Dataform compilation using the parsed parameters". So this task is a PythonOperator, not a pre-existing operator. Therefore, I need to write a Python function that uses the Dataform client to create the compilation result.

But how to handle that. Let's proceed.

So the code would have:

- parse_input_parameters: PythonOperator that processes dag_run.conf, creates a compilation config, pushes to XCom.

- create_compilation_result: PythonOperator that pulls the config from XCom, uses Dataform client to create compilation result, and pushes the compilation result ID to XCom.

Then, the DataformCreateWorkflowInvocationOperator uses the compilation result ID to create the workflow invocation.

Wait, but DataformCreateWorkflowInvocationOperator requires a compilation_result_id parameter. So the create_compilation_result task must generate that ID and push it to XCom.

So the flow is:

parse_input_parameters -> create_compilation_result (PythonOperator) -> create_workflow_invocation (DataformCreateWorkflowInvocationOperator) -> is_workflow_invocation_done (sensor) -> end.

But according to the user's steps, the Create Compilation Result is a PythonOperator, which suggests that it's using some custom code, perhaps because there's no existing operator for creating the compilation result. Alternatively, maybe the existing operator is used.

This is a bit ambiguous. Let me check the Airflow documentation for Dataform operators.

Upon checking, the DataformCreateCompilationResultOperator is available in Airflow's Google provider. It takes parameters like project_id, region, repository_id, and compilation_result. The compilation_result is a dictionary that includes git_commitish, code_compilation_config, etc.

Therefore, the Parse Input Parameters task can prepare the compilation_result dictionary and push it to XCom. Then the Create Compilation Result task uses DataformCreateCompilationResultOperator, pulling the compilation_result from XCom.

But how to pass the parameters. For example:

In the Parse task:

def parse_input_parameters(**context):
    # logic to get parameters from dag_run.conf
    config = {
        'git_commitish': 'main',
        # other params
    }
    context['ti'].xcom_push(key='compilation_config', value=config)

Then, the create_compilation_result_task uses DataformCreateCompilationResultOperator, with compilation_result=config from XCom.

But the operator's parameters may need to be templated. For example:

create_compilation_result = DataformCreateCompilationResultOperator(
    task_id='create_compilation_result',
    project_id='my-project',  # or from connection?
    region='us-central1',
    repository_id='my-repo',
    compilation_result="{{ ti.xcom_pull(task_ids='parse_input_parameters', key='compilation_config') }}",
    # other parameters
)

But the compilation_result parameter may need to be a dictionary, and using Jinja templating can pass it as a JSON string, which the operator can parse.

Alternatively, the PythonOperator for create_compilation_result would be better, as it can retrieve the XCom value and call the Dataform API directly.

But according to the user's steps, the Create Compilation Result is a PythonOperator. So perhaps the user expects that task to use a Python function to create the compilation result using the Dataform client, rather than using a pre-existing operator.

This is a bit unclear. Given the problem statement mentions "dedicated operators and sensors", perhaps the DataformCreateCompilationResultOperator is intended to be used. However, the user's step says it's a PythonOperator. This is conflicting.

Alternatively, perhaps the user made a mistake, and the Create Compilation Result should be a Dataform operator. But given the steps explicitly mention PythonOperator, I'll proceed with that.

Therefore, the create_compilation_result task is a PythonOperator that uses the Dataform client to create the compilation result. This requires setting up the client with the Google Cloud connection.

The code for the PythonOperator would be:

def create_compilation_result(**context):
    from google.cloud import dataform_v1beta1

    config = context['ti'].xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
    client = dataform_v1beta1.DataformClient()
    parent = f"projects/{config['project_id']}/locations/{config['region']}/repositories/{config['repository_id']}"
    compilation_result = dataform_v1beta1.CompilationResult(
        git_commitish=config['git_commitish'],
        code_compilation_config=config.get('code_compilation_config', {})
    )
    response = client.create_compilation_result(
        parent=parent,
        compilation_result=compilation_result
    )
    context['ti'].xcom_push(key='compilation_result_name', value=response.name)

But this requires the project_id, region, repository_id to be part of the config. The parse_input_parameters function should extract these from the dag_run.conf or provide defaults.

Alternatively, the connection may provide the project_id and other details. But the problem statement mentions the connection ID is "modelling_cloud_default", which should include the project ID and other details.

This is getting complicated. Perhaps the DataformCreateCompilationResultOperator is better suited here, but given the user's step says PythonOperator, I'll proceed with the Python function.

Putting it all together:

The DAG code would look like:

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def parse_input_parameters(**context):
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    logical_date = context['execution_date'].strftime('%Y-%m-%d')
    description = conf.get('description', 'default description')
    git_commitish = conf.get('git_commitish', 'main')
    
    compilation_config = {
        'git_commitish': git_commitish,
        'logical_date': logical_date,
        'description': description,
        # Add other necessary parameters here
    }
    context['ti'].xcom_push(key='compilation_config', value=compilation_config)

def create_compilation_result(**context):
    from google.cloud import dataform_v1beta1
    from google.protobuf.json_format import ParseDict

    config = context['ti'].xcom_pull(task_ids='parse_input_parameters', key='compilation_config')
    client = dataform_v1beta1.DataformClient()
    
    project_id = 'your-project-id'  # Should be retrieved from connection
    region = 'us-central1'
    repository_id = 'your-repo-id'
    
    parent = f"projects/{project_id}/locations/{region}/repositories/{repository_id}"
    
    compilation_result = dataform_v1beta1.CompilationResult(
        git_commitish=config['git_commitish'],
        code_compilation_config=dataform_v1beta1.CodeCompilationConfig(
            default_database='your-database',
            default_schema='your-schema',
            assertion_schema='your-assertion-schema',
            vars=config.get('vars', {})
        )
    )
    
    response = client.create_compilation_result(
        parent=parent,
        compilation_result=compilation_result
    )
    context['ti'].xcom_push(key='compilation_result_name', value=response.name)

with DAG(
    dag_id='dataform_data_transformation_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['dataform'],
) as dag:

    start = DummyOperator(task_id='start')
    
    parse_input_params = PythonOperator(
        task_id='parse_input_parameters',
        python_callable=parse_input_parameters,
        provide_context=True,
    )
    
    create_compilation = PythonOperator(
        task_id='create_compilation_result',
        python_callable=create_compilation_result,
        provide_context=True,
    )
    
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id='your-project-id',  # From connection
        region='us-central1',
        repository_id='your-repo-id',
        workflow_invocation={
            'compilation_result': '{{ ti.xcom_pull(task_ids="create_compilation_result", key="compilation_result_name") }}',
        },
        gcp_conn_id='modelling_cloud_default',
    )
    
    is_workflow_done = DataformWorkflowInvocationStateSensor(
        task_id='is_workflow_invocation_done',
        project_id='your-project-id',
        region='us-central1',
        repository_id='your-repo-id',
        workflow_invocation_id='{{ ti.xcom_pull(task_ids="create_workflow_invocation", key="workflow_invocation_id") }}',
        expected_statuses=['SUCCEEDED'],
        gcp_conn_id='modelling_cloud_default',
    )
    
    end = DummyOperator(task_id='end')
    
    start >> parse_input_params >> create_compilation >> create_workflow_invocation >> is_workflow_done >> end

Wait, but the DataformCreateWorkflowInvocationOperator requires a workflow_invocation dictionary, which includes the compilation_result (the name of the compilation result). The create_compilation_result task pushes the compilation_result_name to XCom, which is then pulled here.

However, the DataformCreateWorkflowInvocationOperator may require the compilation_result name as part of the workflow_invocation dict. The operator's documentation shows that the workflow_invocation should have a 'compilation_result' field pointing to the compilation result's name.

In the create_workflow_invocation task, the compilation_result is set to the XCom value from create_compilation_result.

The sensor then monitors the workflow invocation. The workflow_invocation_id is obtained from the create_workflow_invocation task's output. However, the DataformCreateWorkflowInvocationOperator's execute method returns the workflow_invocation_id, which is automatically pushed to XCom. So the sensor can reference it using task_ids='create_workflow_invocation'.

But in the sensor, workflow_invocation_id is set to '{{ ti.xcom_pull(task_ids="create_workflow_invocation") }}'.

Alternatively, the operator may return the name of the workflow invocation, which includes the ID.

Assuming that the DataformCreateWorkflowInvocationOperator's result is the name of the workflow invocation, which includes the ID, the sensor can extract the ID from the name.

For example, if the name is 'projects/my-project/locations/us-central1/repositories/my-repo/workflowInvocations/123', then the ID is '123'.

But the sensor may require the full name or just the ID. The DataformWorkflowInvocationStateSensor's parameters include workflow_invocation_id, which is the ID part. So perhaps the operator returns the full name, and we need to extract the ID.

Alternatively, the operator may return the workflow_invocation_id directly.

This depends on the implementation of the operator. For the sake of this code, let's assume that the create_workflow_invocation task pushes the workflow_invocation_id to XCom, and the sensor uses that.

But in the current code, the create_workflow_invocation is using DataformCreateWorkflowInvocationOperator, which, when executed, returns the workflow_invocation_id. This is stored in XCom under the task's return value. So the sensor can pull it using task_ids='create_workflow_invocation'.

Therefore, the sensor's workflow_invocation_id parameter would be '{{ ti.xcom_pull(task_ids="create_workflow_invocation") }}'.

But in the code I wrote earlier, the sensor's workflow_invocation_id is set to '{{ ti.xcom_pull(...) }}' from create_workflow_invocation. However, the DataformWorkflowInvocationStateSensor may require the full resource name, not just the ID. Let me check the sensor's parameters.

According to the Airflow documentation, the DataformWorkflowInvocationStateSensor requires project_id, region, repository_id, and workflow_invocation_id. So the workflow_invocation_id is the ID part, not the full resource name. Therefore, the create_workflow_invocation task must push the ID part to XCom.

But when you create a workflow invocation, the response's name is the full resource name, which includes the ID. For example, the name is 'projects/{project}/locations/{region}/repositories/{repo}/workflowInvocations/{id}'. The ID is the last part. So the create_workflow_invocation task's Python function should extract the ID from the name and push it to XCom.

But in the current code, the create_workflow_invocation is using DataformCreateWorkflowInvocationOperator, which may return the full name. Therefore, the sensor's workflow_invocation_id should be parsed from that name.

Alternatively, perhaps the operator allows specifying the workflow_invocation_id directly. This is getting too detailed, but for the code to work, perhaps the create_workflow_invocation task's operator returns the workflow_invocation_id, which is then used by the sensor.

Alternatively, the DataformCreateWorkflowInvocationOperator's result is the workflow_invocation_id, which is pushed to XCom, and the sensor uses that.

Assuming that, the sensor's workflow_invocation_id parameter would be '{{ ti.xcom_pull(task_ids="create_workflow_invocation") }}'.

But to confirm, when you call the DataformCreateWorkflowInvocationOperator, it returns the workflow_invocation_id, which is part of the created resource's name. So the code would need to extract it.

For example:

name = response.name
workflow_invocation_id = name.split('/')[-1]

But if the operator does this automatically and returns the ID, then the XCom value is the ID. Otherwise, the code would need to process it.

Given the time constraints, I'll proceed with the assumption that the operator returns the full name, and the sensor requires the ID. Therefore, the create_workflow_invocation task's function should extract the ID from the name and push it to XCom.

But since we're using the DataformCreateWorkflowInvocationOperator, which is a pre-existing operator, perhaps it's designed to return the ID. Alternatively, the sensor may require the full name. This is unclear, but for the code to work, perhaps the sensor uses the full name.

Alternatively, the sensor's parameters may require the full name. Let me check the Airflow documentation for DataformWorkflowInvocationStateSensor.

According to the Airflow documentation, the sensor has parameters:

project_id (str): The ID of the Google Cloud project where the Dataform workflow invocation is located.

region (str): The ID of the Google Cloud region where the Dataform workflow invocation is located.

repository_id (str): The ID of the Dataform repository that contains the workflow invocation.

workflow_invocation_id (str): The ID of the workflow invocation.

So the sensor requires the workflow_invocation_id, which is the ID part, not the full resource name.

Therefore, the create_workflow_invocation task must provide this ID. The DataformCreateWorkflowInvocationOperator's execute method returns the created workflow invocation's name, which is the full resource name. Therefore, the task must extract the ID from the name and push it to XCom.

But since we're using the DataformCreateWorkflowInvocationOperator, which is a pre-existing operator, perhaps it's designed to return the ID. Alternatively, the operator returns the full name, and we need to process it.

In any case, the code for the create_workflow_invocation task would need to push the workflow_invocation_id to XCom. However, since we're using the operator, perhaps the operator's do_xcom_push is True, and it returns the workflow_invocation_id. Alternatively, the operator returns the full name, and we need to extract the ID.

This is getting too detailed, but for the code to work, perhaps the following approach:

When creating the workflow invocation, the operator returns the name, and we can process it in a Python callable. But since we're using the operator directly, perhaps we need to use a PythonOperator after it to extract the ID.

Alternatively, perhaps the sensor can take the full name and extract the ID internally. But according to the sensor's parameters, it requires the ID, not the full name.

This suggests that the DataformCreateWorkflowInvocationOperator should return the ID, but I'm not sure. To resolve this, perhaps the create_workflow_invocation task is a PythonOperator that uses the client to create the workflow invocation and pushes the ID to XCom. But the user's step says that Create Workflow Invocation uses DataformCreateWorkflowInvocationOperator.

Given the ambiguity, I'll proceed with the code as follows:

The create_workflow_invocation task uses DataformCreateWorkflowInvocationOperator, which requires the compilation_result name (from XCom). The operator's execute method returns the workflow_invocation name, which is stored in XCom. Then, the sensor uses this name to extract the ID.

Wait, but the sensor requires the ID, not the name. So perhaps the code should extract the ID from the name. For example:

In the sensor's parameters:

workflow_invocation_id='{{ ti.xcom_pull(task_ids="create_workflow_invocation").split("/")[-1] }}'

But this requires the XCom value from create_workflow_invocation to be the full name.

Alternatively, the create_workflow_invocation task could push the ID to XCom. But since we're using the operator, perhaps it's better to handle this in the sensor.

But this may not be possible with templated fields. Alternatively, the sensor can take the full name and