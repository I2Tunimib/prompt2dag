<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has several steps that must be executed linearly. The tasks are params_validate, download_mondo_terms, normalized_mondo_terms, index_mondo_terms, publish_mondo, and slack. Each task depends on the previous one, so dependencies are straightforward with >> operators.

The DAG should have manual triggers, so schedule_interval is None. Concurrency settings: max_active_runs=1 and maybe concurrency=1. Default_args should include things like owner, start_date, maybe retries, and other common parameters. Since failure handling uses NONE_FAILED trigger rule, I need to check where that applies. Wait, the Slack notification is for completion, but also for task failures. So maybe the slack task should trigger on both success and failure? But the description says "sends a Slack notification upon completion", which might imply only on success. Hmm, maybe the slack task is set to trigger on all states, but the example uses a PythonOperator with a function that sends completion. Alternatively, maybe use a callback or a separate task for failure. But the user's example mentions using NONE_FAILED trigger rule. Wait, the failure handling section says "Uses NONE_FAILED trigger rule and includes Slack notifications for task failures". So perhaps the publish_mondo task needs to have trigger_rule set to NONE_FAILED? Or maybe the slack task? Let me think.

Wait, the pipeline steps mention that the 'slack' task sends a notification upon completion. But the failure handling says Slack notifications for task failures. So maybe there's a separate error notification, but the user's description only lists 'slack' as a step in the pipeline. The user's example might not include error handling beyond the trigger rule. The NONE_FAILED trigger rule is used when a task should run even if some upstream tasks failed, but according to the description, the trigger rule is part of the failure handling. Maybe the publish_mondo task requires that all previous tasks succeeded (since it's linear), but the slack notification should be sent regardless? Or perhaps the trigger_rule is applied to the slack task. Wait, the description says "Failure Handling: Uses NONE_FAILED trigger rule and includes Slack notifications for task failures". So maybe the Slack notification is sent even if some tasks failed. But the pipeline steps list 'slack' as the last step. So perhaps the slack task is set to trigger regardless of upstream failures. To do that, the trigger_rule for the slack task should be 'all_done' or 'none_failed'. Wait, the trigger rule 'none_failed' allows the task to run if all upstream tasks are either successful or skipped. But if any upstream task failed, then 'none_failed' would not trigger. Alternatively, 'all_done' would run regardless of upstream success or failure. So maybe the slack task uses trigger_rule='all_done' so that it sends a notification whether the pipeline succeeded or failed. But according to the problem statement, the Slack notification is upon completion, which includes both success and failure. So the slack task should run regardless. So in the dependencies, the publish_mondo task is followed by the slack task, but the slack task's trigger_rule is set to 'all_done' so that even if publish_mondo fails, the slack task runs. Wait, but the dependencies are linear. So params_validate >> download >> normalize >> index >> publish >> slack. If any task fails, the subsequent tasks are skipped. So the slack task would only run if all previous tasks succeeded. But the user wants Slack notifications upon completion, which includes failures. So perhaps the slack task should be downstream of all tasks and set to trigger regardless. Alternatively, maybe the slack task is set as a downstream of all previous tasks, but with trigger_rule='all_done'. Wait, but in a linear chain, if any task fails, the subsequent tasks are not executed. So the slack task would only be triggered if all previous tasks succeeded. To have the slack task run regardless of the success or failure of the pipeline, we need to set it as a downstream of all possible tasks, but in a linear setup, that's not possible. Alternatively, perhaps the slack task is set to be triggered by all previous tasks, but with trigger_rule set to 'all_done'. Wait, maybe the correct approach is to have the slack task be triggered after publish_mondo, but with trigger_rule='all_done', so that even if publish_mondo fails, the slack task runs. Let me check: if publish_mondo is the last task before slack, then if publish_mondo fails, the slack task would still run if its trigger_rule is 'all_done'. Because the default trigger rule is 'all_success', so if the upstream task (publish_mondo) is failed, the slack task would not run unless the trigger_rule is changed. So to have the slack task run regardless of the success of publish_mondo, set its trigger_rule to 'all_done'. But according to the problem statement, the Slack notification is sent upon completion. So regardless of success or failure, the notification is sent. Therefore, the slack task should have trigger_rule='all_done'. So I need to set that parameter in the Slack task.

Next, the params_validate task needs to validate the 'color' parameter. So the DAG should have parameters passed in, perhaps via DAG params. The user mentions that parameters include color, spark_jar, and obo_parser_spark_jar. So these can be set as DAG parameters, accessed via the task's parameters.

For the tasks:

- params_validate: PythonOperator that checks if 'color' is provided. Maybe raises an error if not, but according to the skip logic, tasks can be skipped if data is up-to-date. Wait, the params_validate step is to validate the color parameter. So perhaps if the color is invalid, the task fails, causing the pipeline to stop. Or maybe it's a check that the parameter exists. Let's assume that the function checks the 'color' parameter and raises a ValueError if it's invalid. The task would then fail, stopping the pipeline.

The download_mondo_terms task downloads the latest OBO file from GitHub, checks if a newer version exists compared to what's already imported, and uploads to S3 if available. If no new version, this task can be skipped. So the function for download_mondo_terms would determine if a new version exists and return a value that Airflow can use to decide to skip downstream tasks. Wait, but Airflow tasks can return a value that triggers a branch. However, since the pipeline is linear, perhaps the download task will skip itself if there's no new version, causing downstream tasks to be skipped as well. So the download task's function would check for a new version. If no new version, it raises AirflowSkipException, which skips the task. Then, since all downstream tasks depend on this task's success, they would be skipped as well. Alternatively, the task could return a value that leads to skipping, but in a linear setup, perhaps raising AirflowSkipException is the way to go.

The normalized_mondo_terms, index_mondo_terms, and publish_mondo tasks are Spark jobs. These can be executed via SparkSubmitOperator, but the user's example uses PythonOperator. Wait, the problem says "processing: Apache Spark for data normalization and Elasticsearch indexing". So perhaps these tasks are Spark jobs submitted via SparkSubmitOperator. However, the user's constraints mention that if sensors are used, demonstrate proper use. But in this case, there are no sensors mentioned. So perhaps the tasks that run Spark jobs are SparkSubmitOperators. But the user's initial constraints specify to use PythonOperator if possible. Wait, the user's example uses PythonOperator for all tasks. But given that Spark jobs are involved, perhaps using SparkSubmitOperator is more appropriate. However, the problem says "generate complete, executable code", assuming required packages are installed. So if SparkSubmitOperator is used, the code would need to have the proper imports. Let me check the constraints again: the user says "Use Airflow 2.x style imports (e.g., from airflow import DAG; from airflow.operators.python import PythonOperator)". So perhaps the Spark tasks are implemented via PythonOperator that calls a function which in turn runs the Spark jobs. Alternatively, use SparkSubmitOperator. But the user's example uses PythonOperator for all steps. Let me check the pipeline steps again. The normalized_mondo_terms step processes the OBO file using Spark. So perhaps the code uses SparkSubmitOperator from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator. But the user's constraints require imports like from airflow.operators.python import PythonOperator. So maybe the Spark jobs are wrapped in PythonOperators that execute the spark-submit command. Alternatively, perhaps the Spark processing is done via a Python function that uses PySpark. But the problem says "processing and normalizes the ontology terms using Spark", which implies Spark jobs. So perhaps the tasks are SparkSubmitOperators. However, the user's code example may not include those operators unless specified. Wait, the user's constraints mention that if sensors are used, demonstrate proper use, but there are no sensors here. So perhaps the code uses PythonOperators for all tasks, with placeholders for Spark processing. But the code needs to be executable. So perhaps the functions called by PythonOperator have pass statements or minimal code for demonstration. For example, the download_mondo_terms function could have a placeholder for downloading and checking the file. The Spark steps could have stubs that simulate processing. The key is to structure the DAG correctly with proper task dependencies and parameters.

The DAG parameters include color, spark_jar, and obo_parser_spark_jar. These can be passed via DAG's params. So in the DAG definition, we can set params with default values. For example:

default_args = {
    # ... other args ...
    'params': {
        'color': None,
        'spark_jar': 'path/to/spark.jar',
        'obo_parser_spark_jar': 'path/to/obo-parser.jar'
    }
}

But in Airflow, the params are usually set in the DAG object, not default_args. So the DAG would have a 'params' parameter. Wait, in Airflow, you can define default parameters via the 'default_args' or directly in the DAG. For example:

with DAG(
    ...,
    params={
        'color': None,
        'spark_jar': 'path/to/spark.jar',
        'obo_parser_spark_jar': 'path/to/obo-parser.jar'
    },
) as dag:

But the problem says the pipeline supports these as configurable parameters. So perhaps these are set as DAG parameters that can be overridden at runtime.

Now, putting it all together:

Imports needed: DAG, PythonOperator, maybe others. Since there's no branching, no need for BranchPythonOperator. The Slack notification can use the SlackAPIPostOperator from airflow.providers.slack.operators.slack_webhook. But the user's constraints say to use Airflow 2.x style imports. So assuming that the Slack operator is available via the provider package. However, the problem says to return executable code assuming required packages are installed. So including the necessary import for SlackAPIPostOperator.

Wait, the pipeline steps include 'slack' as a task that sends a notification. So the code should use the Slack operator. But the user's constraints specify to use PythonOperator if possible. Alternatively, the SlackAPIPostOperator is the correct choice here.

But the problem says to use proper imports. So the code should import SlackAPIPostOperator from airflow.providers.slack.operators.slack_webhook. However, if the user doesn't have that provider installed, the code would not run. But the problem states to assume required packages are installed. So including that import is acceptable.

Putting together the tasks:

- params_validate: PythonOperator that checks if 'color' is provided in the parameters. If not, raises an error. Otherwise, proceeds.

- download_mondo_terms: PythonOperator that checks GitHub for the latest OBO file, compares versions, downloads if newer, uploads to S3. If no new version, skips the task (raises AirflowSkipException).

- normalized_mondo_terms: PythonOperator (or SparkSubmitOperator) that processes the OBO file with Spark. Depends on download_mondo_terms.

- index_mondo_terms: PythonOperator (or SparkSubmitOperator) that indexes data into Elasticsearch using Spark. Depends on normalized_mondo_terms.

- publish_mondo: PythonOperator that publishes the data. Depends on index_mondo_terms.

- slack: SlackAPIPostOperator that sends a notification. Depends on publish_mondo, but with trigger_rule='all_done' to run even if publish fails.

Wait, but the dependencies are linear. So if any task fails, subsequent tasks are skipped. So if download_mondo_terms is skipped (no new version), then normalized_mondo_terms and subsequent tasks are skipped. The slack task would still run if it's set to trigger_rule='all_done', but only if its upstream is publish_mondo. So if publish_mondo is skipped, then the slack task would be skipped unless the trigger_rule is 'none_failed' or 'all_done'. Hmm, perhaps the slack task should be set to trigger after all previous tasks, regardless of their state. But in a linear chain, the slack task is downstream of publish_mondo. So if any task before publish_mondo fails, publish_mondo is not executed, and the slack task would not run unless its trigger_rule is 'all_done'. Wait, the trigger_rule determines whether the task runs based on the state of its direct upstream tasks. So if the slack task's upstream is publish_mondo, then when publish_mondo is failed, the slack task would run only if trigger_rule is 'all_done'. If publish_mondo is skipped (because an upstream task was skipped), then the slack task would run if trigger_rule is 'all_done' (since skipped is a state). So setting the slack task's trigger_rule to 'all_done' would make it run regardless of the state of publish_mondo.

So the code for the slack task would be:

slack_task = SlackAPIPostOperator(
    task_id='slack',
    channel='#notifications',
    username='airflow',
    text='Pipeline completed',
    trigger_rule='all_done',
    # other parameters like slack_webhook_conn_id
)

But the user's code needs to have proper parameters. The SlackAPIPostOperator requires a connection ID. So the code should use, for example, slack_webhook_conn_id='slack_webhook_default', which is a connection set up in Airflow. But since the code must be executable, perhaps the connection ID is a placeholder, and the user is expected to have it configured.

Now, putting all together:

The DAG is named, schedule_interval is None, catchup=False. Default_args include start_date, maybe owner, retries, etc.

The code outline:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackAPIPostOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def validate_params(**kwargs):
    color = kwargs['params'].get('color')
    if not color:
        raise ValueError('Color parameter is required')
    # Additional validation if needed

def download_mondo(**kwargs):
    # Check GitHub for latest version
    # Compare with existing version in S3
    # If new version exists, download and upload to S3
    # Else, raise AirflowSkipException
    new_version_available = False  # Placeholder
    if not new_version_available:
        raise AirflowSkipException('No new version available')

def normalize_mondo(**kwargs):
    # Run Spark job to normalize terms
    pass

def index_mondo(**kwargs):
    # Run Spark job to index to Elasticsearch
    pass

def publish_mondo(**kwargs):
    # Publish the indexed data
    pass

with DAG(
    'mondo_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mondo ontology data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        'color': None,
        'spark_jar': 'path/to/spark.jar',
        'obo_parser_spark_jar': 'path/to/obo-parser.jar'
    },
) as dag:

    params_validate = PythonOperator(
        task_id='params_validate',
        python_callable=validate_params,
        provide_context=True,
    )

    download_mondo_terms = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo,
        provide_context=True,
    )

    normalized_mondo_terms = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=normalize_mondo,
        provide_context=True,
    )

    index_mondo_terms = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_mondo,
        provide_context=True,
    )

    publish_mondo = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_mondo,
        provide_context=True,
    )

    slack = SlackAPIPostOperator(
        task_id='slack',
        slack_webhook_conn_id='slack_webhook_default',
        message='Mondo ETL pipeline completed',
        trigger_rule='all_done',
    )

    params_validate >> download_mondo_terms >> normalized_mondo_terms >> index_mondo_terms >> publish_mondo >> slack

Wait, but the problem mentions that the failure handling uses NONE_FAILED trigger rule. Which task uses that? The publish_mondo task? Or the slack task? The description says "Failure Handling: Uses NONE_FAILED trigger rule and includes Slack notifications for task failures". So perhaps the publish_mondo task has trigger_rule='none_failed', but that doesn't make sense in a linear chain. Alternatively, maybe the slack task uses trigger_rule='none_failed', but that would mean it runs only if all upstream tasks succeeded or were skipped. But the user wants the Slack notification to be sent upon completion, which includes failures. So perhaps the trigger_rule for the slack task is 'all_done', which allows it to run regardless of upstream success or failure.

In the code above, the slack task's trigger_rule is set to 'all_done', so it will run after publish_mondo regardless of whether publish_mondo succeeded or failed.

But according to the problem statement, the failure handling uses NONE_FAILED trigger rule. Maybe that refers to the publish task. For example, if any previous task failed, publish is skipped. But in a linear chain, if any task fails, subsequent tasks are skipped. So perhaps the trigger_rule is not needed here. The problem statement may have conflicting information. But given the pipeline steps are strictly linear, the trigger_rule may not be necessary unless there's branching. Since there's no branching, perhaps the NONE_FAILED is not used here, and the mention in the problem is a mistake. Alternatively, perhaps the publish task uses trigger_rule='none_failed' to ensure that it runs even if some upstream tasks are skipped. But in a linear chain, if a task is skipped, all downstream tasks are skipped unless trigger_rule is 'none_failed' or 'all_done'. For example, if download_mondo_terms is skipped (no new version), then normalized_mondo_terms is skipped, and so on. The publish task would be skipped. But if the publish task's trigger_rule is 'none_failed', then it would run if all upstream tasks succeeded or were skipped. Wait, 'none_failed' means that the task runs if all upstream tasks did not fail (i.e., succeeded or skipped). So if download_mondo_terms is skipped, then normalized_mondo_terms is skipped, index is skipped, and publish is skipped. But if publish has trigger_rule='none_failed', then it would run if all upstream tasks are either succeeded or skipped. So in the case where download is skipped, normalized, index, and publish are skipped by default. But if publish's trigger_rule is 'none_failed', then it would run even if upstream tasks are skipped. Wait, no. The trigger_rule applies to the immediate upstream tasks. So publish's upstream is index_mondo_terms. If index is skipped, then publish would be skipped unless its trigger_rule is 'none_failed'. So if index is skipped, publish's trigger_rule='none_failed' would allow it to run. But in the linear chain, if download is skipped, normalized is skipped, index is skipped, and publish's trigger_rule is 'none_failed', then publish would run. But according to the pipeline description, the publish step should only run if the previous steps succeeded. So perhaps the trigger_rule is not needed here. Maybe the problem statement's mention of NONE_FAILED is a mistake, or perhaps it's referring to the slack task. Alternatively, perhaps the publish task is supposed to run even if some tasks are skipped. For example, if download is skipped (no new data), then normalized and index are skipped, but publish should still run to publish existing data. In that case, the publish task's trigger_rule would be 'none_failed', allowing it to run even if upstream tasks are skipped. But according to the pipeline steps, publish comes after index, so if index is skipped, publish would normally be skipped. To have publish run even if index is skipped, set its trigger_rule to 'none_failed'. But according to the problem's description, the pipeline steps are strictly linear. So perhaps the NONE_FAILED trigger rule is used for the publish task. Let me adjust the code accordingly.

So, the publish_mondo task would have trigger_rule='none_failed', allowing it to run even if upstream tasks are skipped. For example, if download is skipped (no new data), then normalized and index are skipped, but publish can still run. But according to the pipeline steps, publish comes after index. So maybe the intention is that if download is skipped (no new data), then normalized, index, and publish are skipped. But the problem's skip logic says tasks can be skipped if data is already up-to-date. So if download is skipped, then the rest of the tasks are skipped. Therefore, the publish task would not run. But the problem's failure handling says to use NONE_FAILED trigger rule. Maybe this applies to the slack task. For example, the slack task should run even if some tasks failed. But in the current setup, the slack task is downstream of publish, so if publish is skipped or failed, the slack task would run only if its trigger_rule is 'all_done'. So perhaps the slack task's trigger_rule is 'all_done', and the publish task's trigger_rule is 'none_failed'. But I'm not sure. Given the confusion, perhaps the problem's mention of NONE_FAILED refers to the slack task. Alternatively, perhaps the publish task uses 'none_failed' to allow it to run even if some upstream tasks are skipped, but that's not clear. Given the ambiguity, perhaps the correct approach is to set the slack task's trigger_rule to 'all_done' to ensure it runs regardless of the success or failure of the publish task.

Another point: the pipeline's concurrency is limited to one active task and one active run. So in the DAG, set concurrency=1 and max_active_runs=1.

Putting all together, the code would look like this:

Imports:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackAPIPostOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

Default args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

Functions for each task:

def validate_params(**kwargs):
    color = kwargs['params'].get('color')
    if not color:
        raise ValueError("The 'color' parameter must be provided.")
    # Additional validation if needed

def download_mondo_terms(**kwargs):
    # Check for new version
    new_version = False  # Placeholder logic
    if not new_version:
        raise AirflowSkipException("No new version available; skipping.")

def process_normalization(**kwargs):
    # Spark processing logic
    pass

def index_terms(**kwargs):
    # Spark indexing to Elasticsearch
    pass

def publish_data(**kwargs):
    # Publish logic
    pass

DAG definition:

with DAG(
    'mondo_etl',
    default_args=default_args,
    description='ETL pipeline for Mondo ontology data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    params={
        'color': None,
        'spark_jar': 's3://path/to/spark.jar',
        'obo_parser_spark_jar': 's3://path/to/obo-parser.jar'
    },
) as dag:

    validate = PythonOperator(
        task_id='params_validate',
        python_callable=validate_params,
    )

    download = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download_mondo_terms,
    )

    normalize = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=process_normalization,
    )

    index = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=index_terms,
    )

    publish = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_data,
    )

    slack_notification = SlackAPIPostOperator(
        task_id='slack',
        slack_webhook_conn_id='slack_default',
        message='Mondo ETL pipeline completed.',
        trigger_rule='all_done',
    )

    validate >> download >> normalize >> index >> publish >> slack_notification

This code includes all tasks in a linear dependency, uses PythonOperators for processing, SlackAPIPostOperator for notifications, and sets the required parameters. The trigger_rule for the slack task is 'all_done' to ensure it runs regardless of the publish task's outcome. The download task raises AirflowSkipException if no new version is available, causing downstream tasks to be skipped. The validate_params function checks for the 'color' parameter and raises an error if missing. The DAG is configured with manual triggering, concurrency limits, and necessary parameters.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackAPIPostOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

default_args = {
    'owner': 'bio_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_tasks': 1,
}

def validate_color(**context):
    color = context['params'].get('color')
    if not color:
        raise ValueError("Required parameter 'color' not provided")
    context['ti'].xcom_push(key='color', value=color)

def check_mondo_version(**context):
    # Simulated version check
    current_version = '2023-09'
    last_imported = '2023-09'  # Would fetch from metadata store
    if current_version <= last_imported:
        raise AirflowSkipException('No newer Mondo version available')

def run_spark_normalization(**context):
    spark_jar = context['params']['obo_parser_spark_jar']
    print(f"Normalizing with Spark using {spark_jar}")

def run_es_indexing(**context):
    spark_jar = context['params']['spark_jar']
    color = context['ti'].xcom_pull(task_ids='params_validate', key='color')
    print(f"Indexing to ES with color {color} using {spark_jar}")

def publish_data(**context):
    print("Making data available in public API")

def slack_message(context):
    return f"Mondo ETL {'succeeded' if not context.get('exception') else 'failed'}"

with DAG(
    'mondo_etl_pipeline',
    default_args=default_args,
    description='Mondo ontology ETL pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        'color': 'production',
        'spark_jar': 's3://spark-jobs/mondo-indexer.jar',
        'obo_parser_spark_jar': 's3://spark-jobs/obo-parser.jar'
    },
    tags=['bio', 'mondo'],
) as dag:

    validate_task = PythonOperator(
        task_id='params_validate',
        python_callable=validate_color,
    )

    download_task = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=check_mondo_version,
    )

    normalize_task = PythonOperator(
        task_id='normalized_mondo_terms',
        python_callable=run_spark_normalization,
    )

    index_task = PythonOperator(
        task_id='index_mondo_terms',
        python_callable=run_es_indexing,
    )

    publish_task = PythonOperator(
        task_id='publish_mondo',
        python_callable=publish_data,
        trigger_rule='none_failed',
    )

    slack_task = SlackAPIPostOperator(
        task_id='slack',
        slack_webhook_conn_id='slack_alerts',
        message=slack_message,
        trigger_rule='all_done',
    )

    validate_task >> download_task >> normalize_task >> index_task >> publish_task >> slack_task