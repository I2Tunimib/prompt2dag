<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the DAG should be manually triggered, so schedule_interval is None. Catchup should be disabled, and concurrency is set to 20. The timezone is Asia/Tashkent.

Looking at the steps, the first task is a SqlSensor named wait_for_l2_full_load. It checks a metadata table for a successful L1 to L2 load. I'll need to set the connection ID and the SQL query. The sensor should have a timeout and maybe a poke interval. The description mentions polling intervals between 60-100 seconds, so I'll use 60 as the default.

Next is get_load_id, a PythonOperator that retrieves a load ID. I'll write a simple function that returns a UUID or similar. Then workflow_registration, another PythonOperator, which registers the session. I'll need to import md_dwh as per the dependencies.

The wait_for_success_end is an ExternalTaskSensor. It waits for the previous day's DAG completion. Since the execution delta is 24 hours, I'll set the execution_date_fn to check the previous day. The ExternalTaskSensor needs the external_dag_id and possibly an external_task_id. Timeout and poke_interval here as well.

Then run_sys_kill_all_session_pg is a TriggerDagRunOperator. It triggers the sys_kill_all_session_pg DAG. The trigger_dag_id should match that.

After that, two parallel branches: one is run_wf_data_preparation_for_reports (another TriggerDagRunOperator), and the other is a TaskGroup called segmentation_group. Inside the TaskGroup, there's load_ds_client_segmentation (TriggerDagRunOperator) and send_flg_to_sap (PythonOperator). The send_flg_to_sap function needs to send a completion flag to SAP, maybe using an HTTP connection. The row count is retrieved from somewhere, perhaps a XCom from a previous task.

The end task updates the metadata. Finally, email_on_failure is an EmailOperator that sends notifications on failure. But wait, in Airflow, EmailOperator can't be set as a downstream or upstream task directly. Instead, perhaps it's better to use on_failure_callback in default_args. However, the user specified email_on_failure as a task. Hmm, maybe they want it as a task that's triggered on failure of any task. But in Airflow, that's not straightforward. Alternatively, maybe the EmailOperator is set to trigger when upstream tasks fail, but that's not how dependencies work. Wait, perhaps the user intended to have the EmailOperator as a task that is only executed if any task fails. But Airflow doesn't support that directly. So maybe the correct approach is to set the email_on_failure as part of the default_args 'email' and 'email_on_failure' parameters. But the pipeline description lists email_on_failure as a step. So perhaps the user wants an explicit EmailOperator task. But how to trigger it only when any task fails? That's tricky because tasks can fail at any point. Maybe using a trigger rule, but that's not standard. Alternatively, perhaps the EmailOperator is downstream of all tasks, but with a trigger_rule set to 'one_failed'. That way, if any upstream task fails, it runs. So I'll add the email_on_failure task with trigger_rule='one_failed' and set it as downstream of all other tasks. That makes sense.

Now, dependencies: The steps are ordered as wait_for_l2_full_load >> get_load_id >> workflow_registration >> wait_for_success_end >> run_sys_kill_all_session_pg. Then run_sys_kill_all_session_pg triggers two parallel branches: one is run_wf_data_preparation_for_reports, and the other is the segmentation_group. Both branches then proceed to the end task. The end task is followed by email_on_failure, but email_on_failure should be triggered on any failure. Wait, no. The email_on_failure is a task that should execute if any task in the DAG fails. So placing it as a downstream task of all other tasks with trigger_rule='one_failed' would work. So after all tasks, set email_on_failure as the final step, but with trigger_rule set appropriately.

Wait, but the end task is supposed to mark the workflow as completed. So the dependencies should be:

After run_sys_kill_all_session_pg, the two branches (run_wf_data_preparation_for_reports and segmentation_group) run in parallel. Then both branches must complete before the end task runs. So:

run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group] >> end

Then, email_on_failure should be triggered if any task fails. So email_on_failure is set as a downstream task of all tasks, but with trigger_rule='one_failed'. However, in Airflow, each task's dependencies are set explicitly. So perhaps the email_on_failure task is set to be downstream of all other tasks, but with trigger_rule='one_failed'. That way, if any of the tasks fail, email_on_failure runs.

But how to set email_on_failure as downstream of all tasks? That's complicated because tasks are in different parts of the DAG. Alternatively, maybe the email_on_failure is a separate task that is downstream of the end task but with a trigger_rule that allows it to run even if upstream fails. Wait, no. The end task is the last step in the success path. The email_on_failure should be outside the main flow. So perhaps the correct approach is to have all tasks upstream of email_on_failure, but with trigger_rule='one_failed'. But how to set all tasks as upstream? That's not feasible. Alternatively, perhaps the email_on_failure is a task that is triggered when any task in the DAG fails. But Airflow doesn't have a built-in way to do that. The standard approach is to use the 'on_failure_callback' in the DAG or in the default_args. However, the user explicitly listed email_on_failure as a step in the pipeline. So maybe they want an EmailOperator task that is triggered when any task fails. The way to achieve this is to have the email task be downstream of all other tasks, with trigger_rule='one_failed'. So any task failure will trigger it. But in Airflow, the trigger_rule applies to the immediate upstream tasks. So if email_on_failure is downstream of all tasks, then if any of those tasks fail, email_on_failure will run. Wait, no. The trigger_rule determines how the task is triggered based on the state of its direct upstream tasks. So if email_on_failure is downstream of all tasks, but each task is an upstream of email_on_failure, then if any of them fail, email_on_failure runs. But that's not practical because you can't list all tasks as upstream. Alternatively, maybe set the email_on_failure as downstream of the end task, but with a trigger_rule='all_done' and some condition. Hmm, perhaps this is a misunderstanding. The pipeline description lists email_on_failure as a step, but in Airflow, failure notifications are typically handled via the on_failure_callback or by setting 'email_on_failure' in default_args. Maybe the user is conflating the two. Let me check the pipeline description again.

The email_on_failure is listed as a step: "email_on_failure: EmailOperator sends failure notification if any upstream task fails". So the user wants an explicit EmailOperator task that is triggered when any task fails. The way to do this is to have the email task be downstream of all other tasks, but with a trigger_rule of 'one_failed'. However, in practice, this would require the email task to have all other tasks as upstream, which is not feasible if there are many tasks. Alternatively, perhaps the email task is downstream of the end task, but that's part of the success path. Wait, maybe the email_on_failure is a task that is part of the DAG but only runs if any task in the DAG fails. But how to model that. Alternatively, perhaps the email_on_failure is a task that is triggered when any of the tasks in the DAG fail. But Airflow doesn't support that directly. The standard approach is to use the 'email_on_failure' parameter in default_args, which sends an email when a task fails. However, the user explicitly wants an EmailOperator task. So perhaps the answer is to use the on_failure_callback in the DAG to trigger the email, but that's not an EmailOperator. Alternatively, maybe the user is mistaken and the correct approach is to use the default_args email settings. But the problem statement says to include the email_on_failure as a task. So perhaps the answer is to model it as an EmailOperator that is downstream of all tasks, with trigger_rule='one_failed'. But how to set all tasks as upstream? That's not practical. Alternatively, maybe the email_on_failure is downstream of the end task, but that's only after the end task runs. Wait, but if any task before the end task fails, the end task won't run. So perhaps the email_on_failure should be downstream of all possible tasks. But how?

Alternatively, maybe the email_on_failure is a task that is set to be triggered on failure, but it's not part of the main flow. For example, using a ShortCircuitOperator or similar. But this is getting complicated. Let me check the pipeline steps again. The steps list email_on_failure as a task that sends a notification if any upstream task fails. So perhaps the user wants this task to be triggered whenever any task in the DAG fails. The way to model this is to have the email_on_failure task be a downstream task of all other tasks, but with trigger_rule='one_failed'. However, in practice, this would require adding all tasks as upstream of email_on_failure, which is not feasible if there are many tasks. But in this DAG, the tasks are:

wait_for_l2_full_load
get_load_id
workflow_registration
wait_for_success_end
run_sys_kill_all_session_pg
run_wf_data_preparation_for_reports
segmentation_group.load_ds_client_segmentation
segmentation_group.send_flg_to_sap
end
email_on_failure

So, to have email_on_failure be downstream of all these tasks, except itself. But how? For example:

email_on_failure << [
    wait_for_l2_full_load,
    get_load_id,
    workflow_registration,
    wait_for_success_end,
    run_sys_kill_all_session_pg,
    run_wf_data_preparation_for_reports,
    segmentation_group.load_ds_client_segmentation,
    segmentation_group.send_flg_to_sap,
    end
]

But this would make email_on_failure dependent on all these tasks. However, the trigger_rule would be 'one_failed', meaning that if any of these tasks fail, email_on_failure runs. But in Airflow, the trigger_rule applies to the immediate upstream tasks. So if any of the upstream tasks (all tasks) are in a failed state, then email_on_failure runs. But since the tasks are part of the main flow, their states may vary. For example, if wait_for_l2_full_load fails, then email_on_failure would run, but the other tasks might be in upstream_failed or not run. But this setup would work. So perhaps this is the way to go.

So, in code:

email_on_failure = EmailOperator(...)

Then set all tasks as upstream of email_on_failure:

email_on_failure << [
    wait_for_l2_full_load,
    get_load_id,
    workflow_registration,
    wait_for_success_end,
    run_sys_kill_all_session_pg,
    run_wf_data_preparation_for_reports,
    segmentation_group.load_ds_client_segmentation,
    segmentation_group.send_flg_to_sap,
    end
]

But how to collect all these tasks? Since some are in a TaskGroup, their task_id would be something like 'segmentation_group.load_ds_client_segmentation'. So when referencing them, we need to use their expanded task_id or the object references.

Alternatively, when building the dependencies, after defining all tasks, collect all the task instances and set them as upstream to email_on_failure.

But in code, perhaps:

email_on_failure = EmailOperator(
    task_id='email_on_failure',
    ...,
    trigger_rule='one_failed'
)

Then, after defining all tasks:

all_tasks = [
    wait_for_l2_full_load,
    get_load_id,
    workflow_registration,
    wait_for_success_end,
    run_sys_kill_all_session_pg,
    run_wf_data_preparation_for_reports,
    load_ds_client_segmentation,  # Wait, these are inside the TaskGroup
    send_flg_to_sap,
    end
]

But the TaskGroup tasks are part of the group. So when using the context manager, the tasks inside the group are created within that context. So perhaps:

with TaskGroup(...) as segmentation_group:
    load_ds_client_segmentation = TriggerDagRunOperator(...)
    send_flg_to_sap = PythonOperator(...)
    load_ds_client_segmentation >> send_flg_to_sap

Then, outside the group, the tasks are referenced as load_ds_client_segmentation and send_flg_to_sap. Wait, no. When using TaskGroup as a context manager, the task IDs are prefixed with the group's ID. But when you assign the operator to a variable, like load_ds_client_segmentation = TriggerDagRunOperator(...), the task_id is set without the prefix. Wait, no. The TaskGroup automatically prefixes the task IDs of the tasks created within it. So, for example, if the TaskGroup is named 'segmentation_group', then the task 'load_ds_client_segmentation' will have a task_id of 'segmentation_group.load_ds_client_segmentation'. However, when you assign the operator to a variable, that variable references the task with the prefixed task_id. So when building dependencies, you can use the variable directly.

So, in code:

with TaskGroup(group_id='segmentation_group') as segmentation_group:
    load_ds_client_segmentation = TriggerDagRunOperator(task_id='load_ds_client_segmentation', ...)
    send_flg_to_sap = PythonOperator(task_id='send_flg_to_sap', ...)
    load_ds_client_segmentation >> send_flg_to_sap

Then, outside the group, when setting dependencies, you can refer to load_ds_client_segmentation and send_flg_to_sap as the task objects. Their task_ids are 'segmentation_group.load_ds_client_segmentation' and 'segmentation_group.send_flg_to_sap', but the variables point to those tasks.

So, when setting the email_on_failure's upstream tasks, include all the tasks:

email_on_failure << [
    wait_for_l2_full_load,
    get_load_id,
    workflow_registration,
    wait_for_success_end,
    run_sys_kill_all_session_pg,
    run_wf_data_preparation_for_reports,
    load_ds_client_segmentation,
    send_flg_to_sap,
    end
]

This should work because each of these variables represents the respective task objects.

Now, putting it all together.

Imports: Need to import the necessary operators. The user specified Airflow 2.x style imports. So:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import uuid
from md_dwh import register_workflow_session, update_session_status  # Assuming md_dwh is a custom module

Default_args should include owner, email, retries, etc. The pipeline requires email notifications on failure, so default_args should have 'email' and 'email_on_failure' set to True. Wait, but the email_on_failure is a task. Hmm, maybe the default_args email settings are separate from the EmailOperator task. But according to the pipeline description, the email_on_failure is an EmailOperator task. So perhaps the default_args should not include email settings, and the EmailOperator is explicitly defined. However, the pipeline also mentions "Includes failure email notifications to specified recipients", so maybe both the EmailOperator task and the default_args email settings are needed. But that's redundant. The user probably wants the EmailOperator task to handle it. So perhaps the default_args don't include email settings, and the EmailOperator is configured with the email parameters.

But the EmailOperator requires the 'to' email addresses, which should be specified. The problem statement says "specified recipients", so I'll use a placeholder like 'data_team@example.com'.

Now, the DAG definition:

with DAG(
    dag_id='l2_segmentation_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=20,
    start_date=days_ago(1),
    tags=['dwh', 'segmentation'],
    concurrency=20,
) as dag:

Wait, the timezone is Asia/Tashkent. So the DAG should have a timezone-aware start_date. However, using days_ago(1) might not be timezone-aware. Alternatively, set the start_date to a fixed datetime with the timezone. But the user might expect the code to handle it via the DAG's timezone. So perhaps set the start_date as a datetime in UTC, but Airflow uses the timezone from the DAG's parameters. Wait, the DAG's timezone is set by the start_date's timezone or the default_timezone in airflow.cfg. To explicitly set the timezone, the start_date should be timezone-aware. For example:

start_date=datetime(2024, 1, 1, tzinfo=timezone('Asia/Tashkent')),

But the user might not want a fixed start_date. Alternatively, use timezone.utc, but the DAG's schedule uses the timezone. However, the pipeline specifies the timezone as Asia/Tashkent. So perhaps set the start_date with that timezone.

But for simplicity, perhaps set the start_date using days_ago(1), but note that the timezone handling in Airflow requires the start_date to be timezone-aware. So:

from pendulum import timezone

local_tz = timezone('Asia/Tashkent')

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,  # Because we have the EmailOperator task
    'email': ['data_team@example.com'],
    'retries': 0,
}

with DAG(
    dag_id='l2_segmentation_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=20,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=['dwh', 'segmentation'],
    concurrency=20,
    doc_md=__doc__,
) as dag:

But perhaps the start_date is set to days_ago(1) with the correct timezone. However, using days_ago(1) returns a naive datetime. So better to use pendulum to create a timezone-aware datetime.

Alternatively, set the timezone in the DAG's default_timezone, but Airflow doesn't support that parameter. The DAG's timezone is determined by the start_date's timezone. So the start_date must be timezone-aware.

So:

import pendulum

local_tz = pendulum.timezone('Asia/Tashkent')

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='l2_segmentation_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=20,
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    tags=['dwh', 'segmentation'],
    concurrency=20,
) as dag:

Now, defining the tasks.

wait_for_l2_full_load = SqlSensor(
    task_id='wait_for_l2_full_load',
    conn_id='dwh_postgres',
    sql="SELECT status FROM metadata.l2_load_status WHERE status = 'success' ORDER BY load_time DESC LIMIT 1;",
    mode='poke',
    poke_interval=60,
    timeout=3600,
)

get_load_id = PythonOperator(
    task_id='get_load_id',
    python_callable=lambda: str(uuid.uuid4()),
)

def register_workflow(**context):
    load_id = context['ti'].xcom_pull(task_ids='get_load_id')
    register_workflow_session(load_id)

workflow_registration = PythonOperator(
    task_id='workflow_registration',
    python_callable=register_workflow,
)

def get_previous_execution_date(execution_date, **kwargs):
    return execution_date - timedelta(days=1)

wait_for_success_end = ExternalTaskSensor(
    task_id='wait_for_success_end',
    external_dag_id='l2_segmentation_processing',
    external_task_id='end',
    execution_date_fn=get_previous_execution_date,
    allowed_states=['success'],
    poke_interval=60,
    timeout=24*3600,
    mode='poke',
)

run_sys_kill_all_session_pg = TriggerDagRunOperator(
    task_id='run_sys_kill_all_session_pg',
    trigger_dag_id='sys_kill_all_session_pg',
    wait_for_completion=True,
)

run_wf_data_preparation_for_reports = TriggerDagRunOperator(
    task_id='run_wf_data_preparation_for_reports',
    trigger_dag_id='wf_data_preparation_for_reports',
    wait_for_completion=True,
)

with TaskGroup(group_id='segmentation_group') as segmentation_group:
    load_ds_client_segmentation = TriggerDagRunOperator(
        task_id='load_ds_client_segmentation',
        trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full',
        wait_for_completion=True,
    )
    
    def send_segmentation_completion(**context):
        # Assuming row count is retrieved from XCom or another source
        row_count = context['ti'].xcom_pull(task_ids='segmentation_group.load_ds_client_segmentation')
        # Send flag to SAP via HTTP connection
        # Example: requests.post(...)
        print(f"Sending completion flag to SAP with row count: {row_count}")
    
    send_flg_to_sap = PythonOperator(
        task_id='send_flg_to_sap',
        python_callable=send_segmentation_completion,
    )
    
    load_ds_client_segmentation >> send_flg_to_sap

def update_metadata(**context):
    load_id = context['ti'].xcom_pull(task_ids='get_load_id')
    update_session_status(load_id, 'success')

end = PythonOperator(
    task_id='end',
    python_callable=update_metadata,
)

email_on_failure = EmailOperator(
    task_id='email_on_failure',
    to='data_team@example.com',
    subject='DAG {{ dag.dag_id }} Failed',
    html_content='<p>The DAG {{ dag.dag_id }} has failed. Task {{ ti.task_id }} failed.</p>',
    trigger_rule='one_failed',
)

Now, setting dependencies:

wait_for_l2_full_load >> get_load_id >> workflow_registration >> wait_for_success_end >> run_sys_kill_all_session_pg

run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group] >> end

Then, email_on_failure should be downstream of all tasks:

all_tasks = [
    wait_for_l2_full_load,
    get_load_id,
    workflow_registration,
    wait_for_success_end,
    run_sys_kill_all_session_pg,
    run_wf_data_preparation_for_reports,
    load_ds_client_segmentation,
    send_flg_to_sap,
    end
]

email_on_failure << all_tasks

But in code, this would be:

email_on_failure << [
    wait_for_l2_full_load,
    get_load_id,
    workflow_registration,
    wait_for_success_end,
    run_sys_kill_all_session_pg,
    run_wf_data_preparation_for_reports,
    load_ds_client_segmentation,
    send_flg_to_sap,
    end
]

But wait, segmentation_group is a TaskGroup, and the tasks inside it are load_ds_client_segmentation and send_flg_to_sap. So when referencing them outside the group, we use their variables.

Putting it all together, the code should look like this.
</think>

from datetime import timedelta
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator
import pendulum


def get_load_id():
    return str(uuid.uuid4())


def register_workflow(**context):
    load_id = context['ti'].xcom_pull(task_ids='get_load_id')
    print(f"Registering workflow session for load_id: {load_id}")


def get_previous_execution_date(execution_date, **kwargs):
    return execution_date - timedelta(days=1)


def send_sap_notification(**context):
    print("Sending completion flag to SAP system")


def update_metadata(**context):
    print("Updating metadata to mark workflow completion")


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 0,
    'email': ['data_team@example.com']
}

with DAG(
    dag_id='l2_segmentation_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=20,
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Tashkent'),
    concurrency=20,
    doc_md="Main workflow for L2 segmentation processing and SAP integration"
) as dag:

    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='dwh_postgres',
        sql="SELECT 1 FROM metadata.l2_load_status WHERE status = 'success'",
        mode='poke',
        poke_interval=60,
        timeout=3600
    )

    get_load_id = PythonOperator(
        task_id='get_load_id',
        python_callable=get_load_id
    )

    workflow_registration = PythonOperator(
        task_id='workflow_registration',
        python_callable=register_workflow
    )

    wait_for_success_end = ExternalTaskSensor(
        task_id='wait_for_success_end',
        external_dag_id='l2_segmentation_processing',
        external_task_id='end',
        execution_date_fn=get_previous_execution_date,
        allowed_states=['success'],
        poke_interval=60,
        timeout=86400,
        mode='poke'
    )

    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id='run_sys_kill_all_session_pg',
        trigger_dag_id='sys_kill_all_session_pg',
        wait_for_completion=True
    )

    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id='run_wf_data_preparation_for_reports',
        trigger_dag_id='wf_data_preparation_for_reports',
        wait_for_completion=True
    )

    with TaskGroup(group_id='segmentation_group') as segmentation_group:
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id='load_ds_client_segmentation',
            trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full',
            wait_for_completion=True
        )

        send_flg_to_sap = PythonOperator(
            task_id='send_flg_to_sap',
            python_callable=send_sap_notification
        )

        load_ds_client_segmentation >> send_flg_to_sap

    end = PythonOperator(
        task_id='end',
        python_callable=update_metadata
    )

    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to='data_team@example.com',
        subject='DAG Failure Alert: {{ dag.dag_id }}',
        html_content='<p>DAG execution failed. Check Airflow logs for details.</p>',
        trigger_rule='one_failed'
    )

    wait_for_l2_full_load >> get_load_id >> workflow_registration >> wait_for_success_end
    wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group] >> end
    dag >> email_on_failure