<think>
Okay, let's tackle this problem. The user wants to convert a pipeline description into an Apache Airflow DAG. Let me start by understanding the requirements and constraints.

First, the pipeline involves several steps: checking SFTP and shared folders, running parallel HTTP API extractions, synchronization points, a file upload, and a notification. The DAG needs to use Airflow 2.x style imports, proper task dependencies, and handle variables and configurations.

Starting with the imports. They mentioned using operators like KubernetesJobOperator and PythonOperator. Wait, KubernetesJobOperator isn't a standard Airflow operator. Oh, maybe they mean the KubernetesPodOperator? Or perhaps a custom operator. Since the problem mentions "Kubernetes job operator", I'll assume they're using the KubernetesPodOperator from airflow.providers.cncf.kubernetes. So I need to import that. Also, there's a need for EmptyOperator, PythonOperator, and maybe others like DummyOperator. Oh, and the ExternalTaskSensor isn't needed here, but there are sensors for files, but in the description, the first two tasks are Kubernetes jobs checking folders. Wait, the first two steps are "Check_PCD_SFTP_Folder" and "Check_PCD_Shared_Folder" as Kubernetes jobs. So those would be KubernetesPodOperators. Then there's Start_PCD_Extract_1 as an empty operator, which is a DummyOperator.

The parallel HTTP API extraction tasks all start after Start_PCD_Extract_1. Then Start_PCD_Extract_2 is triggered by Status_Tracker. Then all extraction tasks (including those triggered by Start_PCD_Extract_2?) need to complete before PCD_file_upload. Wait, the description says the parallel tasks are all triggered by Start_PCD_Extract_1, except that Start_PCD_Extract_2 is triggered by Status_Tracker. So the Start_PCD_Extract_2 is downstream of Status_Tracker. Then, the PCD_file_upload depends on all previous extraction tasks and Start_PCD_Extract_2.

The notification task (ETL_Notification) uses the all_done trigger rule. So that's a PythonOperator with trigger_rule='all_done'.

Now, the DAG's schedule is determined by an Airflow Variable 'pcd_etl_schedule', which can be a cron expression or None. So I need to retrieve that variable in the DAG definition. Also, default_args should include things like timeout, email settings (but maybe those are handled via variables?), and other parameters.

The DAG has a 60-minute timeout per run, catchup disabled. So default_args might have 'dagrun_timeout_sec': 60*60. And catchup=False in the DAG.

For the KubernetesPodOperator tasks, they need to reference the correct Kubernetes config. The user mentioned that configuration is via Airflow Variables, so perhaps the image, namespace, etc., are pulled from variables. But since the problem says to avoid placeholders, maybe I can use sensible defaults with comments indicating that variables would be used.

The notification task uses a Python function that checks the DAG run's status. How to do that? Maybe using the Airflow's context to get the task instances and determine success or failure. The function would send an email via Airflow's email backend, using variables for recipients.

Now, putting it all together:

1. Import necessary modules: DAG, KubernetesPodOperator, PythonOperator, DummyOperator, etc.
2. Define default_args with owner, depends_on_past, email settings (maybe from variables?), retries, etc.
3. Retrieve the schedule_interval from the Airflow variable 'pcd_etl_schedule', defaulting to None.
4. Define the DAG with the retrieved schedule_interval, catchup=False, default_args, and tags.
5. Define the tasks:
   - check_sftp and check_shared_folder as KubernetesPodOperators. They depend on each other (SFTP check first, then shared folder check).
   - start_extract1 is a DummyOperator that comes after check_shared_folder.
   - The parallel extraction tasks (Status_Tracker, Financial_Expense, etc.) all start after start_extract1. These are DummyOperators since the description says they're HTTP API extraction but doesn't specify the operator. Alternatively, maybe HttpSensor or SimpleHttpOperator? But the description says "empty operator" for Start_PCD_Extract_1, which is a Dummy. The parallel tasks are probably placeholders, so using DummyOperators here.
   - start_extract2 is triggered by Status_Tracker. So Status_Tracker >> start_extract2.
   - Then, all extraction tasks (including those parallel ones and start_extract2) must complete before pcd_file_upload.
   - pcd_file_upload is a KubernetesPodOperator.
   - etl_notification is a PythonOperator that runs after all upstream tasks, using trigger_rule='all_done'.

Wait, the PCD_file_upload is dependent on all previous extraction tasks. So after the parallel tasks and start_extract2, all of them must complete before the upload. So the dependencies would be: [all parallel tasks] >> pcd_file_upload, and start_extract2 >> pcd_file_upload? Or perhaps start_extract2 is part of the parallel tasks? Let me check the description again.

The parallel HTTP API extraction tasks are all triggered by Start_PCD_Extract_1. Then Start_PCD_Extract_2 is triggered by Status_Tracker. So Start_PCD_Extract_2 is downstream of Status_Tracker, and then all extraction tasks (including those parallel ones and Start_PCD_Extract_2) must complete before PCD_file_upload.

Wait, the description says:

- Start_PCD_Extract_2 is triggered by the Status_Tracker task.

So the dependencies are:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1 >> [all parallel tasks]

Among the parallel tasks, Status_Tracker triggers Start_PCD_Extract_2. Then, PCD_file_upload depends on all parallel tasks and Start_PCD_Extract_2.

So the flow is:

Check_SFTP >> Check_Shared >> Start1 >> [parallel tasks]

Status_Tracker >> Start2

Then, all parallel tasks (including Start2?) must complete before PCD_file_upload.

Wait, the parallel tasks are all triggered by Start1. So all those tasks (including Status_Tracker) start after Start1. Then, Status_Tracker triggers Start2. Then, Start2 is part of the upstream for PCD_file_upload. But also, all other parallel tasks (Financial_Expense, etc.) must complete before PCD_file_upload.

So the dependencies would be:

Start1 >> [Status_Tracker, Financial_Expense, ...]

Status_Tracker >> Start2

Then, PCD_file_upload requires [all parallel tasks] and Start2. But since Start2 is triggered by Status_Tracker, which is one of the parallel tasks, perhaps the dependencies are:

[all parallel tasks except Status_Tracker] >> PCD_file_upload

Status_Tracker >> Start2 >> PCD_file_upload

But that might not capture all dependencies. Alternatively, after Start2 is triggered, all other parallel tasks must complete, and Start2 must complete before PCD_file_upload.

Wait, the description says PCD_file_upload is dependent on all previous extraction tasks. So all the parallel extraction tasks (including Status_Tracker) and Start_PCD_Extract_2 must complete before the upload. But Start_PCD_Extract_2 is triggered by Status_Tracker. So the dependencies would be:

Start1 >> [task1, task2, ..., Status_Tracker, ...]

Status_Tracker >> Start2

Then, Start2 and all other parallel tasks must complete before PCD_file_upload.

But how to model that? Because the other parallel tasks (like Financial_Expense) are downstream of Start1. So PCD_file_upload needs to wait for all of them plus Start2.

So:

all_parallel_tasks = [Status_Tracker, Financial_Expense, ...]

all_parallel_tasks >> PCD_file_upload

But Status_Tracker also triggers Start2, which must also be upstream of PCD_file_upload.

Wait, no. Because Start2 is triggered by Status_Tracker, but Start2 itself is a task. So the dependencies would be:

Status_Tracker >> Start2

Then, Start2 must be upstream of PCD_file_upload. But also, all other parallel tasks (Financial_Expense, etc.) must be upstream of PCD_file_upload.

So:

all_parallel_tasks = [Status_Tracker, Financial_Expense, ...]

all_parallel_tasks >> PCD_file_upload

Start2 >> PCD_file_upload

But that's redundant because Status_Tracker is part of all_parallel_tasks. So when Status_Tracker completes, Start2 runs, and then Start2 must complete before PCD_file_upload. But the other tasks (Financial_Expense, etc.) are also part of all_parallel_tasks and must complete before PCD_file_upload.

Wait, but the Start_PCD_Extract_2 is triggered by Status_Tracker, which is one of the parallel tasks. So the flow is:

Start1 triggers all parallel tasks (including Status_Tracker).

Status_Tracker runs, then triggers Start2.

Then, PCD_file_upload must wait for:

- All other parallel tasks (Financial_Expense, etc.) to complete.

- Start2 to complete.

But how to model that? Because the other parallel tasks are independent of Start2.

So perhaps:

all_parallel_tasks = [Status_Tracker, Financial_Expense, ...]

Start1 >> all_parallel_tasks

Status_Tracker >> Start2

Then, PCD_file_upload is downstream of [all_parallel_tasks] and Start2.

But how to combine those dependencies? Because all_parallel_tasks includes Status_Tracker, which triggers Start2. So:

all_parallel_tasks >> PCD_file_upload

Start2 >> PCD_file_upload

But that would mean PCD_file_upload depends on all_parallel_tasks and Start2. But since Start2 is downstream of Status_Tracker (which is part of all_parallel_tasks), this would create a situation where PCD_file_upload waits for all_parallel_tasks and Start2. But since Start2 is triggered by Status_Tracker, which is part of all_parallel_tasks, perhaps the correct way is:

all_parallel_tasks >> PCD_file_upload

But Start2 is not part of all_parallel_tasks. Wait, no. The Start_PCD_Extract_2 is a separate task, not part of the parallel extraction tasks. So the parallel extraction tasks are the list of tasks under "Parallel HTTP API Extraction Tasks", which includes Status_Tracker. Then, Start_PCD_Extract_2 is a separate task triggered by Status_Tracker.

So the dependencies are:

Check_SFTP >> Check_Shared >> Start1 >> [Status_Tracker, Financial_Expense, ...]

Status_Tracker >> Start2

Then, PCD_file_upload must be downstream of [Status_Tracker, Financial_Expense, ...] and Start2.

Wait, but that's not possible because Start2 is downstream of Status_Tracker. So PCD_file_upload needs to wait for:

- All tasks in the parallel extraction group (Status_Tracker, Financial_Expense, etc.) to complete.

- Start2 to complete.

But since Start2 is downstream of Status_Tracker, which is part of the parallel group, perhaps the correct way is to have all parallel tasks (including Status_Tracker) plus Start2 as upstream for PCD_file_upload.

But how to express that? Because the parallel tasks are all triggered by Start1. So:

Start1 >> [task1, task2, ..., Status_Tracker, ...]

Status_Tracker >> Start2

Then, PCD_file_upload is downstream of [task1, task2, ..., Status_Tracker, ...] and Start2.

But since Start2 is downstream of Status_Tracker, which is part of the parallel tasks, perhaps the correct way is:

[task1, task2, ..., Status_Tracker >> Start2, ...] >> PCD_file_upload

But that's not straightforward. Maybe using a list and then the >> operator.

Alternatively, collect all the parallel tasks (excluding Status_Tracker) into a list, then:

Start1 >> parallel_tasks_except_status_tracker

Start1 >> Status_Tracker

Status_Tracker >> Start2

Then, PCD_file_upload is downstream of parallel_tasks_except_status_tracker, Status_Tracker, and Start2.

But that's complicated. Alternatively, after Start1 triggers all parallel tasks (including Status_Tracker), and Status_Tracker triggers Start2, then PCD_file_upload must wait for all parallel tasks and Start2.

So:

all_parallel_tasks = [Status_Tracker, Financial_Expense, ...]

Start1 >> all_parallel_tasks

Status_Tracker >> Start2

Then, PCD_file_upload is downstream of all_parallel_tasks and Start2.

But how to combine these dependencies? Because all_parallel_tasks includes Status_Tracker, which triggers Start2. So:

all_parallel_tasks >> PCD_file_upload

Start2 >> PCD_file_upload

But this would mean PCD_file_upload depends on all_parallel_tasks and Start2. However, since Start2 is already downstream of Status_Tracker (which is part of all_parallel_tasks), this would create a situation where PCD_file_upload waits for all_parallel_tasks to complete and Start2 to complete. But Start2 is triggered after Status_Tracker completes, which is part of all_parallel_tasks.

So, for example, if all_parallel_tasks take varying times, but Status_Tracker completes first and triggers Start2, which completes, but other parallel tasks are still running. Then PCD_file_upload would wait until all_parallel_tasks and Start2 are done. But since Start2 is already done, it's just waiting for the remaining parallel tasks.

Wait, no. Because all_parallel_tasks are started by Start1. So all_parallel_tasks run in parallel. Suppose Status_Tracker finishes first, then Start2 runs. But other tasks may still be running. So PCD_file_upload needs to wait for all_parallel_tasks (including Status_Tracker) and Start2. But since Start2 is triggered by Status_Tracker, which is part of all_parallel_tasks, the dependencies would be:

all_parallel_tasks >> PCD_file_upload

Start2 >> PCD_file_upload

But this would mean PCD_file_upload has two upstream dependencies: all_parallel_tasks and Start2. However, since all_parallel_tasks includes Status_Tracker, which triggers Start2, the PCD_file_upload would wait for all_parallel_tasks to complete and Start2 to complete. But Start2 is already downstream of Status_Tracker. So if all_parallel_tasks complete, then Start2 must have already completed (since Status_Tracker is part of all_parallel_tasks). Wait, no. Because Status_Tracker is part of all_parallel_tasks. So when all_parallel_tasks complete, Status_Tracker has completed, which triggered Start2. So Start2 would have completed before all_parallel_tasks? Not necessarily. Because other tasks in all_parallel_tasks may take longer than Status_Tracker. For example, Status_Tracker runs quickly, triggers Start2, which runs and completes. But other tasks in all_parallel_tasks are still running. So PCD_file_upload would have to wait for all_parallel_tasks (including those still running) and Start2 (already completed). So the Start2 dependency is redundant because Start2 is already completed once Status_Tracker is done. But other tasks may still be running.

Wait, no. The PCD_file_upload task depends on both all_parallel_tasks and Start2. So even if Start2 is done, PCD_file_upload will wait until all_parallel_tasks are done. But since Start2 is part of the dependencies, but Start2 is already done, it's just waiting for the remaining tasks. So perhaps the correct way is to have PCD_file_upload depend on all_parallel_tasks and Start2. But that's redundant because Start2 is downstream of Status_Tracker, which is part of all_parallel_tasks. So when all_parallel_tasks complete, Start2 must have already completed. Therefore, perhaps the dependency is just all_parallel_tasks >> PCD_file_upload.

But according to the pipeline description, PCD_file_upload is dependent on all previous extraction tasks, which includes Start_PCD_Extract_2. So Start_PCD_Extract_2 is a task that must complete before PCD_file_upload. Therefore, the correct dependencies are:

all_parallel_tasks >> PCD_file_upload

Start2 >> PCD_file_upload

But how to model that? Because all_parallel_tasks includes Status_Tracker, which triggers Start2. So:

all_parallel_tasks = [Status_Tracker, Financial_Expense, ...]

Start1 >> all_parallel_tasks

Status_Tracker >> Start2

Then, PCD_file_upload is downstream of all_parallel_tasks and Start2.

But in Airflow, you can't directly set multiple dependencies like that. You have to use the >> operator. So perhaps:

(all_parallel_tasks + [Start2]) >> PCD_file_upload

But how to combine lists. For example:

all_parallel_tasks = [task1, task2, ..., Status_Tracker, ...]

Start1 >> all_parallel_tasks

Status_Tracker >> Start2

Then:

PCD_file_upload << [ *all_parallel_tasks, Start2 ]

But in Python, you can't just add lists like that. So perhaps:

from airflow.utils.helpers import chain

chain(all_parallel_tasks, [Start2], PCD_file_upload)

But that's not correct. Alternatively, use the bitwise operators:

for task in all_parallel_tasks:
    task >> PCD_file_upload

Start2 >> PCD_file_upload

But that's more lines. Alternatively, use a list comprehension:

[task >> PCD_file_upload for task in all_parallel_tasks]

Start2 >> PCD_file_upload

But that's acceptable.

So putting it all together:

check_sftp = KubernetesPodOperator(...)

check_shared_folder = KubernetesPodOperator(...)

start_extract1 = DummyOperator(...)

parallel_tasks = [
    Status_Tracker_task,
    Financial_Expense_task,
    UPCC_Financial_Reportingr_task,
    # ... all other tasks
]

start_extract2 = DummyOperator(...)

Status_Tracker_task >> start_extract2

check_sftp >> check_shared_folder >> start_extract1 >> parallel_tasks

for task in parallel_tasks:
    task >> pcd_file_upload

start_extract2 >> pcd_file_upload

pcd_file_upload >> etl_notification

Wait, but the ETL_Notification task runs after all upstream tasks complete, regardless of success. So etl_notification should be downstream of pcd_file_upload and any other tasks? No, according to the description, ETL_Notification executes after all upstream tasks complete (all upstream tasks in the DAG). So the trigger rule is 'all_done', and it's set as the last task. So:

etl_notification = PythonOperator(
    trigger_rule='all_done',
    ...,
)

pcd_file_upload >> etl_notification

But etl_notification should depend on all tasks, not just pcd_file_upload. Wait, no. Because the notification task is supposed to execute after all upstream tasks complete. So in Airflow, if a task has no upstream dependencies, it runs immediately. But here, the notification task should run after all tasks in the DAG have completed. So how to model that? Because the DAG's tasks form a graph where some tasks are in parallel, and others are sequential.

The correct way is to have etl_notification be downstream of all other tasks. But how to set that? Because in Airflow, dependencies are explicit. So perhaps:

all_tasks_except_notification = [check_sftp, check_shared_folder, start_extract1, ...all parallel tasks, start_extract2, pcd_file_upload]

all_tasks_except_notification >> etl_notification

But that's cumbersome. Alternatively, since etl_notification is the last task, and it's triggered by all upstream tasks, perhaps:

etl_notification.set_upstream([pcd_file_upload, ...all other tasks])

But that's not feasible. Alternatively, use a dummy task that all other tasks point to, but that's not needed here. The correct approach is to set etl_notification as downstream of all tasks that are supposed to complete before it. However, given the DAG structure, the notification task should run after all tasks, regardless of their success. So the trigger_rule is 'all_done', and the dependencies should be set such that all tasks are upstream of etl_notification.

But how to set that? Because in the current structure, the tasks are:

check_sftp >> check_shared_folder >> start_extract1 >> parallel_tasks

parallel_tasks >> pcd_file_upload

start_extract2 >> pcd_file_upload

pcd_file_upload >> etl_notification

But this way, etl_notification only depends on pcd_file_upload. But according to the description, the notification task should execute after all upstream tasks complete (all tasks in the DAG). So if any task fails, the notification still runs. But with the current setup, if a task in parallel_tasks fails, pcd_file_upload may not run, and thus etl_notification would not run. But the trigger_rule is 'all_done', which means it runs when all upstream tasks are in a terminal state (success or failed). Wait, no. The trigger_rule applies to the task's direct upstream tasks. So if etl_notification's upstream is pcd_file_upload, then it would run when pcd_file_upload is done (success or failed). But according to the description, the notification task should execute after all upstream tasks (the entire DAG) have completed. So the notification task should have all other tasks as upstream.

But how to model that? Because in Airflow, each task's dependencies are explicit. So the notification task needs to be downstream of every other task in the DAG. That's impractical if there are many tasks, but in this case, perhaps we can collect all tasks except the notification into a list and set them as upstream.

But how to collect all tasks? Let's see:

The tasks are:

check_sftp

check_shared_folder

start_extract1

parallel_tasks (many tasks)

start_extract2

pcd_file_upload

So all_tasks = [check_sftp, check_shared_folder, start_extract1] + parallel_tasks + [start_extract2, pcd_file_upload]

Then, etl_notification should be downstream of all these tasks.

But how to set that? For example:

all_tasks = [check_sftp, check_shared_folder, start_extract1] + parallel_tasks + [start_extract2, pcd_file_upload]

for task in all_tasks:
    task >> etl_notification

But this would make etl_notification depend on all tasks. However, in the current setup, some tasks are already upstream of others. For example, check_sftp is upstream of check_shared_folder, which is upstream of start_extract1, etc. So if etl_notification depends on check_sftp, check_shared_folder, etc., then it would wait for all of them. But since dependencies are transitive, maybe it's sufficient to have etl_notification depend on the leaf nodes. But no, because if any task fails, the notification should still run. So the trigger_rule='all_done' ensures that even if upstream tasks have failed, the notification runs. But the notification task's trigger_rule is based on its direct upstream tasks. So if we set all tasks as upstream of etl_notification, then etl_notification will run when all of them are done. But that's a lot of dependencies. Alternatively, set etl_notification as downstream of the last task (pcd_file_upload) and any other tasks that are not in the path to pcd_file_upload. But in this DAG, all tasks are upstream of pcd_file_upload except for the notification task itself. Wait, no. For example, the parallel_tasks are upstream of pcd_file_upload, and start_extract2 is upstream of pcd_file_upload. So pcd_file_upload depends on all parallel_tasks and start_extract2. Then, pcd_file_upload is upstream of etl_notification. But if any of the parallel_tasks fail, pcd_file_upload may be skipped or failed, but the notification task should still run. However, with the current setup, if pcd_file_upload is upstream of etl_notification, and pcd_file_upload is skipped due to upstream failure, then etl_notification would not run unless its trigger_rule is set appropriately.

Wait, the notification task's trigger_rule is 'all_done', which means it runs when all its direct upstream tasks are in a terminal state (success or failed). So if etl_notification's upstream is pcd_file_upload, then it will run when pcd_file_upload is done. But according to the pipeline description, the notification should execute after all upstream tasks complete, regardless of their success. So the notification task should depend on all tasks in the DAG, not just pcd_file_upload.

Therefore, the correct approach is to make etl_notification depend on all tasks. But how to do that?

In Airflow, you can set multiple upstream dependencies. So:

etl_notification.set_upstream([check_sftp, check_shared_folder, start_extract1] + parallel_tasks + [start_extract2, pcd_file_upload])

But this is tedious. Alternatively, collect all tasks into a list and loop through them.

But in the code, after defining all tasks, collect them into a list and set each as upstream of etl_notification.

But how to collect all tasks? Let's see:

check_sftp = KubernetesPodOperator(...)

check_shared_folder = KubernetesPodOperator(...)

start_extract1 = DummyOperator(...)

parallel_tasks = [
    DummyOperator(task_id='Status_Tracker'),
    DummyOperator(task_id='Financial_Expense'),
    # ... all others
]

start_extract2 = DummyOperator(...)

pcd_file_upload = KubernetesPodOperator(...)

Then, all_tasks = [check_sftp, check_shared_folder, start_extract1] + parallel_tasks + [start_extract2, pcd_file_upload]

Then:

for task in all_tasks:
    task >> etl_notification

But this would make etl_notification depend on all tasks. However, in the DAG's structure, some tasks are already upstream of others. For example, check_sftp is upstream of check_shared_folder, which is upstream of start_extract1, etc. So when check_sftp completes, check_shared_folder runs, etc. But etl_notification depends on check_sftp directly. So even if check_shared_folder hasn't completed, but check_sftp has, etl_notification would run? No, because etl_notification's trigger_rule is 'all_done', which requires all upstream tasks to be in a terminal state. So if etl_notification has multiple upstream tasks, it will wait until all of them are done.

But in this case, the DAG's tasks form a linear chain plus parallel branches. For example:

check_sftp >> check_shared_folder >> start_extract1 >> parallel_tasks

parallel_tasks >> pcd_file_upload

start_extract2 >> pcd_file_upload

So, the tasks check_sftp, check_shared_folder, start_extract1, parallel_tasks, start_extract2, pcd_file_upload are all part of the DAG. The etl_notification task needs to depend on all of them. So by setting each of these tasks as upstream of etl_notification, the notification task will wait until all of them are done (success or failed) before executing.

But this approach may not be efficient, but it's necessary to ensure that the notification runs after all tasks. However, in practice, this could be done by making etl_notification a downstream task of all leaf nodes. But given the complexity, perhaps the simplest way is to collect all tasks and set them as upstream of etl_notification.

Now, the code structure:

Import necessary modules.

Define default_args.

Retrieve the schedule_interval from Airflow variable.

Define the DAG.

Define tasks:

check_sftp = KubernetesPodOperator(...)

check_shared_folder = KubernetesPodOperator(...)

start_extract1 = DummyOperator(...)

parallel_tasks = [DummyOperator for each of the listed extraction tasks]

start_extract2 = DummyOperator(...)

pcd_file_upload = KubernetesPodOperator(...)

etl_notification = PythonOperator(...)

Set dependencies:

check_sftp >> check_shared_folder >> start_extract1

start_extract1 >> parallel_tasks

Status_Tracker_task (one of the parallel_tasks) >> start_extract2

parallel_tasks >> pcd_file_upload

start_extract2 >> pcd_file_upload

all_tasks = [check_sftp, check_shared_folder, start_extract1] + parallel_tasks + [start_extract2, pcd_file_upload]

for task in all_tasks:
    task >> etl_notification

But wait, etl_notification is a PythonOperator with trigger_rule='all_done'. So even if some tasks failed, it will run after all are done.

Now, the code:

For the KubernetesPodOperator tasks, the user mentioned that they are Kubernetes job operators. So perhaps using the KubernetesPodOperator with certain parameters. But since the exact parameters (like image, cmds, etc.) are not provided, I'll use placeholders with comments indicating that variables would be used.

The notification function: need to send an email based on the DAG run's status. Using the Airflow email backend. The function can access the context, check the dag_run's status, and send an email. Recipients are from an Airflow variable.

Putting it all together:

The code would look like:

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'dagrun_timeout_sec': 60 * 60,
}

def send_etl_notification(**context):
    dag_run = context['dag_run']
    success = all(ti.state == 'success' for ti in dag_run.get_task_instances())
    message = 'ETL Pipeline Succeeded' if success else 'ETL Pipeline Failed'
    recipients = Variable.get('pcd_etl_notification_recipients', default_var=[]).split(',')
    # Assuming email backend is configured
    # Use Airflow's EmailOperator or send email via SMTP
    # For simplicity, print to logs
    print(f"Notification: {message}. Recipients: {recipients}")

with DAG(
    dag_id='pcd_etl_pipeline',
    schedule_interval=Variable.get('pcd_etl_schedule', default_var=None),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['pcd', 'etl'],
) as dag:

    check_sftp = KubernetesPodOperator(
        task_id='Check_PCD_SFTP_Folder',
        name='check-sftp-folder',
        namespace='default',
        image='sftp-checker:latest',
        cmds=['python', 'check_sftp.py'],
        arguments=['--folder', Variable.get('pcd_sftp_folder')],
    )

    check_shared_folder = KubernetesPodOperator(
        task_id='Check_PCD_Shared_Folder',
        name='check-shared-folder',
        namespace='default',
        image='shared-folder-checker:latest',
        cmds=['python', 'check_shared_folder.py'],
        arguments=['--path', Variable.get('pcd_shared_folder')],
    )

    start_extract1 = DummyOperator(task_id='Start_PCD_Extract_1')

    extraction_tasks = [
        DummyOperator(task_id='Status_Tracker'),
        DummyOperator(task_id='Financial_Expense'),
        DummyOperator(task_id='UPCC_Financial_Reportingr'),
        DummyOperator(task_id='CHC_Financial_reporting'),
        DummyOperator(task_id='PCN_Financial_Reporting'),
        DummyOperator(task_id='NPPCC_Financial_Reporting'),
        DummyOperator(task_id='Fiscal_Year_Reporting_Dates'),
        DummyOperator(task_id='UPCC_Primary_Care_Patient_Services'),
        DummyOperator(task_id='CHC_Primary_Care_Patient_Services'),
        DummyOperator(task_id='Practitioner_Role_Mapping'),
        DummyOperator(task_id='HR_Records'),
        DummyOperator(task_id='Provincial_Risk_Tracking'),
        DummyOperator(task_id='Decision_Log'),
        DummyOperator(task_id='HA_Hierarchy'),
        DummyOperator(task_id='UPPC_Budget'),
        DummyOperator(task_id='CHC_Budget'),
        DummyOperator(task_id='PCN_Budget'),
        DummyOperator(task_id='NPPCC_Budget'),
    ]

    start_extract2 = DummyOperator(task_id='Start_PCD_Extract_2')

    pcd_file_upload = KubernetesPodOperator(
        task_id='PCD_file_upload',
        name='pcd-file-upload',
        namespace='default',
        image='file-uploader:latest',
        cmds=['python', 'upload_files.py'],
        arguments=['--destination', Variable.get('pcd_upload_destination')],
    )

    etl_notification = PythonOperator(
        task_id='ETL_Notification',
        python_callable=send_etl_notification,
        trigger_rule='all_done',
        provide_context=True,
    )

    check_sftp >> check_shared_folder >> start_extract1
    start_extract1 >> extraction_tasks
    extraction_tasks[0] >> start_extract2  # Status_Tracker is the first task in extraction_tasks

    # Set dependencies for PCD_file_upload
    for task in extraction_tasks:
        task >> pcd_file_upload
    start_extract2 >> pcd_file_upload

    # Set all tasks as upstream of etl_notification
    all_tasks = [check_sftp, check_shared_folder, start_extract1] + extraction_tasks + [start_extract2, pcd_file_upload]
    for task in all_tasks:
        task >> etl_notification

Wait, but extraction_tasks[0] is Status_Tracker. So extraction_tasks[0] >> start_extract2.

Then, for the PCD_file_upload dependencies:

Each extraction task (including Status_Tracker) is upstream of pcd_file_upload, and start_extract2 is also upstream.

But in the code above, after the loop:

for task in extraction_tasks:
    task >> pcd_file_upload

start_extract2 >> pcd_file_upload

This sets all extraction tasks and start_extract2 as upstream of pcd_file_upload.

Then, etl_notification depends on all tasks, including check_sftp, check_shared_folder, etc.

But in the code, all_tasks includes check_sftp, check_shared_folder, start_extract1, extraction_tasks, start_extract2, pcd_file_upload. Then, each of these tasks is set as upstream of etl_notification.

This way, etl_notification will run after all tasks have completed, regardless of their outcome, due to the trigger_rule='all_done'.

But in the code, the etl_notification task is downstream of all tasks. So even if some tasks fail, etl_notification will run once all tasks are done.

The send_etl_notification function checks all task instances in the dag_run to determine success or failure.

But in Airflow, the 'success' status of the DAG run is determined by all tasks succeeding. However, the function here checks all task instances. So if any task failed, the notification reports failure.

This should meet the requirement.

Now,