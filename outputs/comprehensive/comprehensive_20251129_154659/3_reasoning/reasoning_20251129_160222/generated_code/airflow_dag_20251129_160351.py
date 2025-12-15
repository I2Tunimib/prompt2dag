<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the DAG involves several steps: checking SFTP and shared folders, parallel API extractions, synchronization points, a file upload, and a notification task. The user specified using Airflow 2.x style imports, so I'll make sure to import the correct operators.

The pipeline starts with two KubernetesJobOperators for checking folders. These are Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder, with the latter depending on the former. Then there's Start_PCD_Extract_1 as an empty operator (probably DummyOperator) to mark the start of parallel tasks. All the extraction tasks run in parallel after this. The Status_Tracker task triggers Start_PCD_Extract_2, which is another synchronization point. The PCD_file_upload depends on all extraction tasks and the second start task. Finally, the ETL_Notification uses a PythonOperator with trigger_rule set to all_done.

Wait, the notification task should run regardless of upstream success or failure. So the trigger_rule should be 'all_done'. Also, the email notification needs to check the DAG run's status. How to do that? Maybe using the Airflow context to get the task instances and determine if any failed.

For the DAG configuration, the schedule_interval is taken from an Airflow Variable 'pcd_etl_schedule', which could be a cron expression or None. The default_args should include the timeout (60 minutes), and catchup is disabled.

Now, the KubernetesJobOperator—since the user mentioned using it, but Airflow doesn't have a built-in KubernetesJobOperator. Wait, maybe they meant the KubernetesPodOperator? Or perhaps a custom operator? The description says "Kubernetes job operator", so maybe they're using a custom operator like KubernetesJobOperator from a provider. But since the user didn't specify, perhaps I should use the KubernetesPodOperator as a placeholder, assuming that the actual operator is available. Alternatively, use DummyOperator for the sake of example. But the user wants executable code. Hmm. Let me check the Airflow providers. The KubernetesPodOperator is part of the apache-airflow-providers-cncf-kubernetes package. So I should import KubernetesPodOperator from airflow.providers.cncf.kubernetes.operators.pod. But since the user didn't specify exact parameters, I'll use them with placeholders like config_file and arguments, but make sure the code is syntactically correct.

Wait, the user said to avoid placeholders. So maybe I can create the operators with necessary parameters but add comments indicating where variables would be used. For example, using Airflow Variables to get the job template paths.

The HTTP API extraction tasks are all parallel. Since they are all triggered by Start_PCD_Extract_1, they should be downstream of it. Then, the Status_Tracker task triggers Start_PCD_Extract_2, so Start_PCD_Extract_2 is downstream of Status_Tracker. Then, the PCD_file_upload is downstream of all extraction tasks and Start_PCD_Extract_2.

Wait, the description says PCD_file_upload is dependent on all previous extraction tasks. So all the parallel tasks (including Status_Tracker) must complete before PCD_file_upload runs. But Start_PCD_Extract_2 is triggered by Status_Tracker. So the dependencies would be:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> [all extraction tasks]

Then, Status_Tracker >> Start_PCD_Extract_2

But Start_PCD_Extract_2 is part of the upstream for PCD_file_upload. So PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. Wait, the description says PCD_file_upload is dependent on all previous extraction tasks. So maybe Start_PCD_Extract_2 is just another task that some tasks depend on, but the main upload depends on all extraction tasks. Let me re-read the step description.

In the pipeline steps:

- Start_PCD_Extract_2 is triggered by the Status_Tracker task. So Status_Tracker >> Start_PCD_Extract_2.

But the PCD_file_upload is dependent on all previous extraction tasks. So all the extraction tasks (including Status_Tracker) must complete before PCD_file_upload. Additionally, Start_PCD_Extract_2 is triggered by Status_Tracker, but does the upload depend on Start_PCD_Extract_2? The description says PCD_file_upload is dependent on all previous extraction tasks. So perhaps Start_PCD_Extract_2 is part of the extraction phase, and the upload depends on all extraction tasks plus Start_PCD_Extract_2? Or maybe Start_PCD_Extract_2 is part of the synchronization before the upload.

Wait, the description says:

Start_PCD_Extract_2 is an empty operator triggered by the Status_Tracker task. Then PCD_file_upload is dependent on all previous extraction tasks. So perhaps the upload depends on all extraction tasks and Start_PCD_Extract_2. Because Start_PCD_Extract_2 is part of the extraction phase.

So the dependencies would be:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> [all extraction tasks]

Status_Tracker >> Start_PCD_Extract_2

Then, all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2 must complete before PCD_file_upload.

Wait, but Start_PCD_Extract_2 is triggered by Status_Tracker. So after Status_Tracker completes, Start_PCD_Extract_2 runs. Then, PCD_file_upload must wait for all extraction tasks and Start_PCD_Extract_2.

But how to set that up? Let's list all extraction tasks:

The parallel extraction tasks are:

Status_Tracker

Financial_Expense

UPCC_Financial_Reportingr (note typo in original description: Reportingr)

CHC_Financial_reporting

PCN_Financial_Reporting

NPPCC_Financial_Reporting

Fiscal_Year_Reporting_Dates

UPCC_Primary_Care_Patient_Services

CHC_Primary_Care_Patient_Services

Practitioner_Role_Mapping

HR_Records

Provincial_Risk_Tracking

Decision_Log

HA_Hierarchy

UPPC_Budget

CHC_Budget

PCN_Budget

NPPCC_Budget

So all these tasks are downstream of Start_PCD_Extract_1.

Then, Status_Tracker >> Start_PCD_Extract_2.

So Start_PCD_Extract_2 is downstream of Status_Tracker.

Then, PCD_file_upload must be downstream of all extraction tasks and Start_PCD_Extract_2.

Wait, but Start_PCD_Extract_2 is an empty operator. So perhaps the upload task depends on all extraction tasks and Start_PCD_Extract_2. But how to set that up? Because the extraction tasks are in parallel, and Start_PCD_Extract_2 is downstream of Status_Tracker. So:

extraction_tasks = [Status_Tracker, Financial_Expense, ...]

Then, all extraction_tasks >> PCD_file_upload

But also, Start_PCD_Extract_2 >> PCD_file_upload.

But since Start_PCD_Extract_2 is downstream of Status_Tracker, which is one of the extraction tasks, then if PCD_file_upload depends on all extraction tasks, then Start_PCD_Extract_2 would already be a downstream task of Status_Tracker, but perhaps the upload needs to wait for Start_PCD_Extract_2 as well. Wait, no. Because Start_PCD_Extract_2 is triggered by Status_Tracker, but the upload depends on all extraction tasks. So if all extraction tasks are done, then Status_Tracker is done, which triggers Start_PCD_Extract_2. Then, the upload must wait for Start_PCD_Extract_2 as well. So the upload's dependencies are all extraction tasks plus Start_PCD_Extract_2.

But how to model that? Let's see:

After Start_PCD_Extract_1, all extraction tasks are set to run in parallel. Then, Status_Tracker triggers Start_PCD_Extract_2. Then, the upload task depends on all extraction tasks and Start_PCD_Extract_2.

Wait, but if the upload depends on all extraction tasks, then it's already waiting for Status_Tracker. But Start_PCD_Extract_2 is downstream of Status_Tracker. So the upload would need to wait for Start_PCD_Extract_2 as well. So perhaps the upload task is downstream of both the extraction tasks and Start_PCD_Extract_2.

But how to represent that? Let's say:

extraction_tasks = [task1, task2, ..., Status_Tracker, ...]

Then:

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since Status_Tracker is part of extraction_tasks, and Start_PCD_Extract_2 is downstream of Status_Tracker, then PCD_file_upload would depend on all extraction tasks and Start_PCD_Extract_2.

Alternatively, collect all extraction tasks and Start_PCD_Extract_2 into a list and set them as upstream for PCD_file_upload.

Wait, perhaps the correct approach is:

After all extraction tasks complete, and Start_PCD_Extract_2 completes, then PCD_file_upload can run.

So:

all_extraction_tasks = [Status_Tracker, Financial_Expense, ...]

all_extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But that would require PCD_file_upload to wait for both all_extraction_tasks and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is downstream of Status_Tracker (which is part of all_extraction_tasks), then once all_extraction_tasks are done, Start_PCD_Extract_2 would have already run. So perhaps PCD_file_upload only needs to depend on all_extraction_tasks. But according to the pipeline description, Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of the extraction tasks. So maybe the upload task depends on all extraction tasks and Start_PCD_Extract_2. Because Start_PCD_Extract_2 is part of the process.

But according to the pipeline steps:

PCD_file_upload is dependent on all previous extraction tasks. So perhaps Start_PCD_Extract_2 is not considered an extraction task. Wait, Start_PCD_Extract_2 is an empty operator serving as a synchronization point. So maybe the upload depends on all extraction tasks and Start_PCD_Extract_2.

But how to model that? Let's see:

The extraction tasks are all the ones listed, including Status_Tracker. Then, Start_PCD_Extract_2 is downstream of Status_Tracker. Then, the upload task must depend on all extraction tasks and Start_PCD_Extract_2.

But that would mean:

[all extraction tasks] >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since Start_PCD_Extract_2 is downstream of Status_Tracker (which is part of the extraction tasks), then when all extraction tasks are done, Status_Tracker is done, which triggers Start_PCD_Extract_2. Then, Start_PCD_Extract_2 must complete before PCD_file_upload can run. So the upload task depends on both all extraction tasks and Start_PCD_Extract_2.

But how to set that dependency? Because the extraction tasks are a list, and Start_PCD_Extract_2 is another task.

So perhaps:

PCD_file_upload is downstream of all extraction tasks and Start_PCD_Extract_2.

But how to express that in Airflow? Using the bitshift operator:

PCD_file_upload << [all_extraction_tasks] + [Start_PCD_Extract_2]

Alternatively, set each extraction task and Start_PCD_Extract_2 as upstream of PCD_file_upload.

But that's cumbersome. Maybe collect all extraction tasks into a list, then add Start_PCD_Extract_2 to that list.

Wait, perhaps:

extraction_tasks = [Status_Tracker, Financial_Expense, ...]

sync_tasks = extraction_tasks + [Start_PCD_Extract_2]

sync_tasks >> PCD_file_upload

But that would make PCD_file_upload depend on all extraction tasks and Start_PCD_Extract_2.

But Start_PCD_Extract_2 is downstream of Status_Tracker. So when Status_Tracker completes, Start_PCD_Extract_2 runs. Then, PCD_file_upload will wait for all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2.

But since Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of the extraction tasks, then when all extraction tasks complete, Start_PCD_Extract_2 may or may not have completed. Wait, no. Because Status_Tracker is part of the extraction tasks. So when all extraction tasks complete, Status_Tracker has completed, which triggers Start_PCD_Extract_2. So Start_PCD_Extract_2 would run after Status_Tracker completes. Therefore, PCD_file_upload must wait for Start_PCD_Extract_2 to complete as well.

So the correct dependencies would be:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> [all extraction tasks]

Status_Tracker >> Start_PCD_Extract_2

Then, PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2.

But how to model that? Because all extraction tasks are a list, and Start_PCD_Extract_2 is another task.

In Airflow, you can use the list of tasks and the >> operator. For example:

extraction_tasks = [task1, task2, ..., Status_Tracker, ...]

Start_PCD_Extract_1 >> extraction_tasks

Status_Tracker >> Start_PCD_Extract_2

Then, for PCD_file_upload:

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since extraction_tasks includes Status_Tracker, which triggers Start_PCD_Extract_2, then Start_PCD_Extract_2 will run after Status_Tracker completes. Then, PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2.

But when all extraction tasks complete, Start_PCD_Extract_2 may still be running. So PCD_file_upload must wait for Start_PCD_Extract_2 as well.

So combining all extraction tasks and Start_PCD_Extract_2 as upstream of PCD_file_upload.

But how to represent that? Maybe:

PCD_file_upload << extraction_tasks

PCD_file_upload << Start_PCD_Extract_2

Alternatively, create a list that includes all extraction tasks and Start_PCD_Extract_2, then set them as upstream.

But extraction_tasks is a list of tasks. So:

all_upload_dependencies = extraction_tasks + [Start_PCD_Extract_2]

all_upload_dependencies >> PCD_file_upload

Yes, that would work.

Now, the ETL_Notification task needs to run after all upstream tasks (including PCD_file_upload) complete, regardless of their status. So the trigger_rule is 'all_done'. The Python function for this task should check the status of the DAG run and send an email.

But how to get the DAG run status? The function can access the context, get the dag_run, and check its state. Then, send an email using Airflow's email backend.

The email recipients are managed via Airflow Variables. So in the code, we can use Variable.get to retrieve the email list.

Putting it all together:

Import necessary modules: DAG, PythonOperator, DummyOperator, KubernetesPodOperator (assuming that's the correct operator), etc.

Default_args should include the owner, start_date (a safe default like days_ago(1)), email (maybe from a Variable), email_on_failure and email_on_retry (probably False, since the notification task handles it), retries, etc.

The DAG is defined with schedule_interval from Variable.get('pcd_etl_schedule'), catchup=False, default_args, and dagrun_timeout of 60 minutes.

Now, code structure:

- Import statements.
- Default args.
- Define DAG.
- Define tasks:
   - Check_PCD_SFTP_Folder (KubernetesPodOperator)
   - Check_PCD_Shared_Folder (KubernetesPodOperator)
   - Start_PCD_Extract_1 (DummyOperator)
   - All extraction tasks (DummyOperator for each, since they are HTTP API extractions; maybe PythonOperator in real code, but the example uses Dummy)
   - Start_PCD_Extract_2 (DummyOperator)
   - PCD_file_upload (KubernetesPodOperator)
   - ETL_Notification (PythonOperator)

Wait, the extraction tasks are described as HTTP API extractions. So perhaps they should be SimpleHttpOperator or another HTTP operator. But the user didn't specify, so maybe use DummyOperator for simplicity, given that the code needs to be executable. Alternatively, use PythonOperator with pass.

But the user wants executable code. So using DummyOperator for the extraction tasks.

So, each extraction task is a DummyOperator.

Then, dependencies:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> extraction_tasks

Status_Tracker >> Start_PCD_Extract_2

Then, all extraction tasks and Start_PCD_Extract_2 >> PCD_file_upload

Then, PCD_file_upload >> ETL_Notification? No, because ETL_Notification should run after all upstream tasks, including PCD_file_upload. Wait, the description says ETL_Notification executes after all upstream tasks complete. So ETL_Notification's upstream is PCD_file_upload and any other tasks? Or is ETL_Notification downstream of PCD_file_upload and all extraction tasks?

Wait, the ETL_Notification task is supposed to execute after all upstream tasks complete, regardless of their success. So ETL_Notification's trigger_rule is 'all_done', and its upstream includes PCD_file_upload and any other tasks that are part of the pipeline. Wait, no. The pipeline steps say ETL_Notification executes after all upstream tasks complete. The upstream tasks are Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, all extraction tasks, Start_PCD_Extract_1 and 2, and PCD_file_upload. So ETL_Notification should be downstream of all these tasks. But how to model that?

Actually, the ETL_Notification task is the last task in the pipeline. So all previous tasks (Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1, all extraction tasks, Start_PCD_Extract_2, PCD_file_upload) must be upstream of ETL_Notification.

But since ETL_Notification uses trigger_rule 'all_done', it will run once all upstream tasks have completed, regardless of their state.

So the dependencies would be:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> extraction_tasks

Status_Tracker >> Start_PCD_Extract_2

extraction_tasks + [Start_PCD_Extract_2] >> PCD_file_upload

Then, PCD_file_upload >> ETL_Notification

But that would only set ETL_Notification downstream of PCD_file_upload. But according to the pipeline description, ETL_Notification should execute after all upstream tasks complete. So ETL_Notification should depend on all tasks in the DAG. But how?

Wait, perhaps ETL_Notification should be downstream of all tasks. But that's not feasible unless we collect all tasks and set them as upstream.

Alternatively, since ETL_Notification is the last task, perhaps it's downstream of PCD_file_upload and any other tasks that might run in parallel. But in this case, after PCD_file_upload completes, ETL_Notification runs. But according to the pipeline steps, ETL_Notification should execute after all upstream tasks complete. So perhaps the ETL_Notification task's upstream includes all tasks except itself.

But that's complicated. Alternatively, since the DAG's tasks are structured such that ETL_Notification is the final task, and all paths lead to it. For example:

All tasks except ETL_Notification are upstream of ETL_Notification.

But how to model that? Maybe:

all_tasks_except_notification = [Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1] + extraction_tasks + [Start_PCD_Extract_2, PCD_file_upload]

all_tasks_except_notification >> ETL_Notification

But that's possible. However, in Airflow, dependencies are set task by task. So perhaps the correct way is to have ETL_Notification depend on PCD_file_upload and any other tasks that are not already upstream of PCD_file_upload. But in this case, all tasks are upstream of PCD_file_upload except Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder. Wait, no:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> extraction_tasks

extraction_tasks and Start_PCD_Extract_2 >> PCD_file_upload

So PCD_file_upload's upstream includes Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1, extraction_tasks, and Start_PCD_Extract_2.

Therefore, ETL_Notification should be downstream of PCD_file_upload and any other tasks that are not upstream of PCD_file_upload. But in this case, all tasks are upstream of PCD_file_upload except Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder. Wait, no. Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder are upstream of Start_PCD_Extract_1, which is upstream of extraction_tasks. So all tasks are upstream of PCD_file_upload. Therefore, ETL_Notification can be downstream of PCD_file_upload, and since PCD_file_upload depends on all previous tasks, ETL_Notification will run after PCD_file_upload completes, which is after all upstream tasks. But according to the pipeline description, ETL_Notification should execute after all upstream tasks complete regardless of their state. So if PCD_file_upload fails, ETL_Notification should still run. But if ETL_Notification is downstream of PCD_file_upload, then if PCD_file_upload fails, ETL_Notification would still run because its trigger_rule is 'all_done'. Wait, no. The trigger_rule determines when the task runs based on the state of its direct upstream tasks. So if ETL_Notification's upstream is PCD_file_upload, then it will run when PCD_file_upload is done, regardless of its state. But other tasks (like Check_PCD_SFTP_Folder, etc.) are upstream of PCD_file_upload. So if any of those tasks fail, PCD_file_upload might be skipped or not run. Therefore, ETL_Notification's trigger_rule would depend on PCD_file_upload's state. But according to the pipeline description, ETL_Notification should run after all upstream tasks complete, not just PCD_file_upload.

Ah, this is a problem. Because ETL_Notification is supposed to run after all tasks in the DAG have completed, regardless of their success. So how to model that?

In Airflow, a task can be set to trigger when all of its upstream tasks are done, regardless of their state. So ETL_Notification should have all other tasks as upstream and trigger_rule='all_done'.

But how to set all other tasks as upstream of ETL_Notification?

In this DAG, the tasks are:

1. Check_PCD_SFTP_Folder

2. Check_PCD_Shared_Folder

3. Start_PCD_Extract_1

4. All extraction tasks (17 tasks)

5. Start_PCD_Extract_2

6. PCD_file_upload

7. ETL_Notification

So ETL_Notification needs to have upstream tasks: Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1, all extraction tasks, Start_PCD_Extract_2, and PCD_file_upload.

But how to set that up? Because Check_PCD_SFTP_Folder is upstream of Check_PCD_Shared_Folder, which is upstream of Start_PCD_Extract_1, etc. So ETL_Notification's upstream should include all these tasks.

But in Airflow, dependencies are transitive. So if ETL_Notification is downstream of PCD_file_upload, and PCD_file_upload is downstream of all other tasks, then ETL_Notification would effectively be downstream of all tasks. However, if any task fails before PCD_file_upload, PCD_file_upload might not run, and ETL_Notification would still need to run.

Wait, no. If Check_PCD_SFTP_Folder fails, then Check_PCD_Shared_Folder would not run, Start_PCD_Extract_1 would not run, etc. So PCD_file_upload would not run. Therefore, ETL_Notification would not be triggered unless it's downstream of all tasks.

But how to make ETL_Notification run even if some tasks fail. The solution is to set ETL_Notification as downstream of all tasks. But how to do that?

One approach is to use a DummyOperator as a final task that all tasks trigger, and then ETL_Notification depends on that. But that's complicated.

Alternatively, use the TriggerRule.ALL_DONE and set ETL_Notification's upstream to include all tasks. But how to collect all tasks into a list.

In code:

all_tasks = [
    Check_PCD_SFTP_Folder,
    Check_PCD_Shared_Folder,
    Start_PCD_Extract_1,
    Status_Tracker,
    Financial_Expense,
    UPCC_Financial_Reportingr,
    CHC_Financial_reporting,
    PCN_Financial_Reporting,
    NPPCC_Financial_Reporting,
    Fiscal_Year_Reporting_Dates,
    UPCC_Primary_Care_Patient_Services,
    CHC_Primary_Care_Patient_Services,
    Practitioner_Role_Mapping,
    HR_Records,
    Provincial_Risk_Tracking,
    Decision_Log,
    HA_Hierarchy,
    UPPC_Budget,
    CHC_Budget,
    PCN_Budget,
    NPPCC_Budget,
    Start_PCD_Extract_2,
    PCD_file_upload,
]

Then:

for task in all_tasks:
    task >> ETL_Notification

But that's not feasible because tasks have dependencies among themselves. For example, Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder. So adding Check_PCD_SFTP_Folder >> ETL_Notification would create a redundant dependency.

Alternatively, ETL_Notification should be downstream of all leaf nodes. But that's complicated.

Another approach is to use the fact that ETL_Notification is the last task and depends on PCD_file_upload and all other tasks that are not upstream of PCD_file_upload. But this may not capture all tasks.

Alternatively, use a list of all tasks except ETL_Notification and set them as upstream:

all_tasks_except_notification = [
    Check_PCD_SFTP_Folder,
    Check_PCD_Shared_Folder,
    Start_PCD_Extract_1,
    Status_Tracker,
    Financial_Expense,
    UPCC_Financial_Reportingr,
    CHC_Financial_reporting,
    PCN_Financial_Reporting,
    NPPCC_Financial_Reporting,
    Fiscal_Year_Reporting_Dates,
    UPCC_Primary_Care_Patient_Services,
    CHC_Primary_Care_Patient_Services,
    Practitioner_Role_Mapping,
    HR_Records,
    Provincial_Risk_Tracking,
    Decision_Log,
    HA_Hierarchy,
    UPPC_Budget,
    CHC_Budget,
    PCN_Budget,
    NPPCC_Budget,
    Start_PCD_Extract_2,
    PCD_file_upload,
]

all_tasks_except_notification >> ETL_Notification

But this would require explicitly listing all tasks, which is error-prone. However, given the pipeline steps, this is manageable.

But in code, this would be tedious. Let's see:

extraction_tasks = [
    Status_Tracker,
    Financial_Expense,
    UPCC_Financial_Reportingr,
    CHC_Financial_reporting,
    PCN_Financial_Reporting,
    NPPCC_Financial_Reporting,
    Fiscal_Year_Reporting_Dates,
    UPCC_Primary_Care_Patient_Services,
    CHC_Primary_Care_Patient_Services,
    Practitioner_Role_Mapping,
    HR_Records,
    Provincial_Risk_Tracking,
    Decision_Log,
    HA_Hierarchy,
    UPPC_Budget,
    CHC_Budget,
    PCN_Budget,
    NPPCC_Budget,
]

Then, all_tasks_except_notification = [Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1] + extraction_tasks + [Start_PCD_Extract_2, PCD_file_upload]

Then:

for task in all_tasks_except_notification:
    task >> ETL_Notification

But this would create dependencies like Check_PCD_SFTP_Folder >> ETL_Notification, which is redundant because Check_PCD_SFTP_Folder is upstream of Check_PCD_Shared_Folder, which is upstream of Start_PCD_Extract_1, etc. But Airflow allows multiple dependencies, and it's okay.

But this approach ensures that ETL_Notification is downstream of all tasks, regardless of their position in the DAG. So even if some tasks fail, ETL_Notification will run once all tasks have completed (success or failed).

But in Airflow, when you set task A >> task B, task B will wait for task A to complete. So if ETL_Notification is downstream of all tasks, it will run once all tasks have completed, regardless of their state. But with trigger_rule='all_done', it will run regardless of upstream states.

So combining:

ETL_Notification's trigger_rule is 'all_done', and its upstream includes all tasks except itself.

Thus, the code would have:

ETL_Notification = PythonOperator(
    task_id='ETL_Notification',
    python_callable=send_notification,
    trigger_rule='all_done',
    provide_context=True,
    dag=dag,
)

Then, set all tasks except ETL_Notification as upstream:

Check_PCD_SFTP_Folder >> ETL_Notification

Check_PCD_Shared_Folder >> ETL_Notification

Start_PCD_Extract_1 >> ETL_Notification

for task in extraction_tasks:
    task >> ETL_Notification

Start_PCD_Extract_2 >> ETL_Notification

PCD_file_upload >> ETL_Notification

But this is redundant because many tasks are already upstream of others. However, Airflow allows this and it ensures that ETL_Notification runs after all tasks.

But this approach may not be efficient, but it's correct.

Alternatively, since ETL_Notification is the last task and depends on PCD_file_upload, and PCD_file_upload depends on all previous tasks, then ETL_Notification would run after PCD_file_upload completes. But if PCD_file_upload is skipped or fails, ETL_Notification would still run if its trigger_rule is 'all_done' and its upstream is PCD_file_upload. However, this would not capture upstream tasks that failed before PCD_file_upload.

For example, if Check_PCD_SFTP_Folder fails, then Check_PCD_Shared_Folder and all subsequent tasks are skipped. So PCD_file_upload would be in the 'upstream_failed' state. Then, ETL_Notification with trigger_rule 'all_done' and upstream PCD_file_upload would run because PCD_file_upload is in a terminal state (upstream_failed). So in this case, ETL_Notification would run.

But if Check_PCD_SFTP_Folder fails, then Check_PCD_Shared_Folder is skipped, Start_PCD_Extract_1 is skipped, etc. So PCD_file_upload is skipped. Then, ETL_Notification's upstream is PCD_file_upload, which is skipped. With trigger_rule 'all_done', ETL_Notification would run because PCD_file_upload is in a terminal state (skipped). But does the trigger_rule 'all_done' consider skipped tasks as done? Yes, because 'all_done' means all upstream tasks are in a terminal state (success, failed, skipped, etc.).

So perhaps setting ETL_Notification's upstream to PCD_file_upload is sufficient, because PCD_file_upload's upstream includes all other tasks. Therefore, when PCD_file_upload is done (success, failed, or skipped), ETL_Notification runs.

But according to the pipeline description, ETL_Notification should analyze the entire DAG run's outcome, not just the PCD_file_upload task. So the notification should consider all tasks' states.

Therefore, the correct approach is to have ETL_Notification depend on all tasks, so that it runs after all tasks have completed, regardless of their state.

Thus, the code should set ETL_Notification's upstream to all tasks.

But how to implement that in code?

Perhaps collect all tasks into a list and set them as upstream.

In code:

all_tasks = [
    Check_PCD_SFTP_Folder,
    Check_PCD_Shared_Folder,
    Start_PCD_Extract_1,
    Status_Tracker,
    Financial_Expense,
    UPCC_Financial_Reportingr,
    CHC_Financial_reporting,
    PCN_Financial_Reporting,
    NPPCC_Financial_Reporting,
    Fiscal_Year_Reporting_Dates,
    UPCC_Primary_Care_Patient_Services,
    CHC_Primary_Care_Patient_Services,
    Practitioner_Role_Mapping,
    HR_Records,
    Provincial_Risk_Tracking,
    Decision_Log,
    HA_Hierarchy,
    UPPC_Budget,
    CHC_Budget,
    PCN_Budget,
    NPPCC_Budget,
    Start_PCD_Extract_2,
    PCD_file_upload,
]

for task in all_tasks:
    task >> ETL_Notification

This way, ETL_Notification depends on all tasks and will run once all are done.

Now, the Python function for ETL_Notification:

def send_notification(**context):
    dag_run = context['dag_run']
    success = all(ti.state == 'success' for ti in dag_run.get_task_instances())
    # Get email recipients from Airflow Variable
    recipients = Variable.get('pcd_etl_notification_emails', default_var='admin@example.com').split(',')
    subject = f"DAG {dag_run.dag_id} {'succeeded' if success else 'failed'}"
    body = f"The DAG run {dag_run.run_id} completed with overall status: {'success' if success else 'failure'}."
    # Send email using Airflow's email backend
    send_email(to=recipients, subject=subject, html_content=body)

But Airflow's send_email function is part of the email utility. So the function should import it:

from airflow.utils.email import send_email

But the user's code must include proper imports.

So in the code, the send_notification function uses send_email.

Putting it