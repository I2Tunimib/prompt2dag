<think>
Okay, I need to convert the given pipeline description into an Apache Airflow DAG. Let's start by understanding the requirements and constraints.

First, the DAG should use Airflow 2.x style imports. So I'll import DAG from airflow, and necessary operators like PythonOperator, KubernetesJobOperator, etc. Wait, KubernetesJobOperator isn't part of the core Airflow. Oh right, maybe the user is using a custom operator or the KubernetesPodOperator. But the problem mentions KubernetesJobOperator, so perhaps I should use that. However, in standard Airflow, KubernetesPodOperator is available. Maybe the user expects me to use KubernetesPodOperator, assuming that's what they meant. Alternatively, perhaps they have a custom operator. Since the exact operator isn't specified, I'll proceed with KubernetesPodOperator and note that in a comment, but maybe the user just wants to use DummyOperator for the example since the actual job specifics aren't provided. Hmm, the problem says "Kubernetes job operator", so perhaps the tasks Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder are using KubernetesPodOperator. But without specific details, I'll use DummyOperator as placeholders for those tasks, since the actual implementation would require more configuration (like pod specs, images, etc.) which aren't provided. Alternatively, maybe the user expects to see the use of KubernetesJobOperator, which might be a custom operator. But to keep the code executable, perhaps using DummyOperator is safer, with a comment indicating that KubernetesJobOperator should be used in real scenarios.

Next, the pipeline steps. The first two tasks are Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder, which are Kubernetes jobs. The second check depends on the first. Then Start_PCD_Extract_1 is an empty operator (like DummyOperator) that triggers parallel extraction tasks. All the extraction tasks run in parallel after Start_PCD_Extract_1. Then Start_PCD_Extract_2 is triggered by Status_Tracker. Wait, the description says Start_PCD_Extract_2 is triggered by the Status_Tracker task. So after Status_Tracker completes, Start_PCD_Extract_2 runs. Then the PCD_file_upload task depends on all previous extraction tasks, including those parallel ones and Start_PCD_Extract_2. Then ETL_Notification runs after all upstream tasks, regardless of success or failure, using the all_done trigger rule.

The DAG's schedule is determined by an Airflow Variable 'pcd_etl_schedule', which can be a cron expression or None. So I need to retrieve that variable in the DAG definition. Also, the DAG has a 60-minute timeout, catchup disabled, and default_args that include the timeout, email settings, etc.

The notification task uses a PythonOperator that checks the DAG run's status. To do that, it might need to access the task instances or the DagRun object. However, in Airflow, the context in a PythonOperator includes 'dag_run', so the function can check the status of the DAG run's tasks. Alternatively, the function could check all the upstream task instances to determine success or failure. But the problem says the notification should report the final status of the entire DAG run. So perhaps the function sends an email based on whether the DAG run as a whole succeeded or failed. However, in Airflow, the success of the DAG run depends on all tasks succeeding, unless some are skipped or have trigger rules. But the ETL_Notification task is set to trigger with all_done, so it runs regardless. The function would need to determine if the DAG run was successful. Maybe by checking if all tasks are in a success state, but that's complicated. Alternatively, perhaps the function can check the DagRun's state. Wait, the ETL_Notification task is part of the DAG, so when it runs, the DAG run is still ongoing. So maybe the function can't directly get the final state. Alternatively, the notification could check the status of all the upstream tasks and send an email accordingly. For example, if any task failed, send a failure email; else, success. That's probably the way to go.

Now, structuring the DAG:

- default_args should include owner, depends_on_past, email, retries, etc. The problem mentions a 60-minute timeout per DAG run, so the dagrun_timeout is set to 60*60. Also, catchup is disabled.

The steps:

1. Check_PCD_SFTP_Folder (KubernetesJobOperator) → Check_PCD_Shared_Folder (KubernetesJobOperator). The second depends on the first.

2. Check_PCD_Shared_Folder → Start_PCD_Extract_1 (DummyOperator).

3. Start_PCD_Extract_1 triggers all parallel extraction tasks. These are all DummyOperators for the example. The list includes Status_Tracker, Financial_Expense, etc. All of these are set to be downstream of Start_PCD_Extract_1.

4. Status_Tracker → Start_PCD_Extract_2 (DummyOperator). So after Status_Tracker completes, Start_PCD_Extract_2 runs.

5. All extraction tasks (including Status_Tracker) and Start_PCD_Extract_2 must complete before PCD_file_upload runs. So PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2.

Wait, the description says PCD_file_upload is dependent on all previous extraction tasks. So all the parallel tasks (including Status_Tracker and others) must complete before PCD_file_upload. But Start_PCD_Extract_2 is triggered by Status_Tracker. So Start_PCD_Extract_2 is downstream of Status_Tracker. Then, PCD_file_upload depends on Start_PCD_Extract_2 and all other extraction tasks. Wait, no. Because Start_PCD_Extract_2 is triggered by Status_Tracker, which is one of the extraction tasks. So the Start_PCD_Extract_2 is part of the extraction phase. Then, all extraction tasks (including those parallel ones and Start_PCD_Extract_2) must complete before PCD_file_upload. Wait, no. The Start_PCD_Extract_2 is a synchronization point triggered by Status_Tracker. But according to the description, the parallel extraction tasks are all triggered by Start_PCD_Extract_1. Then, Start_PCD_Extract_2 is triggered by Status_Tracker. So the flow is:

Start_PCD_Extract_1 → [all extraction tasks]

Among the extraction tasks, Status_Tracker → Start_PCD_Extract_2.

Then, PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2? Or just all extraction tasks and Start_PCD_Extract_2?

Wait, the description says: "PCD_file_upload: ... dependent on all previous extraction tasks." So all extraction tasks (the parallel ones) must complete. But Start_PCD_Extract_2 is part of the extraction phase? Or is it part of the next phase? The description says Start_PCD_Extract_2 is triggered by Status_Tracker task, which is one of the extraction tasks. So perhaps Start_PCD_Extract_2 is part of the extraction phase, and PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. But that may not be necessary. Let me re-read the pipeline steps:

- Start_PCD_Extract_2: An empty operator triggered by the Status_Tracker task, providing an additional synchronization point.

So after Status_Tracker completes, Start_PCD_Extract_2 runs. Then, PCD_file_upload depends on all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2? Or does Start_PCD_Extract_2's completion imply that Status_Tracker has completed, so PCD_file_upload just needs to depend on Start_PCD_Extract_2 and the other extraction tasks?

Wait, the extraction tasks are all triggered by Start_PCD_Extract_1. So Start_PCD_Extract_1 is a dummy task that all extraction tasks depend on. Then, each extraction task runs in parallel. Among them, Status_Tracker triggers Start_PCD_Extract_2. So the dependencies are:

Check_PCD_Shared_Folder → Start_PCD_Extract_1 → [all extraction tasks]

Status_Tracker → Start_PCD_Extract_2

Then, PCD_file_upload must wait for all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2. Wait, no. Because Start_PCD_Extract_2 is triggered by Status_Tracker, which is one of the extraction tasks. So Start_PCD_Extract_2 is downstream of Status_Tracker. Then, PCD_file_upload needs to depend on all extraction tasks and Start_PCD_Extract_2? Or just all extraction tasks? Because once all extraction tasks are done, Start_PCD_Extract_2 would have already run (since it's triggered by Status_Tracker). But perhaps Start_PCD_Extract_2 is part of the extraction phase, and PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. But according to the description, PCD_file_upload is dependent on all previous extraction tasks. So maybe Start_PCD_Extract_2 is part of the extraction phase, and thus PCD_file_upload depends on all extraction tasks (including Start_PCD_Extract_2). Wait, no. Start_PCD_Extract_2 is an empty operator triggered by Status_Tracker. So perhaps the extraction phase includes all the parallel tasks plus Start_PCD_Extract_2. But according to the steps, the parallel extraction tasks are all triggered by Start_PCD_Extract_1, and Start_PCD_Extract_2 is triggered by Status_Tracker. So the extraction phase includes the parallel tasks and Start_PCD_Extract_2. Therefore, PCD_file_upload must depend on all extraction tasks (the parallel ones) and Start_PCD_Extract_2. But how?

Wait, perhaps the extraction tasks are all the ones listed under "Parallel HTTP API Extraction Tasks", which includes Status_Tracker. Then, after Start_PCD_Extract_1 triggers all of them, each runs in parallel. Among them, Status_Tracker triggers Start_PCD_Extract_2. So the dependencies are:

Start_PCD_Extract_1 >> [Status_Tracker, Financial_Expense, ...]

Status_Tracker >> Start_PCD_Extract_2

Then, PCD_file_upload must depend on all extraction tasks (the list) and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is triggered by Status_Tracker, which is one of the extraction tasks, perhaps PCD_file_upload can depend on the completion of all extraction tasks and Start_PCD_Extract_2. But that's redundant because Start_PCD_Extract_2 is downstream of Status_Tracker. So if all extraction tasks are done, then Status_Tracker is done, which triggers Start_PCD_Extract_2. So PCD_file_upload needs to depend on Start_PCD_Extract_2 and all other extraction tasks except Status_Tracker? Or perhaps the extraction tasks include all the parallel ones, and Start_PCD_Extract_2 is part of the extraction phase. So PCD_file_upload depends on all extraction tasks (the parallel ones) and Start_PCD_Extract_2. But that would mean:

extraction_tasks = [Status_Tracker, Financial_Expense, ...]

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since Status_Tracker is in extraction_tasks, and Start_PCD_Extract_2 is downstream of Status_Tracker, then PCD_file_upload would depend on Start_PCD_Extract_2 and all extraction tasks. However, if extraction_tasks includes Status_Tracker, then when extraction_tasks are all done, Start_PCD_Extract_2 would have already run. So perhaps PCD_file_upload can depend on Start_PCD_Extract_2 and the other extraction tasks. But how to model that?

Alternatively, maybe the extraction phase consists of all the parallel tasks plus Start_PCD_Extract_2. So PCD_file_upload depends on all of them. But how to set that up.

This is a bit confusing. Let me re-express the steps:

After Check_PCD_Shared_Folder, Start_PCD_Extract_1 runs. Then, all extraction tasks (parallel) are triggered. Among them, Status_Tracker triggers Start_PCD_Extract_2. So the extraction phase includes all the parallel tasks and Start_PCD_Extract_2. Then, PCD_file_upload must wait for all extraction tasks (parallel ones) and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of the extraction tasks, perhaps PCD_file_upload can depend on the completion of all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2. But that would require:

extraction_tasks = [Status_Tracker, Financial_Expense, ...]

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since Status_Tracker is part of extraction_tasks, and Start_PCD_Extract_2 is downstream of Status_Tracker, then when extraction_tasks are done, Start_PCD_Extract_2 would have already run. Therefore, PCD_file_upload would depend on extraction_tasks and Start_PCD_Extract_2. But that's redundant because Start_PCD_Extract_2 is already downstream of Status_Tracker, which is part of extraction_tasks. So perhaps PCD_file_upload can depend on extraction_tasks and Start_PCD_Extract_2. But that would create a dependency where PCD_file_upload waits for all extraction_tasks and Start_PCD_Extract_2. However, Start_PCD_Extract_2 is downstream of Status_Tracker, so once all extraction_tasks are done, Start_PCD_Extract_2 is already done. Therefore, perhaps PCD_file_upload can depend on extraction_tasks and Start_PCD_Extract_2, but that's redundant. Alternatively, maybe the extraction_tasks list includes all the parallel tasks except Status_Tracker, and then Status_Tracker is handled separately. But according to the problem description, Status_Tracker is one of the parallel extraction tasks triggered by Start_PCD_Extract_1.

Alternatively, perhaps the extraction_tasks list includes all the parallel tasks, and Start_PCD_Extract_2 is a separate task. Then, PCD_file_upload depends on all extraction_tasks and Start_PCD_Extract_2. But how to set that up.

Let me outline the tasks:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> [all extraction tasks]

Status_Tracker >> Start_PCD_Extract_2

Then, PCD_file_upload must depend on all extraction tasks and Start_PCD_Extract_2. So:

extraction_tasks = [Status_Tracker, Financial_Expense, ...]

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since Status_Tracker is part of extraction_tasks, and Start_PCD_Extract_2 is downstream of Status_Tracker, then when extraction_tasks are all done, Start_PCD_Extract_2 would have already completed. Therefore, PCD_file_upload would depend on extraction_tasks and Start_PCD_Extract_2. But that's redundant. However, perhaps the correct way is to have PCD_file_upload depend on Start_PCD_Extract_2 and all other extraction tasks except Status_Tracker. Because Status_Tracker's completion triggers Start_PCD_Extract_2, and the other extraction tasks can proceed in parallel. Wait, no. All extraction tasks are in parallel. So PCD_file_upload needs to wait for all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is downstream of Status_Tracker, once Status_Tracker completes, Start_PCD_Extract_2 runs. So PCD_file_upload must wait for:

- All extraction tasks except Status_Tracker (since they are parallel)
- Status_Tracker (which triggers Start_PCD_Extract_2)
- Start_PCD_Extract_2 (which depends on Status_Tracker)

But how to model that. Because if PCD_file_upload depends on all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2, then when all extraction tasks are done, Start_PCD_Extract_2 is already done (since Status_Tracker is part of extraction tasks). Therefore, PCD_file_upload can just depend on all extraction tasks and Start_PCD_Extract_2. But that's redundant. Alternatively, perhaps PCD_file_upload depends on Start_PCD_Extract_2 and all other extraction tasks except Status_Tracker. Because Status_Tracker's completion is already captured by Start_PCD_Extract_2. But that's not correct because other extraction tasks may take longer than Status_Tracker. So PCD_file_upload needs to wait for all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2.

But how to code that. Let's see:

extraction_tasks = [task1, task2, ..., Status_Tracker, ...]

Then:

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But this would mean PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. However, since Start_PCD_Extract_2 is downstream of Status_Tracker (which is part of extraction_tasks), once all extraction_tasks are done, Start_PCD_Extract_2 is already done. Therefore, adding Start_PCD_Extract_2 as a dependency is redundant. But perhaps the problem description requires that PCD_file_upload depends on Start_PCD_Extract_2. So perhaps the correct way is to have:

all_extraction_tasks = extraction_tasks + [Start_PCD_Extract_2]

Then, all_extraction_tasks >> PCD_file_upload

But no, because Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of extraction_tasks. So when extraction_tasks are all done, Start_PCD_Extract_2 is already done. Therefore, PCD_file_upload can depend on extraction_tasks and Start_PCD_Extract_2. But perhaps the correct way is to have PCD_file_upload depend on Start_PCD_Extract_2 and all extraction tasks except Status_Tracker. Because Status_Tracker's completion is already captured by Start_PCD_Extract_2. But that's not correct because other extraction tasks may still be running. So PCD_file_upload must wait for all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is downstream of Status_Tracker, which is part of extraction_tasks, once extraction_tasks are done, Start_PCD_Extract_2 is done. Therefore, PCD_file_upload can depend on extraction_tasks and Start_PCD_Extract_2. But in code, how to represent that.

Alternatively, perhaps the extraction phase includes Start_PCD_Extract_2. So PCD_file_upload depends on all extraction tasks (parallel ones) and Start_PCD_Extract_2. But how to code that.

Let me proceed step by step.

First, define the tasks:

Check_PCD_SFTP_Folder = KubernetesPodOperator(...) → but use DummyOperator for example.

Check_PCD_Shared_Folder = DummyOperator(...)

Start_PCD_Extract_1 = DummyOperator(...)

Then, the parallel extraction tasks:

extraction_tasks = [
    'Status_Tracker',
    'Financial_Expense',
    'UPCC_Financial_Reportingr',
    'CHC_Financial_reporting',
    'PCN_Financial_Reporting',
    'NPPCC_Financial_Reporting',
    'Fiscal_Year_Reporting_Dates',
    'UPCC_Primary_Care_Patient_Services',
    'CHC_Primary_Care_Patient_Services',
    'Practitioner_Role_Mapping',
    'HR_Records',
    'Provincial_Risk_Tracking',
    'Decision_Log',
    'HA_Hierarchy',
    'UPPC_Budget',
    'CHC_Budget',
    'PCN_Budget',
    'NPPCC_Budget'
]

Each of these is a DummyOperator (or PythonOperator if they do something). For the example, use DummyOperator.

Then, Start_PCD_Extract_2 = DummyOperator(...)

Then, Status_Tracker >> Start_PCD_Extract_2

Then, PCD_file_upload = DummyOperator(...)

ETL_Notification = PythonOperator(..., trigger_rule='all_done')

Now, dependencies:

Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1

Start_PCD_Extract_1 >> extraction_tasks

Status_Tracker >> Start_PCD_Extract_2

Then, PCD_file_upload must depend on all extraction_tasks and Start_PCD_Extract_2. So:

extraction_tasks >> PCD_file_upload

Start_PCD_Extract_2 >> PCD_file_upload

But since Status_Tracker is part of extraction_tasks, and Start_PCD_Extract_2 is downstream of Status_Tracker, when extraction_tasks are all done, Start_PCD_Extract_2 is already done. Therefore, PCD_file_upload would depend on extraction_tasks and Start_PCD_Extract_2, which is redundant. But perhaps the correct way is to have PCD_file_upload depend on Start_PCD_Extract_2 and all other extraction tasks except Status_Tracker. But how to code that.

Alternatively, perhaps the extraction_tasks list includes all tasks except Status_Tracker, and then Status_Tracker is handled separately. But according to the problem description, Status_Tracker is one of the parallel extraction tasks.

This is getting complicated. Let's proceed with the code structure.

Now, the ETL_Notification task uses a Python function that sends an email based on the DAG run's status. The function would need to check the status of all tasks or the DAG run. However, in Airflow, the PythonOperator's context includes 'dag_run', and 'ti' (task instance). The function can iterate through all the task instances in the DagRun and check their statuses. For example:

def send_etl_notification(**context):
    dag_run = context['dag_run']
    failed_tasks = [ti for ti in dag_run.get_task_instances() if ti.state == 'failed']
    if failed_tasks:
        # send failure email
    else:
        # send success email

But this requires that the ETL_Notification task has access to all task instances. However, since ETL_Notification is set to trigger_rule='all_done', it will run after all upstream tasks are done, regardless of their state. The upstream tasks for ETL_Notification would be PCD_file_upload. Wait, no. According to the pipeline steps, ETL_Notification should execute after all upstream tasks complete (regardless of individual success/failure). So ETL_Notification's upstream is PCD_file_upload. But according to the description, ETL_Notification should run after all tasks, not just PCD_file_upload. So perhaps ETL_Notification is downstream of PCD_file_upload. But then, if any task before PCD_file_upload fails, PCD_file_upload may not run, and ETL_Notification would not trigger. But the trigger_rule is 'all_done', which means it runs when all upstream tasks are done (success or failed). So ETL_Notification's upstream is PCD_file_upload. But according to the description, ETL_Notification should run after all upstream tasks complete, which includes all tasks in the DAG. So perhaps ETL_Notification should be a downstream task of all other tasks. But how to model that.

Alternatively, ETL_Notification is set as a downstream task of PCD_file_upload, but with trigger_rule='all_done'. However, that would only consider PCD_file_upload's upstream tasks. Wait, no. The trigger_rule applies to the immediate upstream tasks of ETL_Notification. So if ETL_Notification is downstream of PCD_file_upload, then its trigger_rule would check the state of PCD_file_upload. But according to the problem statement, ETL_Notification should execute after all upstream tasks complete, regardless of their state. So perhaps ETL_Notification should be a downstream task of all tasks in the DAG. But that's not feasible. Alternatively, ETL_Notification can be set to have no upstream tasks but use a TriggerRule.ALL_DONE. But that's not possible. Alternatively, ETL_Notification should be downstream of all other tasks. So how to set that up.

In Airflow, you can set ETL_Notification to be downstream of all other tasks. But how to collect all tasks. One way is to list all tasks and set them as upstream to ETL_Notification. But that's tedious. Alternatively, use a list of all tasks and set them as upstream.

But perhaps the correct approach is to have ETL_Notification be downstream of PCD_file_upload and all other tasks. Wait, no. Because PCD_file_upload depends on extraction tasks and Start_PCD_Extract_2, which in turn depend on other tasks. So ETL_Notification should be downstream of PCD_file_upload. But then, if any task before PCD_file_upload fails, PCD_file_upload may not run, and ETL_Notification would not trigger. But the problem says ETL_Notification should execute after all upstream tasks complete, regardless of individual success/failure. So ETL_Notification's trigger_rule is 'all_done', but its upstream tasks are all tasks. So how to set that.

Alternatively, ETL_Notification can be set as a downstream task of all other tasks. For example:

all_tasks = [Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1, ...all extraction tasks, Start_PCD_Extract_2, PCD_file_upload]

all_tasks >> ETL_Notification

But that's not practical. Alternatively, use a dummy task that is downstream of all tasks, and ETL_Notification depends on that dummy task. But that's more complex.

Alternatively, since ETL_Notification is supposed to run after all tasks, regardless of their state, perhaps set it as a downstream task of all other tasks. But how to collect all tasks.

Alternatively, use the fact that ETL_Notification is the last task and set it to be downstream of PCD_file_upload. But then, if PCD_file_upload is skipped or failed, ETL_Notification would still run because of trigger_rule='all_done'. But other tasks that are upstream of PCD_file_upload may have failed, and ETL_Notification would not capture that unless it's downstream of all tasks.

This is a bit tricky. The problem statement says: "executes after all upstream tasks complete (regardless of individual success/failure)". So ETL_Notification's upstream tasks are all the tasks in the DAG. Therefore, ETL_Notification should be downstream of all tasks. But how to model that.

In Airflow, you can use the >> operator to set multiple upstream dependencies. For example:

task1 >> task2
task3 >> task2

But when there are many tasks, this becomes cumbersome. Alternatively, use a list:

all_tasks_except_notification = [Check_PCD_SFTP_Folder, Check_PCD_Shared_Folder, Start_PCD_Extract_1, ...]

all_tasks_except_notification >> ETL_Notification

But how to collect all tasks except ETL_Notification.

Alternatively, after defining all tasks, collect them into a list and set them as upstream to ETL_Notification.

But this requires careful coding.

Alternatively, since ETL_Notification is the last task, perhaps it's easier to set each task in the DAG to be upstream of ETL_Notification. But that's not feasible with many tasks.

Alternatively, use a dummy task that is downstream of all other tasks, and ETL_Notification depends on that dummy task. For example:

final_dummy = DummyOperator(task_id='final_dummy')

Check_PCD_SFTP_Folder >> final_dummy
Check_PCD_Shared_Folder >> final_dummy
Start_PCD_Extract_1 >> final_dummy
# and so on for all tasks...

final_dummy >> ETL_Notification

But this is impractical for many tasks.

Alternatively, use a bit of Python code to collect all the tasks and set them as upstream to ETL_Notification.

But in Airflow, tasks are added to the DAG as they are created, so perhaps after defining all tasks, collect them and set their downstream to ETL_Notification.

But how to do that in code.

Alternatively, use the chain function or other methods. But this may complicate the code.

Given the time constraints, perhaps the correct approach is to have ETL_Notification be downstream of PCD_file_upload and all extraction tasks and other tasks. But this may not capture all tasks.

Alternatively, perhaps the problem expects ETL_Notification to be downstream of PCD_file_upload, and since PCD_file_upload depends on all previous tasks, then ETL_Notification would run after PCD_file_upload completes, but not necessarily after all tasks. However, if PCD_file_upload's dependencies include all previous tasks, then ETL_Notification would run after PCD_file_upload, which in turn runs after all previous tasks. Therefore, ETL_Notification would effectively run after all tasks. But if any task fails before PCD_file_upload, PCD_file_upload may not run, and ETL_Notification would not trigger. But with trigger_rule='all_done', ETL_Notification would run even if upstream tasks (PCD_file_upload) are failed or skipped. Wait, no. The trigger_rule applies to the immediate upstream tasks of ETL_Notification. If ETL_Notification's upstream is PCD_file_upload, then it will run when PCD_file_upload is done (success, failed, etc.). But if PCD_file_upload is skipped because its upstream tasks failed, then ETL_Notification would not run. Therefore, this approach is incorrect.

The correct approach is to have ETL_Notification be downstream of all tasks in the DAG. So how to model that.

One way is to create a list of all tasks except ETL_Notification and set them as upstream to ETL_Notification.

But in code:

# After defining all tasks:

all_tasks = [
    check_sftp,
    check_shared,
    start_extract_1,
    status_tracker,
    financial_expense,
    # ... all other tasks ...
    start_extract_2,
    pcd_upload
]

for task in all_tasks:
    task >> etl_notification

But this requires collecting all tasks into a list. However, in the code, each task is assigned to a variable, which can be tedious.

Alternatively, use the DAG's tasks, but that's not straightforward in the code.

Given the time constraints, perhaps the problem expects ETL_Notification to be downstream of PCD_file_upload, and since PCD_file_upload depends on all previous tasks, then ETL_Notification would run after PCD_file_upload. But this is not correct because ETL_Notification should run regardless of whether PCD_file_upload runs or not.

Alternatively, set ETL_Notification's upstream to be all tasks. But how.

Perhaps the correct way is to make ETL_Notification a downstream task of all tasks except itself. For example:

etl_notification = PythonOperator(..., trigger_rule='all_done', task_id='ETL_Notification')

Then, after defining all tasks:

check_sftp >> etl_notification
check_shared >> etl_notification
start_extract_1 >> etl_notification
status_tracker >> etl_notification
# and so on for all tasks...

But this is impractical. Alternatively, use a list comprehension or loop.

But given the code structure, perhaps the problem expects ETL_Notification to be downstream of PCD_file_upload, and the trigger_rule='all_done' ensures it runs even if PCD_file_upload fails. But that would miss other tasks that may have failed before PCD_file_upload.

This is a problem. The correct approach is to have ETL_Notification depend on all tasks in the DAG. But how to implement that.

Alternatively, use a dummy task that is downstream of all tasks, and ETL_Notification depends on that dummy task with trigger_rule='all_done'.

For example:

all_tasks = [check_sftp, check_shared, start_extract_1, ..., pcd_upload]

dummy_final = DummyOperator(task_id='dummy_final')

for task in all_tasks:
    task >> dummy_final

dummy_final >> etl_notification

But then, etl_notification's trigger_rule would be 'all_success' by default, but since dummy_final is downstream of all tasks, etl_notification would run when dummy_final is done. But dummy_final's trigger_rule is 'all_success', so if any task fails, dummy_final is not triggered. Therefore, this approach doesn't work.

Alternatively, set dummy_final's trigger_rule to 'all_done', but DummyOperator doesn't have a trigger_rule. Wait, trigger_rule is a parameter of BaseOperator. So:

dummy_final = DummyOperator(task_id='dummy_final', trigger_rule='all_done')

But then, dummy_final would run when all upstream tasks (all_tasks) are done, regardless of their state. Then, dummy_final >> etl_notification, and etl_notification's trigger_rule is 'all_done' (though default is 'all_success'). Wait, etl_notification's trigger_rule should be 'all_done' to run regardless of dummy_final's state. But dummy_final is a DummyOperator, which would be in success state if all upstream tasks are done. Wait, no. The trigger_rule determines whether the task runs, not the state. So dummy_final's trigger_rule='all_done' ensures it runs when all upstream tasks are done, regardless of their state. Then, dummy_final's state would be success if it runs. Then, etl_notification, which is downstream of dummy_final, would run when dummy_final is done (success), which is always the case. Therefore, etl_notification can have trigger_rule='all_success' (default), but since dummy_final is always successful, etl_notification would run. But this way, etl_notification runs after all tasks are done, regardless of their state.

This seems like a viable approach.

So steps:

1. Create a dummy_final task with trigger_rule='all_done' and upstream as all tasks.

2. dummy_final >> etl_notification

But how to set upstream for dummy_final.

In code:

all_tasks = [check_sftp, check_shared, start_extract_1, status_tracker, ..., pcd_upload]

dummy_final = DummyOperator(
    task_id='dummy_final',
    trigger_rule='all_done',
    dag=dag
)

for task in all_tasks:
    task >> dummy_final

dummy_final >> etl_notification

Then, etl_notification's trigger_rule is 'all_success' (default), but since dummy_final is always successful (as it runs when all upstream tasks are done), etl_notification will run.

But etl_notification's function needs to check the status of all tasks, not just dummy_final.

This approach ensures that etl_notification runs after all tasks are done, regardless of their state.

But this adds an extra task (dummy_final), which may not be necessary. However, given the problem's constraints, this seems like a reasonable solution.

Alternatively, the problem states that the notification task uses the "all_done" trigger rule to ensure it executes after all upstream tasks have reached a terminal state. So perhaps the notification task