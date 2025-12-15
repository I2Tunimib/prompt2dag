<think>
Okay, I need to convert this pipeline description into a Prefect 2.x flow. Let's start by understanding the steps and dependencies.

First, the pipeline has two initial Kubernetes job tasks: Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder. The latter depends on the former. So those will be sequential tasks. Then there's Start_PCD_Extract_1, which is an empty synchronization point. After that, a bunch of parallel tasks are triggered by Start_PCD_Extract_1. All these tasks should be submitted concurrently using .submit() in Prefect. 

Wait, but the description says the parallel extraction tasks are all triggered by Start_PCD_Extract_1. So Start_PCD_Extract_1 is a task that runs first, and then all the extraction tasks start. Since it's an empty operator, maybe it's just a dummy task. In Prefect, I can represent that with a @task that does nothing, maybe just returns a signal. Then, all the parallel tasks depend on this task completing.

Then, there's Start_PCD_Extract_2, which is triggered by Status_Tracker. So among the parallel tasks, Status_Tracker is one, and once it's done, Start_PCD_Extract_2 runs. Then, PCD_file_upload depends on all previous extraction tasks. Wait, the description says PCD_file_upload is dependent on all previous extraction tasks. So after all the parallel tasks (including those after Start_PCD_Extract_2?), but how? Let me check the steps again.

The steps are:

- Check_PCD_SFTP_Folder
- Check_PCD_Shared_Folder (depends on SFTP check)
- Start_PCD_Extract_1 (marks beginning of parallel extraction)
- Parallel HTTP API tasks triggered by Start_PCD_Extract_1
- Start_PCD_Extract_2 triggered by Status_Tracker
- PCD_file_upload depends on all previous extraction tasks
- ETL_Notification runs after all upstream tasks, regardless of success

Wait, the PCD_file_upload is dependent on all previous extraction tasks. So all the parallel tasks (including those after Start_PCD_Extract_1 and Start_PCD_Extract_2?) need to complete before PCD_file_upload runs. But Start_PCD_Extract_2 is triggered by Status_Tracker. So perhaps the PCD_file_upload depends on all the extraction tasks and Start_PCD_Extract_2?

Hmm, maybe the extraction tasks are all the ones listed under the parallel section. So all those tasks (Status_Tracker, Financial_Expense, etc.) are submitted in parallel after Start_PCD_Extract_1. Then, Start_PCD_Extract_2 runs after Status_Tracker completes. But how does that affect the dependencies for PCD_file_upload? The description says PCD_file_upload is dependent on all previous extraction tasks. So regardless of Start_PCD_Extract_2, all the extraction tasks must complete before PCD_file_upload can run.

So in Prefect, the flow would be:

Check_SFTP → Check_Shared → Start1 → [parallel tasks] → then Start2 (depends on Status_Tracker) → but PCD_file_upload depends on all parallel tasks and Start2? Or just all parallel tasks?

Wait, the description says PCD_file_upload is dependent on all previous extraction tasks. The extraction tasks are the parallel ones. So PCD_file_upload must wait for all those parallel tasks to complete. Start_PCD_Extract_2 is triggered by Status_Tracker, but perhaps it's part of the extraction phase. So maybe Start_PCD_Extract_2 is another synchronization point, but the main upload depends on all extraction tasks, including those that may have been triggered after Start_PCD_Extract_2? Or maybe the extraction phase includes all the parallel tasks plus Start_PCD_Extract_2, but the upload depends on all extraction tasks.

This is a bit unclear. Let me re-read the pipeline steps:

- Start_PCD_Extract_1 is the beginning of parallel extraction.

- Parallel HTTP API tasks are all triggered by Start_PCD_Extract_1.

- Start_PCD_Extract_2 is triggered by Status_Tracker task (which is one of the parallel tasks). So after Status_Tracker completes, Start_PCD_Extract_2 runs. But the other parallel tasks may still be running.

Then, PCD_file_upload is dependent on all previous extraction tasks. So all the parallel tasks (including Status_Tracker) and Start_PCD_Extract_2? Or just the parallel tasks?

Wait, the extraction tasks are the ones listed under the parallel section. So Start_PCD_Extract_2 is not an extraction task but a synchronization point. So PCD_file_upload depends on all extraction tasks (the parallel ones) and Start_PCD_Extract_2? Or perhaps the Start_PCD_Extract_2 is part of the extraction phase but is a sequential step after Status_Tracker.

But according to the description, the upload is dependent on all previous extraction tasks. So all the parallel tasks (the HTTP API extraction ones) must complete before the upload can proceed. The Start_PCD_Extract_2 is triggered by Status_Tracker, but perhaps it's a task that runs after Status_Tracker, but the upload depends on all extraction tasks (the parallel ones) and Start_PCD_Extract_2? Or maybe Start_PCD_Extract_2 is part of the extraction phase, but the upload depends on all extraction tasks (parallel ones) and Start_PCD_Extract_2.

This is a bit confusing. Let me try to model the dependencies:

Check_SFTP → Check_Shared → Start1 → [parallel tasks]

Among the parallel tasks, Status_Tracker is one. When Status_Tracker completes, Start_PCD_Extract_2 runs. Then, PCD_file_upload depends on all parallel tasks (including Status_Tracker) and Start_PCD_Extract_2? Or does Start_PCD_Extract_2 not affect the upload's dependencies?

The description says PCD_file_upload is dependent on all previous extraction tasks. The extraction tasks are the parallel ones. So regardless of Start_PCD_Extract_2, the upload depends on all the parallel extraction tasks. So in Prefect, the upload task would have to wait for all the parallel extraction tasks to complete. Then, the notification task runs after all upstream tasks (including upload) complete.

Wait, the ETL_Notification task executes after all upstream tasks complete, regardless of success. So the notification depends on the upload task and all extraction tasks.

Wait, the pipeline steps:

After the parallel extraction tasks, there's Start_PCD_Extract_2 (triggered by Status_Tracker), then PCD_file_upload (depends on all extraction tasks), then ETL_Notification.

So the flow is:

Check_SFTP → Check_Shared → Start1 → [parallel extraction tasks]

Start_PCD_Extract_2 is triggered by Status_Tracker (one of the parallel tasks). Then, PCD_file_upload depends on all extraction tasks (parallel ones) and Start_PCD_Extract_2? Or just the extraction tasks?

The description says PCD_file_upload is dependent on all previous extraction tasks. So regardless of Start_PCD_Extract_2, the upload depends on all the extraction tasks (the parallel ones). So Start_PCD_Extract_2 may be a synchronization point but not a dependency for the upload. Or perhaps the upload depends on Start_PCD_Extract_2 as well.

But according to the pipeline steps, Start_PCD_Extract_2 is triggered by Status_Tracker. So Start_PCD_Extract_2 runs after Status_Tracker completes. Then, the upload depends on Start_PCD_Extract_2? Or is Start_PCD_Extract_2 part of the extraction phase?

This is unclear. Let me look again at the pipeline steps:

The steps are:

1. Check_PCD_SFTP_Folder

2. Check_PCD_Shared_Folder (depends on 1)

3. Start_PCD_Extract_1 (depends on 2?)

4. Parallel HTTP API Extraction Tasks (all triggered by 3)

5. Start_PCD_Extract_2 (triggered by Status_Tracker task, which is one of the parallel tasks)

6. PCD_file_upload (depends on all previous extraction tasks)

So the upload depends on all extraction tasks (the parallel ones) and Start_PCD_Extract_2? Or just the extraction tasks?

The description says "dependent on all previous extraction tasks". The extraction tasks are the parallel ones. So perhaps Start_PCD_Extract_2 is not considered an extraction task. Therefore, the upload depends on all the parallel extraction tasks (the HTTP API ones) and Start_PCD_Extract_2? Or maybe Start_PCD_Extract_2 is part of the extraction phase, but the upload depends on all extraction tasks (parallel ones) and Start_PCD_Extract_2.

Alternatively, maybe Start_PCD_Extract_2 is a synchronization point but doesn't contribute to the upload's dependencies. So the upload depends on all the parallel extraction tasks, regardless of Start_PCD_Extract_2.

But according to the pipeline steps, the upload is dependent on all previous extraction tasks, which would include the parallel ones. So in Prefect, the upload task would have to wait for all the parallel extraction tasks to complete. Then, the notification task runs after the upload.

But how to model the dependencies in Prefect? Let's outline the steps:

Check_SFTP → Check_Shared → Start1 → [parallel tasks]

Start_PCD_Extract_2 is triggered by Status_Tracker (so after Status_Tracker completes). Then, the upload depends on all parallel tasks and Start_PCD_Extract_2? Or just the parallel tasks?

Wait, perhaps the upload depends on all parallel tasks (including Status_Tracker) and Start_PCD_Extract_2. Because Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of the parallel tasks. So once Status_Tracker completes, Start_PCD_Extract_2 runs, and then the upload can start only after all parallel tasks and Start_PCD_Extract_2 are done.

But according to the description, the upload is dependent on all previous extraction tasks. The extraction tasks are the parallel ones. So Start_PCD_Extract_2 is not an extraction task. Therefore, the upload depends on all extraction tasks (parallel ones), and Start_PCD_Extract_2 is a separate task that may not be part of the extraction phase.

Hmm. Maybe the Start_PCD_Extract_2 is part of the extraction phase, but the upload depends on all extraction tasks (parallel ones) and Start_PCD_Extract_2. But how?

Alternatively, perhaps the Start_PCD_Extract_2 is just a synchronization point that doesn't do anything, but the upload depends on all extraction tasks (parallel ones) regardless of Start_PCD_Extract_2.

This is a bit ambiguous. To model this correctly, perhaps the upload depends on all the parallel extraction tasks (the HTTP API ones) and Start_PCD_Extract_2. Because Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of the parallel tasks. So once all parallel tasks are done, including Status_Tracker, then Start_PCD_Extract_2 would have already run (since it's triggered by Status_Tracker), and the upload can proceed.

Wait, no. If Start_PCD_Extract_2 is triggered by Status_Tracker, then Start_PCD_Extract_2 runs after Status_Tracker completes. But other parallel tasks may still be running. So the upload would have to wait for all parallel tasks (including Status_Tracker) and Start_PCD_Extract_2. But how?

Alternatively, maybe the upload depends on all parallel tasks and Start_PCD_Extract_2. But that would require that Start_PCD_Extract_2 is a dependency for the upload. But according to the description, the upload is dependent on all previous extraction tasks. So if Start_PCD_Extract_2 is not an extraction task, then it's not a dependency.

This is confusing. Let's look again at the pipeline steps:

The steps after the parallel extraction tasks are:

- Start_PCD_Extract_2: triggered by Status_Tracker task.

- PCD_file_upload: dependent on all previous extraction tasks.

So the upload depends on all extraction tasks (parallel ones), and Start_PCD_Extract_2 is triggered by Status_Tracker but perhaps is not part of the extraction phase. Therefore, the upload doesn't depend on Start_PCD_Extract_2. But then, when does the upload run? After all extraction tasks (parallel ones) complete, regardless of Start_PCD_Extract_2.

But Start_PCD_Extract_2 is triggered by Status_Tracker, which is part of the extraction tasks. So once Status_Tracker completes, Start_PCD_Extract_2 runs. But other extraction tasks may still be running. So the upload would have to wait for all extraction tasks (including Status_Tracker) and Start_PCD_Extract_2.

Wait, but the upload is dependent on all extraction tasks. So even if Start_PCD_Extract_2 is triggered by Status_Tracker, the upload doesn't need to wait for Start_PCD_Extract_2. Because Start_PCD_Extract_2 is not an extraction task.

So perhaps the upload depends on all extraction tasks (parallel ones), and Start_PCD_Extract_2 is a separate task that runs after Status_Tracker but doesn't affect the upload's dependencies.

In that case, the upload would run once all extraction tasks are done, and Start_PCD_Extract_2 would run when Status_Tracker is done, but other extraction tasks may still be running.

But how to model this in Prefect? Let's outline the dependencies:

Check_SFTP → Check_Shared → Start1 → [parallel extraction tasks]

Start_PCD_Extract_2 is triggered by Status_Tracker (so after Status_Tracker completes). But other extraction tasks may still be running.

Then, the upload depends on all extraction tasks (parallel ones). So in Prefect, the upload task would have to wait for all the parallel extraction tasks to finish. Start_PCD_Extract_2 is a separate task that runs after Status_Tracker, but the upload doesn't depend on it.

But according to the pipeline steps, Start_PCD_Extract_2 is triggered by Status_Tracker, but the upload is dependent on all extraction tasks, not on Start_PCD_Extract_2. So Start_PCD_Extract_2 is just a synchronization point that doesn't affect the upload's dependencies.

Therefore, in Prefect, the flow would be:

Check_SFTP → Check_Shared → Start1 → [parallel extraction tasks]

Start_PCD_Extract_2 runs after Status_Tracker (one of the parallel tasks) completes.

Then, the upload depends on all parallel extraction tasks (so all of them must complete before upload starts). The Start_PCD_Extract_2 may run before or after other extraction tasks, but the upload doesn't depend on it.

But how to model that in Prefect? Because in Prefect, tasks are executed based on their dependencies. So the parallel extraction tasks are submitted when Start1 completes. Then, Start_PCD_Extract_2 is triggered by Status_Tracker. The upload depends on all extraction tasks (parallel ones), so it waits for all of them to complete. Start_PCD_Extract_2 is a separate task that runs after Status_Tracker, but the upload doesn't depend on it.

So the upload's dependencies are all the parallel extraction tasks. Start_PCD_Extract_2 is a separate task that runs after Status_Tracker but doesn't block the upload.

But according to the pipeline steps, the upload is dependent on all previous extraction tasks, which includes Start_PCD_Extract_2? Or is Start_PCD_Extract_2 not considered an extraction task?

The pipeline steps list Start_PCD_Extract_1 and Start_PCD_Extract_2 as empty operators (synchronization points). So they are not extraction tasks. The extraction tasks are the parallel ones listed under the HTTP API Extraction Tasks.

Therefore, the upload depends on all extraction tasks (parallel ones), not on Start_PCD_Extract_2. So in Prefect, the upload task would be called after all the parallel extraction tasks have completed. Start_PCD_Extract_2 is a task that runs after Status_Tracker, but it's not a dependency for the upload.

But then, how does Start_PCD_Extract_2 fit into the flow? It's triggered by Status_Tracker, but it's not a dependency for any other task except maybe the notification? No, the notification depends on all upstream tasks, including Start_PCD_Extract_2 and the upload.

Wait, the ETL_Notification task executes after all upstream tasks complete. So all tasks in the flow must complete (regardless of success) before the notification runs. So the notification depends on the upload task, which in turn depends on all extraction tasks, and Start_PCD_Extract_2 depends on Status_Tracker.

So the flow would look like:

Check_SFTP → Check_Shared → Start1 → [parallel extraction tasks]

Start_PCD_Extract_2 runs after Status_Tracker (one of the parallel tasks)

Upload depends on all parallel extraction tasks → runs after all extraction tasks complete

Notification depends on upload and Start_PCD_Extract_2? Or all upstream tasks?

The description says the notification task executes after all upstream tasks complete. So in Prefect, the notification task would have to wait for all tasks in the flow to reach a terminal state. But Prefect's default is to have dependencies based on the flow's structure. So perhaps the notification task is placed at the end, and depends on the upload task and Start_PCD_Extract_2. But how?

Alternatively, since Prefect 2.x uses a dynamic DAG structure, the notification task can be set to run after all previous tasks. But how to model that.

Alternatively, the notification task is the last step and uses wait_for=[all_other_tasks]. But Prefect doesn't have a built-in way to collect all previous tasks automatically. So perhaps the notification task is called after the upload, and also after Start_PCD_Extract_2. But how to collect all the tasks.

This is getting complicated. Let's outline the tasks and their dependencies:

Tasks:

1. check_sftp (Kubernetes job)
2. check_shared (Kubernetes job, depends on check_sftp)
3. start_extract_1 (empty task, depends on check_shared)
4. All parallel extraction tasks (depend on start_extract_1)
   a. status_tracker
   b. financial_expense
   c. ... (others)
5. start_extract_2 (empty task, depends on status_tracker)
6. pcd_file_upload (depends on all extraction tasks)
7. etl_notification (depends on pcd_file_upload and start_extract_2?)

But according to the pipeline steps, the notification task executes after all upstream tasks complete. So it depends on all tasks: check_sftp, check_shared, start_extract_1, all extraction tasks, start_extract_2, pcd_file_upload.

But in Prefect, how to model that. Because the notification task would need to wait for all those tasks to complete. However, Prefect's tasks are executed based on their dependencies. So if the notification task is called after pcd_file_upload, but how to ensure it also waits for start_extract_2 and other tasks.

Alternatively, perhaps the notification task is the last step and uses a wait_for parameter that includes all tasks. But in Prefect, you can't dynamically collect all tasks. So perhaps the notification task is called at the end, and depends on the upload task and start_extract_2. But that's not all upstream tasks.

Alternatively, the notification task is the last step and is called after the upload task, but since Prefect's flow is imperative, any task that is called after others will run after them. But if there are tasks that are not in the direct dependency chain, like start_extract_2, which depends on status_tracker, but the notification task is called after the upload, which depends on all extraction tasks, then start_extract_2 may have already completed by then.

Wait, start_extract_2 depends on status_tracker. Once status_tracker completes, start_extract_2 runs. The upload depends on all extraction tasks (including status_tracker). So once all extraction tasks complete, the upload runs. So start_extract_2 would have already completed before the upload starts, because status_tracker (an extraction task) completes, triggering start_extract_2, which runs, and then when all extraction tasks complete, the upload runs.

So the notification task, which runs after the upload, would have all previous tasks completed by then. Therefore, in Prefect, the notification task can be called after the upload, and since all other tasks (including start_extract_2) would have completed before the upload starts, the notification task would indeed run after all upstream tasks.

But how to ensure that? Because in Prefect, tasks are executed as they are called, but dependencies are enforced through parameters and .submit().

Alternatively, perhaps the notification task should explicitly wait for all tasks. But that's not straightforward.

Another approach: Since the notification task is supposed to run after all tasks, regardless of their success, perhaps in Prefect, we can use a final task that is decorated with @flow, but wait, no. Wait, Prefect 2.x flows are functions, and tasks are called within them. The flow itself will complete when all tasks are done. So the notification task can be the last task called, and it will run after all previous tasks, but how to ensure it runs after everything.

Alternatively, the notification task can be called at the end of the flow function, after all other tasks. Since Prefect runs tasks in the order they are called, unless there are dependencies. So if the notification task is called after the upload, but other tasks like start_extract_2 may have been called earlier but are not dependencies of the upload, Prefect will manage their execution.

Wait, Prefect tasks are run asynchronously when using .submit(), but the flow function is synchronous. So when you call a task with .submit(), it starts the task and returns a future. The flow will wait for all submitted tasks to complete before proceeding, unless you use wait_for.

Hmm, perhaps I'm overcomplicating. Let's outline the code structure.

First, define all the tasks as @task-decorated functions. Then, in the @flow function:

def pcd_etl_flow():
    sftp_result = check_sftp_folder()
    shared_result = check_shared_folder(sftp_result)
    start1 = start_pcd_extract_1(shared_result)
    
    # Parallel extraction tasks
    extraction_tasks = [
        status_tracker.submit(start1),
        financial_expense.submit(start1),
        # ... all other parallel tasks submitted with start1 as upstream
    ]
    
    # Start_PCD_Extract_2 depends on status_tracker
    status_tracker_future = extraction_tasks[0]  # assuming status_tracker is first in list
    start2 = start_pcd_extract_2(status_tracker_future)
    
    # Wait for all extraction tasks to complete before upload
    # In Prefect, to wait for all, we can gather their results
    for task_future in extraction_tasks:
        task_future.result()
    
    upload_result = pcd_file_upload()
    
    # Notification runs after upload and all other tasks
    etl_notification(upload_result)

But this approach may not capture all dependencies correctly. Because the upload depends on all extraction tasks, which are submitted in parallel. So in Prefect, to ensure that the upload runs after all extraction tasks, we need to wait for all of them to complete. One way is to call .result() on each future, which blocks until they complete.

Alternatively, in Prefect 2.x, you can use a task that depends on all the extraction tasks. For example, the upload task could take all the extraction task results as parameters. But since they are submitted, you can pass the futures as parameters.

Wait, in Prefect, when you submit tasks, you get futures. If you pass a future as a parameter to another task, Prefect will automatically wait for that future to complete before running the dependent task. So if the upload task requires all extraction task results, you can pass them as parameters. But if the upload doesn't actually use the results, you can still pass them as dependencies.

But the upload is a Kubernetes job operator, which may not take input from the extraction tasks. So perhaps the upload task should depend on all extraction tasks. In Prefect, this can be done by passing all extraction task futures as parameters to the upload task.

But how to collect all the extraction task futures. For example:

extraction_tasks = [status_tracker.submit(start1), financial_expense.submit(start1), ...]

Then, upload_task = pcd_file_upload.submit(*extraction_tasks)

But the upload task doesn't actually use the results of the extraction tasks, but this would enforce that all extraction tasks must complete before the upload runs.

Yes, that's a way to model dependencies in Prefect. Even if the upload task doesn't use the parameters, passing the futures as parameters ensures that the upload task waits for them.

So in code:

extraction_tasks = [
    status_tracker.submit(start1),
    financial_expense.submit(start1),
    # ... all others
]

start2 = start_pcd_extract_2.submit(extraction_tasks[0])  # depends on status_tracker

upload = pcd_file_upload.submit(*extraction_tasks, start2)

Wait, but according to the pipeline steps, the upload depends on all previous extraction tasks (the parallel ones), not on start2. So start2 is triggered by status_tracker, but upload doesn't depend on start2. So perhaps upload should depend only on the extraction tasks.

But according to the pipeline steps, Start_PCD_Extract_2 is triggered by Status_Tracker, but the upload depends on all extraction tasks. So upload should depend on extraction tasks, not on start2.

So:

upload = pcd_file_upload.submit(*extraction_tasks)

Then, start2 is submitted after status_tracker completes, but upload doesn't depend on start2.

But then, the notification task needs to run after all tasks, including start2 and upload.

So notification should depend on upload and start2.

But how to model that. Since start2 is submitted after extraction_tasks[0], and upload is submitted after all extraction_tasks, the notification can be called after upload and start2.

But in Prefect, the flow is imperative. So:

start2 = start_pcd_extract_2.submit(extraction_tasks[0])

upload = pcd_file_upload.submit(*extraction_tasks)

notification = etl_notification.submit(upload, start2)

But this would make the notification depend on upload and start2. But according to the pipeline description, the notification should execute after all upstream tasks complete, regardless of their success. So the notification should depend on all tasks in the flow.

But how to collect all tasks. That's tricky. Because the flow has multiple tasks: check_sftp, check_shared, start1, all extraction tasks, start2, upload.

The notification task needs to wait for all of them. But in Prefect, you can't dynamically collect all tasks. So perhaps the notification task is called at the end, and depends on upload and start2. But that's not all tasks.

Alternatively, the notification task can be called after all other tasks, and since Prefect runs tasks in the order they are called, but with dependencies, perhaps it's sufficient to call it last.

But Prefect tasks are asynchronous when submitted with .submit(). So the flow function will proceed to the next line after submitting a task. So to ensure that the notification runs after all tasks, perhaps we need to gather all futures and pass them to the notification.

But that's complicated. Alternatively, since the notification is supposed to run after all tasks, regardless of their state, perhaps in Prefect, we can use a hook or a final step that runs after the flow completes. But Prefect 2.x doesn't have built-in on-exit hooks. So the notification task must be part of the flow and depend on all other tasks.

But how to model that. One approach is to have the notification task depend on all other tasks. So:

notification = etl_notification.submit(check_sftp_result, check_shared_result, start1_result, *extraction_tasks, start2_result, upload_result)

But this requires passing all the futures to the notification task. However, this may not be feasible if there are many tasks.

Alternatively, the notification task can be called at the end of the flow function, and since Prefect runs tasks in the order they are called, but with dependencies, perhaps the notification task will run after all previous tasks have completed.

But that's not necessarily true. For example, if some tasks are submitted but not awaited, the flow function may proceed to call the notification task before those tasks have completed.

Therefore, the correct approach is to ensure that the notification task depends on all previous tasks. To do this, we can collect all the futures and pass them as parameters to the notification task.

So:

all_tasks = [check_sftp_result, check_shared_result, start1_result] + extraction_tasks + [start2_result, upload_result]

etl_notification.submit(*all_tasks)

But how to collect all these futures.

In code:

check_sftp_result = check_sftp_folder.submit()
check_shared_result = check_shared_folder.submit(check_sftp_result)
start1_result = start_pcd_extract_1.submit(check_shared_result)

extraction_tasks = [
    status_tracker.submit(start1_result),
    financial_expense.submit(start1_result),
    # ... all others
]

start2_result = start_pcd_extract_2.submit(extraction_tasks[0])

upload_result = pcd_file_upload.submit(*extraction_tasks)

all_futures = [check_sftp_result, check_shared_result, start1_result] + extraction_tasks + [start2_result, upload_result]

etl_notification.submit(*all_futures)

But this requires collecting all the futures into a list and passing them to the notification task. However, the notification task may not need all these parameters, but passing them ensures dependencies.

But the notification task's function would have to accept all these parameters, even if it doesn't use them. For example:

@task
def etl_notification(*args):
    # determine overall success
    success = all(future.get_state().is_completed() for future in args)
    # send email based on success

But Prefect doesn't allow passing futures to tasks directly. Wait, when you submit a task, you get a PrefectFuture. If you pass that future as a parameter to another task, Prefect automatically waits for it to complete before the dependent task runs. So by passing all the futures to the notification task, Prefect ensures that all those tasks have completed before the notification runs.

But the notification task's function would receive the results of all those tasks. However, in this case, many of the tasks (like Kubernetes jobs) may return some result, but the notification task doesn't need them. So the function can ignore the parameters.

So the etl_notification task can be defined as:

@task
def etl_notification(*args):
    # logic to determine overall success
    # check all upstream tasks' states
    # send email

But how to get the states of all upstream tasks. Because in Prefect, the task doesn't have access to the flow's state. So perhaps the notification task needs to receive the results of all upstream tasks and determine if any failed.

Alternatively, the notification task can be designed to accept a list of results, and if any result is an exception, mark as failure.

But this depends on how the tasks are structured. For example, if tasks raise exceptions on failure, then the notification task can catch those.

Alternatively, the notification task can be a Python function that checks the status of all upstream tasks.

But this is complicated. Perhaps the simplest way is to have the notification task depend on all previous tasks by receiving their results (even if unused), and then in the notification task, we can assume that if it runs, all upstream tasks have completed (successfully or failed), and then we can check their final states.

But Prefect doesn't provide a straightforward way to get the states of upstream tasks from within a task. So perhaps the notification task can be designed to send an email regardless, and the email content can mention whether the flow succeeded or failed based on whether any tasks failed.

Alternatively, the notification task can have access to the flow run's state. But that's more advanced.

Given the time constraints, perhaps the notification task can be designed to send a success email if all upstream tasks succeeded, else a failure email. But how to track that.

Alternatively, the notification task can be passed the results of all upstream tasks, and check if any are exceptions.

But if the tasks are designed to return results on success and raise exceptions on failure, then the notification task's *args would contain the results or exceptions.

So in the etl_notification task:

@task
def etl_notification(*dependencies):
    success = True
    for dep in dependencies:
        if isinstance(dep, Exception):
            success = False
            break
    # send email based on success

But how to handle this. When a Prefect task fails, it raises an exception, which would cause the flow to fail unless the task is handled with a retry or something. But in this case, the notification task is supposed to run regardless of upstream failures.

In Prefect, if a task fails, the flow run is marked as failed, but the notification task can still run if it's set with a trigger rule. However, Prefect 2.x uses "orchestration" rules, and the default trigger rule is "all_successful". To have the notification task run regardless, we need to set the trigger rule to "all_finished".

Ah, right! So for the notification task, we can set the trigger rule to "all_finished", which means it will run when all upstream tasks have completed, regardless of their state.

So in the @task decorator, we can specify trigger_rule=all_finished.

But in Prefect 2.x, the trigger rules are part of the task decorator parameters. So:

from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner
from prefect.blocks.core import Block
from prefect.futures import PrefectFuture
from prefect import get_run_logger

@task(trigger_rule=all_finished)
def etl_notification():
    # check the state of upstream tasks
    # send email

But how to get the upstream task states. This is tricky because within the task, we don't have access to the flow's state. So perhaps the notification task needs to receive the results of all upstream tasks, and check if any are exceptions.

But if upstream tasks are designed to return results on success and raise exceptions on failure, then the notification task's parameters would include those results/exceptions.

But in Prefect, if a task fails (raises an exception), any dependent task that requires its result will also fail, unless the trigger rule is set to allow it.

So to have the notification task run even if some tasks failed, we need to set its trigger rule to "all_finished" and ensure that it doesn't require the results of the failed tasks. But how?

Alternatively, the notification task can be designed to not depend on the results of the upstream tasks, but instead just run after all tasks have completed. But Prefect doesn't have a built-in way to do that; dependencies are explicit.

So the solution is to have the notification task depend on all upstream tasks, but with a trigger rule of "all_finished", and in its parameters, accept the results of all upstream tasks. If any upstream task failed, its result would be an exception, which the notification task can check.

But how to collect all the upstream task futures and pass them as parameters to the notification task.

In code:

Within the flow:

# ... previous tasks

all_dependencies = [check_sftp_result, check_shared_result, start1_result] + extraction_tasks + [start2_result, upload_result]

etl_notification.submit(*all_dependencies)

Then, the etl_notification task is defined as:

@task(trigger_rule=all_finished)
def etl_notification(*deps):
    # Check if any dependencies are exceptions
    success = True
    for dep in deps:
        if isinstance(dep, Exception):
            success = False
    # Send email based on success
    logger = get_run_logger()
    if success:
        logger.info("