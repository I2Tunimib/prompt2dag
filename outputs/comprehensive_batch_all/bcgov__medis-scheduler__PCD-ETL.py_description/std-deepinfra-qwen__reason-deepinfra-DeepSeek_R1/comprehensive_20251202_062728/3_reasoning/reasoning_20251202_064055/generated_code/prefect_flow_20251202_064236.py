<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements and constraints.

First, the pipeline has several steps, some of which are parallel. The user mentioned using @task and @flow decorators. For parallel tasks, I should use .submit() to run them concurrently. Sequential steps are called normally.

Looking at the steps:

1. Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder are Kubernetes jobs. The shared folder check depends on the SFTP check. So these two tasks are sequential.

2. Then there's Start_PCD_Extract_1, which is an empty operator as a sync point. Since it's just a synchronization, maybe I can represent it as a task that does nothing, perhaps a dummy function.

3. After that, multiple parallel HTTP API extraction tasks are triggered. All of these tasks depend on Start_PCD_Extract_1. So I need to gather all these tasks and have them start after the start task. Since they're parallel, each should be submitted with .submit().

4. The Status_Tracker task is part of the parallel extraction, but Start_PCD_Extract_2 depends on it. So after Status_Tracker completes, Start_PCD_Extract_2 runs. Then, the PCD_file_upload task depends on all previous extraction tasks and Start_PCD_Extract_2.

5. Finally, the ETL_Notification task runs regardless of the success or failure of upstream tasks. In Prefect, this is handled with a wait_for parameter and possibly using the trigger argument, but Prefect 2 doesn't have trigger rules like Airflow. Instead, I can use the task runner and ensure that the notification task waits for all upstream tasks, even if some fail. Using a wait_for parameter with all the task futures should work.

Now, considering the Kubernetes jobs: Since Prefect doesn't have a KubernetesJobOperator built-in, I'll need to create tasks that interface with Kubernetes. Maybe using the kubernetes Python client or subprocess to run kubectl commands. But the user wants executable code, so perhaps mock functions with placeholders, but the code must be complete. So I'll create tasks that have a placeholder for Kubernetes job execution, maybe using print statements or logging.

For the email notification, again, Prefect doesn't have Airflow's email backend, so I'll create a task that simulates sending an email, perhaps using smtplib or a mock function.

The original pipeline mentions Airflow variables, but in Prefect, configurations can be handled via blocks, environment variables, or parameters. Since the user wants minimal code, I'll parameterize the flow with the necessary configurations, like schedule, timeout, etc.

The schedule is dynamic via an Airflow variable, but Prefect uses deployments for scheduling. The user said to include a comment about deployment/schedule if mentioned. So I'll add a comment indicating that the schedule is configured via a deployment.

The flow should have a 60-minute timeout. In Prefect, this can be set with the timeout_seconds parameter in the @flow decorator.

Catchup is disabled, which in Prefect is handled by the deployment configuration (schedule with catchup=False), so another comment there.

The notification task uses "all_done" trigger rule. In Prefect, tasks run when their upstream dependencies are complete, regardless of success or failure. So if ETL_Notification depends on all previous tasks, it will run after all are done. To capture failures, the task can check the states of the upstream tasks.

Now, structuring the code:

- Import necessary modules: from prefect import flow, task, get_run_logger
- Define each step as a task. For Kubernetes jobs, create tasks with a placeholder implementation.
- The flow function orchestrates the tasks in order.
- For parallel tasks, collect their futures and pass them to subsequent tasks that need to wait for them.
- The ETL_Notification task should receive all the task futures to determine the overall success.

Wait, in Prefect 2, when you submit tasks with .submit(), they return futures. To wait for all of them, you can gather the futures and then pass them to the next task. However, if a task fails, the flow might fail unless handled. But the notification task needs to run regardless. So perhaps using a try-except block around the flow, but the user wants the notification to run after all tasks. Alternatively, using a final task that is always triggered.

But Prefect 2's model is that tasks run when their upstreams are complete. So if ETL_Notification is set to wait for all the upstream tasks (even if some failed), it will run. So in the flow, after all the extraction tasks and the upload task, we pass their futures to the notification task.

Wait, but how to collect all the task futures? Let's see:

The parallel extraction tasks are all submitted, and their futures are stored in variables. Then, the upload task depends on all of them plus Start_PCD_Extract_2. Then, the notification task depends on the upload task and the extraction tasks? Or maybe the notification should depend on all tasks in the flow.

Alternatively, in the flow, after all tasks are launched, collect all their futures and pass them to the notification task. But how?

Alternatively, the notification task doesn't need explicit dependencies because Prefect tracks all upstream tasks automatically. But in the problem description, the notification task should execute after all upstream tasks complete, regardless of their state.

In Prefect, if a task has no upstream dependencies, it runs at the start. So to make the notification task run last, it should depend on all previous tasks.

So in the flow, after all tasks are done (including the upload), the notification task is called, and it waits for all those futures.

But how to collect all the futures? Let's think:

The flow would be structured as:

@flow(timeout_seconds=3600)
def pcd_etl_flow():
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)
    start1 = start_pcd_extract_1(shared_check)
    
    # Parallel tasks
    status_tracker = status_tracker_task(start1).submit()
    financial_expense = financial_expense_task(start1).submit()
    # ... all other parallel tasks submitted here
    
    # Wait for status_tracker to start Start_PCD_Extract_2
    start2 = start_pcd_extract_2(status_tracker)
    
    # Upload depends on all parallel tasks and start2
    # Need to collect all extraction futures
    extraction_tasks = [status_tracker, financial_expense, ...]
    upload = pcd_file_upload(extraction_tasks, start2)
    
    # Notification depends on upload and all extraction tasks?
    etl_notification(upload, extraction_tasks)

Wait, but in Prefect, when you pass futures as parameters, the task will wait for those futures to complete. So the upload task would need to wait for all extraction tasks and start2. Then the notification task would wait for upload and extraction tasks again? Or perhaps the upload task is the last step before notification, and the notification depends on upload.

But according to the pipeline description, the notification runs after all upstream tasks complete, regardless of their success. So the notification should depend on all tasks in the flow.

But how to structure that. Maybe collect all the task futures and pass them to the notification task.

Alternatively, the notification task can be called at the end, without any dependencies, but that might not work because Prefect tasks are only executed if they are called within the flow. So perhaps the notification task should be given all the task futures as parameters to ensure it waits for them.

But that could be cumbersome. Let's see:

In the flow:

After starting all parallel tasks, collect their futures in a list. Then, after the upload task (which depends on them and start2), pass all those futures plus the upload future to the notification task.

So:

extraction_tasks = [status_tracker, financial_expense, upcc_financial_reporting, ...]
start2 = start_pcd_extract_2(status_tracker)
upload = pcd_file_upload(start2, *extraction_tasks)
notification = etl_notification(upload, *extraction_tasks)

But each of these tasks would have to accept *args or similar. Alternatively, the notification task can take no parameters but is placed after all other tasks in the flow, so Prefect knows to run it last. But I'm not sure if that's how Prefect's dependency system works.

Alternatively, the notification task could depend on the upload task, which in turn depends on all previous tasks. Then, the notification task would run after the upload task completes, and since the upload task depends on all extraction tasks, the notification would effectively run after all.

But according to the pipeline description, the notification should execute after all upstream tasks complete, regardless of individual success. So even if some extraction tasks fail, the upload might not run, but the notification should still run. Hmm, that complicates things.

Wait, the original description says: "ETL_Notification: ... executes after all upstream tasks complete (regardless of individual success/failure)". So the notification task is triggered when all upstream tasks (all previous tasks) have finished, regardless of their state.

In Prefect, this can be achieved by making the notification task depend on all previous tasks. But how to collect all those dependencies.

In the flow, after all tasks are submitted, collect all their futures and pass them as parameters to the notification task. Then, the notification task will wait for all those futures to complete (successfully or not) before executing.

So, in code:

extraction_tasks = [task1.submit(), task2.submit(), ...]
start2 = start_pcd_extract_2(...)
upload = pcd_file_upload(start2, *extraction_tasks)
all_upstream_tasks = extraction_tasks + [sftp_check, shared_check, start1, start2, upload]
etl_notification(all_upstream_tasks)

But how to pass all these futures to the notification task. Each task that is submitted returns a future. So in the flow, collect all the futures into a list and pass them to the notification task.

But the notification task's function would need to accept these futures. However, the notification task might not actually need the results, just to know the statuses. So perhaps the notification task can take a list of futures, and then check their states.

But in Prefect, when you pass a future as a parameter to a task, the task waits for that future to complete before running. So if we pass all the futures to the notification task, it will wait for all of them to complete, regardless of success or failure.

So, in the flow:

all_tasks = [sftp_check, shared_check, start1, status_tracker, financial_expense, ..., start2, upload]
etl_notification(all_tasks)

But how to collect all these futures. Let's outline the code step by step.

First, the flow starts with the SFTP check and shared folder check, which are sequential.

Then, start_pcd_extract_1 runs after shared_check.

Then, all the parallel extraction tasks are submitted, each depending on start1.

Then, start_pcd_extract_2 runs after status_tracker.

Then, the upload task depends on start2 and all extraction tasks.

Finally, the notification task depends on all tasks.

So, code outline:

@flow(timeout_seconds=3600)
def pcd_etl_flow():
    logger = get_run_logger()
    
    # Step 1: SFTP check
    sftp_result = check_pcd_sftp_folder()
    
    # Step 2: Shared folder check depends on SFTP
    shared_result = check_pcd_shared_folder(sftp_result)
    
    # Step 3: Start extract 1
    start1 = start_pcd_extract_1(shared_result)
    
    # Step 4: Parallel extraction tasks
    extraction_tasks = []
    status_tracker_future = status_tracker_task(start1).submit()
    extraction_tasks.append(status_tracker_future)
    financial_expense_future = financial_expense_task(start1).submit()
    extraction_tasks.append(financial_expense_future)
    # ... do this for all parallel tasks
    
    # Wait for Status_Tracker to complete before Start_PCD_Extract_2
    start2 = start_pcd_extract_2(status_tracker_future)
    
    # Step 5: Upload depends on all extraction tasks and start2
    # Wait for all extraction tasks and start2
    # To wait for all extraction tasks, we can pass them as parameters
    upload_result = pcd_file_upload(start2, *extraction_tasks)
    
    # Step 6: Notification depends on all tasks
    # Collect all task futures
    all_tasks = [sftp_result, shared_result, start1, start2, upload_result] + extraction_tasks
    etl_notification(all_tasks)

But how to pass all_tasks to etl_notification? Since each element in all_tasks is a future, the etl_notification task will wait for all of them to complete before executing.

In the etl_notification task, we can check the status of each task. For example:

@task
def etl_notification(tasks):
    logger = get_run_logger()
    success = all(task.result().is_completed() for task in tasks)  # Not sure about the exact method
    if success:
        logger.info("All tasks succeeded")
        # send success email
    else:
        logger.error("Some tasks failed")
        # send failure email

But handling the results correctly would require checking each task's state. However, in Prefect, when a task fails, it raises an exception. But if the etl_notification task is waiting for the futures, and some tasks failed, how does that affect it?

Wait, when you pass a future to a task as a parameter, the task will wait for the future to complete. If the future failed, accessing its result() in the task will raise an exception. So to handle failures, the etl_notification task needs to catch exceptions or check the state of each future.

Alternatively, in Prefect, you can retrieve the state of each task. Maybe using Prefect's client or the task's state attribute. But I'm not sure how to do that within the task.

Alternatively, the etl_notification task can iterate over the futures and check if any of them failed. For example:

from prefect import get_run_logger
from prefect.states import State

@task
def etl_notification(tasks):
    logger = get_run_logger()
    failed = False
    for task_future in tasks:
        state = task_future.get_state()
        if not state.is_completed() or state.is_failed():
            failed = True
            logger.error(f"Task {task_future} failed with state {state}")
    if failed:
        logger.error("ETL pipeline failed. Sending failure notification.")
        # send failure email
    else:
        logger.info("ETL pipeline succeeded. Sending success notification.")
        # send success email

But how to get the state of each future. The future object has a get_state() method? Or perhaps we need to use the Prefect client to query the state.

Alternatively, when you call .result() on a future, it will raise an exception if the task failed. So in the notification task, we can try to get the result and catch exceptions.

But since the notification task is supposed to run regardless of failure, perhaps we can loop through each task future and check their statuses.

Wait, in Prefect 2, when you submit a task, you get a PrefectFuture. To get the state, you can use the .wait() method with a timeout of 0, but I'm not sure.

Alternatively, perhaps the code can be structured to collect all the task futures and then, in the notification task, check their states.

But this might be complex. For the sake of creating executable code, perhaps the notification task can simply check if all upstream tasks succeeded by attempting to get their results. If any result raises an exception, then the notification marks it as a failure.

But handling this in code would require try-except blocks around each task.result() call.

Alternatively, the notification task can assume that if it is running, all upstream tasks have completed (successfully or failed), and then check their states.

But I'm not sure how to implement this correctly. For the purpose of this code, perhaps the notification task can log a message indicating success or failure based on whether any exceptions were raised in the upstream tasks.

But since the code needs to be executable, maybe the notification task can just print a message based on whether all tasks succeeded. For example:

@task
def etl_notification(tasks):
    logger = get_run_logger()
    success = True
    for task_future in tasks:
        try:
            task_future.result()
        except Exception:
            success = False
    if success:
        logger.info("All tasks completed successfully. Sending success email.")
        # send_email(success=True)
    else:
        logger.error("Some tasks failed. Sending failure email.")
        # send_email(success=False)

This way, the notification task tries to get the result of each task, which will raise an exception if the task failed. Catching the exception allows us to determine if any task failed.

However, in Prefect, if a task in the flow fails, the flow itself will fail unless the exception is handled. But since the notification task is set to run after all tasks, even if some tasks failed, the flow would proceed to the notification task only if the failed tasks are handled.

Wait, no. In Prefect, if a task fails, the flow run will be marked as failed unless the exception is caught. But the notification task is supposed to run even if some tasks failed. So how to handle this?

Ah, Prefect 2 allows tasks to have retries or to continue on failure. But in this case, the notification task needs to run regardless of upstream failures. To achieve this, the flow should not fail before reaching the notification task. This can be done by using task.submit() which allows the flow to continue even if the task fails, and then the notification task can check the results.

Wait, when you submit a task with .submit(), it runs asynchronously, and any exceptions in the task are captured in the future. The flow will continue executing until it reaches the notification task, which then checks the futures.

So, in the flow, even if some extraction tasks fail, the flow will proceed to the upload task (if possible) and then the notification task. However, if the upload task depends on the failed extraction tasks, it might not run. But according to the pipeline description, the upload task is dependent on all previous extraction tasks. So if any extraction task fails, the upload task might not run. But the notification task should still run.

This suggests that the upload task's dependencies should allow it to run even if some extraction tasks fail. But Prefect's default behavior is that if a task's upstream dependency fails, the task is not run. So how to handle this?

In the original pipeline description, the ETL_Notification uses the "all_done" trigger rule, which means it runs after all upstream tasks are done, regardless of their state. In Prefect, this is not directly possible with the default task dependencies. To achieve similar behavior, the notification task should not depend on the upload task, but rather on all the tasks that need to complete before it.

Alternatively, the notification task can be made to depend on all tasks, regardless of their success. But Prefect doesn't have a built-in trigger rule for this. So the solution is to have the notification task depend on all the task futures, and handle any exceptions when retrieving their results.

But how to structure the dependencies correctly.

Let me think again. The flow steps are:

1. SFTP check → shared check → start1 → parallel extraction tasks.

2. Among the parallel tasks, Status_Tracker → start2 → upload.

3. Upload depends on all extraction tasks and start2.

4. Notification depends on all tasks.

But if any extraction task fails, the upload task may not run (since it depends on all extraction tasks). So the upload task would be skipped if any extraction task fails. Then, the notification task depends on upload, which may not have run. So the notification task would not run unless we structure dependencies correctly.

This is a problem. The notification task needs to run regardless of whether previous tasks succeeded or failed.

In Prefect, a task will only run if all its upstream dependencies have completed (successfully or not). Wait, no. By default, if an upstream task fails, the downstream task is not run. To change this behavior, you can set a task's trigger to "all_finished" or similar. But Prefect 2.x does not have trigger rules like Airflow. Instead, you can use the allow_failure parameter when passing futures to a task.

Wait, Prefect has a feature where you can pass futures with allow_failure=True, which allows the task to receive the result even if the future failed. But I'm not sure how that works.

Alternatively, the notification task can be designed to not depend on any specific tasks but instead be the last step in the flow. However, Prefect's execution is based on the task dependencies. So to ensure the notification task runs after all other tasks, it must depend on them.

So, the solution is to have the notification task depend on all tasks in the flow. But how to collect all those dependencies.

In code:

In the flow function, after all tasks are submitted, collect all their futures into a list and pass them to the notification task. The notification task will then wait for all those futures to complete (regardless of their success) before executing.

So:

@flow(timeout_seconds=3600)
def pcd_etl_flow():
    sftp = check_pcd_sftp_folder()
    shared = check_pcd_shared_folder(sftp)
    start1 = start_pcd_extract_1(shared)
    
    # Parallel extraction tasks
    extraction_futures = [
        status_tracker_task(start1).submit(),
        financial_expense_task(start1).submit(),
        # ... all other extraction tasks
    ]
    
    # Start2 depends on status_tracker
    start2 = start_pcd_extract_2(extraction_futures[0])  # assuming status_tracker is first
    
    # Upload depends on start2 and all extraction tasks
    upload = pcd_file_upload(start2, *extraction_futures)
    
    # Collect all task futures
    all_futures = [sftp, shared, start1, start2, upload] + extraction_futures
    
    # Notification depends on all futures
    etl_notification(all_futures)

Then, in the etl_notification task:

@task
def etl_notification(task_futures):
    logger = get_run_logger()
    failed = False
    for future in task_futures:
        try:
            future.result()
        except Exception:
            failed = True
    if failed:
        logger.error("ETL failed. Sending failure email.")
        # send email
    else:
        logger.info("ETL succeeded. Sending success email.")
        # send email

But wait, the upload task depends on all extraction tasks. So if any extraction task fails, the upload task will not run. Therefore, the upload future would be in a failed state or not exist. But in Prefect, if a task's upstream dependencies fail, the task is not run. So the upload task would not run, and its future would not be completed. Therefore, the notification task, which depends on upload, would not run.

This contradicts the requirement that the notification task runs after all upstream tasks complete, regardless of their success.

Ah, this is a problem. The upload task is dependent on all extraction tasks. If any extraction task fails, the upload task is not executed. Therefore, the notification task, which depends on the upload task, would not run.

To fix this, the upload task's dependencies should allow it to run even if some extraction tasks fail. But how?

In Prefect, you can use the wait_for parameter with allow_failure=True. For example:

upload = pcd_file_upload.submit(start2, *extraction_futures, wait_for=[allow_failure(...)])

But I'm not sure about the exact syntax. Alternatively, when creating the upload task, specify that it should run even if some dependencies failed.

Alternatively, the upload task should not depend on the extraction tasks' success. But how to structure that.

Wait, perhaps the upload task should depend on the completion of the extraction tasks, not their successful completion. In Prefect, this can be achieved by using the wait_for parameter with allow_failure=True.

But I'm not sure how to implement this. Let me check Prefect's documentation.

Upon checking, Prefect allows you to pass futures to a task with the wait_for parameter. By default, all wait_for futures must complete successfully. To allow failures, you can use the allow_failure parameter.

Wait, Prefect 2.0 introduced the concept of task runners and the ability to wait for tasks with certain conditions. For example:

from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner

@flow(task_runner=SequentialTaskRunner())
def my_flow():
    future1 = task1.submit()
    future2 = task2.submit()
    
    # Wait for both tasks to complete, regardless of success
    result = task3.submit(wait_for=[future1, future2], allow_failure=True)

But I'm not sure about the exact syntax. Alternatively, when submitting a task, you can specify which futures it should wait for, and whether failures are allowed.

Alternatively, the upload task can be submitted with allow_failure=True for its dependencies. This way, even if some extraction tasks failed, the upload task will still run.

But I'm not sure how to implement this. For the sake of this code, perhaps the upload task should not depend on the extraction tasks' success but just their completion. So, in the flow, when calling pcd_file_upload, we pass the extraction futures with allow_failure=True.

But how to do that in code.

Alternatively, the upload task can be submitted with wait_for=[all_extraction_futures], and allow_failure=True. This would allow the upload task to run even if some extraction tasks failed.

But I'm not sure about the syntax. Maybe:

upload_future = pcd_file_upload.submit(
    start2,
    wait_for=[extraction_futures],
    allow_failure=True
)

But this is speculative. Since the user wants executable code, perhaps the best approach is to structure the upload task to depend on start2 and all extraction tasks, but in a way that allows failures.

Alternatively, perhaps the upload task doesn't actually depend on the extraction tasks' data, but just their completion. So even if they failed, the upload task runs. But how to model that.

In Prefect, if you want a task to run after other tasks regardless of their success, you can use the wait_for parameter with allow_failure=True.

For example:

upload = pcd_file_upload.submit(
    start2,
    wait_for=[extraction_futures],
    allow_failure=True
)

But I'm not sure about the exact syntax. Alternatively, when submitting the upload task, pass all extraction futures as wait_for with allow_failure=True.

But this requires that the upload task doesn't take any parameters from the extraction tasks. If the upload task doesn't need the results of the extraction tasks, then this is possible.

Assuming that the upload task doesn't need the results, just that the extraction tasks have completed (successfully or not), then:

In the flow:

upload_future = pcd_file_upload.submit(
    start2,
    wait_for=extraction_futures,
    allow_failure=True
)

Then, the upload task will run after all extraction tasks have completed, regardless of their success.

This way, even if some extraction tasks failed, the upload task will still run. Then, the notification task can depend on the upload task and all other tasks.

But how to collect all the task futures for the notification.

In code:

extraction_futures = [task.submit() for each extraction task]
start2_future = start_pcd_extract_2.submit(status_tracker_future)
upload_future = pcd_file_upload.submit(start2_future, wait_for=extraction_futures, allow_failure=True)

all_futures = [sftp_future, shared_future, start1_future, start2_future, upload_future] + extraction_futures

notification_future = etl_notification.submit(all_futures)

But this requires that each step is submitted as a future. However, the initial steps (sftp, shared, start1) are called normally, not submitted, so they are not futures. Wait, no: in Prefect, if you call a task normally within a flow, it runs sequentially, and returns the result. If you submit it, it returns a future.

So to collect all futures, all tasks must be submitted. But the initial tasks (sftp, shared, start1) are sequential, so they can be called normally, but then their futures are not available. Unless we submit them as well.

But that complicates things. Alternatively, perhaps all tasks should be submitted to allow collecting their futures.

But the initial tasks are sequential: sftp must complete before shared starts, which must complete before start1.

If we submit them, we can still enforce the order.

For example:

sftp_future = check_pcd_sftp_folder.submit()
shared_future = check_pcd_shared_folder.submit(sftp_future)
start1_future = start_pcd_extract_1.submit(shared_future)

Then, the parallel extraction tasks are submitted with start1_future as a dependency.

But this requires that all tasks are submitted, which might complicate the code but allows collecting all futures.

So, adjusting the code:

@flow(timeout_seconds=3600)
def pcd_etl_flow():
    # Step 1: SFTP check
    sftp_future = check_pcd_sftp_folder.submit()
    
    # Step 2: Shared folder check depends on SFTP
    shared_future = check_pcd_shared_folder.submit(sftp_future)
    
    # Step 3: Start extract 1 depends on shared check
    start1_future = start_pcd_extract_1.submit(shared_future)
    
    # Step 4: Parallel extraction tasks depend on start1
    extraction_futures = [
        status_tracker_task.submit(start1_future),
        financial_expense_task.submit(start1_future),
        # ... all other extraction tasks
    ]
    
    # Step: Start_PCD_Extract_2 depends on Status_Tracker
    status_tracker_future = extraction_futures[0]  # assuming first is status_tracker
    start2_future = start_pcd_extract_2.submit(status_tracker_future)
    
    # Step: Upload depends on start2 and all extraction tasks (allow failures)
    upload_future = pcd_file_upload.submit(
        start2_future,
        wait_for=extraction_futures,
        allow_failure=True
    )
    
    # Collect all futures for notification
    all_futures = [sftp_future, shared_future, start1_future, start2_future, upload_future] + extraction_futures
    
    # Notification task depends on all futures
    etl_notification.submit(all_futures)

But wait, the upload task's submit method has parameters. The pcd_file_upload task may need to take start2 and possibly other parameters. But if the task doesn't actually use the results of the extraction tasks, then it's okay. Otherwise, we need to pass them as parameters.

Assuming that the upload task requires the output of start2 and the extraction tasks, but since some extraction tasks may have failed, their results would be unavailable. So perhaps the upload task doesn't require the results, just that the tasks have completed.

This is a bit of a stretch, but for the sake of creating executable code, we can proceed.

Now, the etl_notification task is submitted with all_futures as a parameter. The task will wait for all these futures to complete (regardless of success) before executing.

In the etl_notification task, we can loop through each future and check if it succeeded or failed.

Putting it all together:

The code would look like this:

from prefect import flow, task, get_run_logger

@task
def check_pcd_sftp_folder():
    logger = get_run_logger()
    logger.info("Checking PCD SFTP folder")
    # Execute Kubernetes job to check SFTP folder
    return "sftp_check_result"

@task
def check_pcd_shared_folder(sftp_result):
    logger = get_run_logger()
    logger.info("Checking PCD shared folder")
    # Execute Kubernetes job to check shared folder
    return "shared_check_result"

@task
def start_pcd_extract_1(shared_check_result):
    logger = get_run_logger()
    logger.info("Starting parallel extraction phase 1")
    return "start1_result"

@task
def status_tracker_task(start_signal):
    logger = get_run_logger()
    logger.info("Extracting data from Status Tracker API")
    return "status_tracker_data"

# Define all other extraction tasks similarly
@task
def financial_expense_task(start_signal):
    logger = get_run_logger()
    logger.info("Extracting financial expense data")
    return "financial_expense_data"

# ... other extraction tasks (similarly defined)

@task
def start_pcd_extract_2(status_tracker_result):
    logger = get_run_logger()
    logger.info("Starting parallel extraction phase 2")
    return "start2_result"

@task
def pcd_file_upload(start_signal, *extraction_results):
    logger = get_run_logger()
    logger.info("Uploading consolidated files")
    # Execute Kubernetes job for file upload
    return "upload_result"

@task
def etl_notification(task_futures):
    logger = get_run_logger()
    failed = False
    for future in task_futures:
        try:
            future.result()
        except Exception as e:
            logger.error(f"Task {future} failed: {e}")
            failed = True
    if failed:
        logger.error("ETL pipeline failed. Sending failure email.")
        # Implement email sending logic here
    else:
        logger.info("ETL pipeline succeeded. Sending success email.")
        # Implement email sending logic here

@flow(timeout_seconds=3600)
def pcd_etl_flow():
    sftp_future = check_pcd_sftp_folder.submit()
    shared_future = check_pcd_shared_folder.submit(sftp_future)
    start1_future = start_pcd_extract_1.submit(shared_future)
    
    extraction_futures = [
        status_tracker_task.submit(start1_future),
        financial_expense_task.submit(start1_future),
        # Add all other extraction tasks here
    ]
    
    status_tracker_future = extraction_futures[0]
    start2_future = start_pcd_extract_2.submit(status_tracker_future)
    
    upload_future = pcd_file_upload.submit(
        start2_future,
        wait_for=extraction_futures,
        allow_failure=True
    )
    
    all_futures = [sftp_future, shared_future, start1_future, start2_future, upload_future] + extraction_futures
    etl_notification.submit(all_futures)

if __name__ == "__main__":
    pcd_etl_flow()

But wait, the extraction_futures list includes all the parallel tasks. However, the pipeline description lists many extraction tasks (Status_Tracker, Financial_Expense, UPCC_Financial_Reportingr, etc.), which I've only partially implemented. For brevity, the code should include all the tasks mentioned, but in the sample code, I'll have to include placeholders for all of them.

But the user said to avoid placeholders like <TODO>, so I need to define all tasks explicitly.

So, adding all the extraction tasks:

@task
def upcc_financial_reporting_task(start_signal):
    logger = get_run_logger()
    logger.info("Extracting UPCC Financial Reporting data")
    return "upcc_financial_data"

@task
def chc_financial_reporting_task(start_signal):
    logger = get_run_logger()
    logger.info("Extracting CHC Financial Reporting data")
    return "chc_financial_data"

# Continue for all other extraction tasks...

But this would make the code lengthy. However, the user requires complete code, so I'll have to include all tasks.

But given the time constraints, perhaps abbreviate by including a few examples and comments indicating the others.

But the user said to output complete code. So I'll have to write out all the tasks.

Alternatively, use a loop or dynamic task creation, but Prefect tasks must