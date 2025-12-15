<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the steps and dependencies.

First, the pipeline has several steps that are executed in a specific order. The Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder are Kubernetes jobs, with the latter depending on the former. Then there's Start_PCD_Extract_1, which triggers parallel extraction tasks. The Status_Tracker task then triggers Start_PCD_Extract_2. All extraction tasks plus Start_PCD_Extract_2 must complete before PCD_file_upload runs. Finally, ETL_Notification runs regardless of success or failure of upstream tasks.

In Dagster, each step is an op, and dependencies are set using the job's structure. Since the parallel extraction tasks all start after Start_PCD_Extract_1, I'll need to have each of those extraction ops depend on Start_PCD_Extract_1. Then, Start_PCD_Extract_2 depends on Status_Tracker. The PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. The notification op needs to run after all previous tasks, using Dagster's hook or a fan-in approach.

Wait, the notification uses an "all_done" trigger rule, which in Airflow means it runs regardless of upstream success. In Dagster, ops will only run if their upstream dependencies succeed unless we use something like conditional execution or hooks. But since the user mentioned to model branching with standard Python branching within ops or fan-in patterns, maybe the ETL_Notification op should depend on all previous ops, and within the op, check the success of each. Alternatively, use Dagster's event log to determine the overall success.

But how to handle dependencies where some ops might fail? In Dagster, if an upstream op fails, downstream ops won't run unless using a sensor or a different approach. However, the user specified that ETL_Notification should execute after all upstream tasks complete regardless of their success. So perhaps using a root manager or a hook that triggers on job completion. Alternatively, structure the job such that all ops are connected in a way that ETL_Notification depends on all, but use Dagster's built-in mechanisms to handle failures.

Alternatively, since in Dagster, if any upstream op fails, the downstream ops won't run unless they're configured to. So to have ETL_Notification run regardless, maybe all the previous ops should be connected via a fan-in to ETL_Notification, but using some conditional logic. Alternatively, use a collector op that depends on all previous ops, but that might not work if some ops fail.

Hmm, perhaps the best approach is to have ETL_Notification depend on all the upstream ops, but in Dagster, if any of those ops fail, the notification op won't run. So this contradicts the requirement. Therefore, maybe the notification should be triggered via a hook that runs when the job completes, regardless of success or failure. Dagster has hooks that can execute on job success or failure.

Yes, that's a better approach. So instead of making ETL_Notification an op that depends on all previous steps, we can use a job success/failure hook. That way, regardless of whether the job succeeds or fails, the hook will trigger the notification.

But the user's instruction says to model the notification as a Python operator (op) that executes after all upstream tasks complete. So perhaps the original design in Airflow uses an operator with trigger_rule=ALL_DONE. In Dagster, to replicate this, we need to ensure that the ETL_Notification op runs even if some upstream ops fail.

In Dagster, this isn't directly possible because ops only run if their upstream dependencies succeed. So how to handle this? One approach is to have each upstream op emit an output indicating success or failure, and then have a collector op that depends on all of them. But if any op fails, the collector op won't run. Alternatively, use Dagster's RetryPolicy or configure the ops to not fail but return a result indicating failure.

Alternatively, structure the job so that all ops are connected to ETL_Notification via a fan-in, but use a custom logic where ETL_Notification is the only op that has no dependencies, but checks the status of all previous ops. But that's not feasible because Dagster's execution is based on the dependency graph.

Wait, maybe using Dagster's event log within the ETL_Notification op to check the status of all previous steps. But how would that op know about the other steps unless they are upstream?

Alternatively, perhaps the ETL_Notification op should depend on all previous ops, but those ops are configured to not fail on errors. Instead, they could return a result indicating success or failure, and the ETL_Notification op collects these results. However, this would require modifying all the extraction ops to handle exceptions and return a status, which might not align with the original description where some tasks might fail.

This is getting complicated. Let me refer back to the user's constraints. The user says: "If branching/conditional paths appear, model them using standard Python branching within ops or via fan-in patterns." So perhaps the ETL_Notification op should depend on all previous ops, but each of those ops is designed to not raise exceptions (i.e., handle errors internally and pass a status). Then, the ETL_Notification can check the statuses.

But in the original pipeline, the notification is supposed to run regardless of individual task success. So perhaps in Dagster, the ETL_Notification op must run even if some upstream ops fail. But in Dagster, if any upstream op fails, the downstream op won't execute. Therefore, this approach might not work unless we use some form of conditional execution or hooks.

Alternatively, maybe the user's original pipeline uses Airflow's trigger rules, which allow an operator to run even if upstream tasks fail. To replicate this in Dagster, perhaps the ETL_Notification op should not have any upstream dependencies, but that's not possible because it needs to know when all tasks are done.

Another approach: Use a Dagster sensor or a schedule that launches the job, and then have a separate op that checks the status of the entire run. But that might complicate things.

Alternatively, structure the job such that all ops are connected to ETL_Notification via a fan-in, but each op is designed to complete successfully even if their actual task fails (by catching exceptions and returning a failure status). Then, ETL_Notification can collect all the statuses and send the email accordingly.

So, for each extraction op, instead of letting them fail, they would return a success or failure indicator. Then, ETL_Notification depends on all of them and can check the results.

But according to the original pipeline description, the ETL_Notification task executes after all upstream tasks complete regardless of individual success. So in Airflow, even if some tasks fail, the notification task still runs. In Dagster, to achieve this, all upstream ops must complete successfully, which might not be the case. Therefore, perhaps the extraction ops should handle their own exceptions and return a result indicating success or failure, allowing ETL_Notification to run as long as all ops complete (successfully, but with possible failure statuses).

So, modifying the extraction ops to not raise exceptions but return a status. Then, ETL_Notification can depend on all of them and aggregate the results.

This seems feasible. Let's proceed with that approach.

Now, structuring the code:

First, define the Check_PCD_SFTP_Folder op. It uses a Kubernetes job, so perhaps we need a resource for Kubernetes operations. The user mentioned to provide simplified resource examples as comments or stubs. So, maybe add a Kubernetes resource as a comment.

Then, Check_PCD_Shared_Folder depends on Check_PCD_SFTP_Folder.

Start_PCD_Extract_1 is an empty op (a no-op) that depends on Check_PCD_Shared_Folder.

Then, all the parallel extraction ops depend on Start_PCD_Extract_1. Each of these extraction ops would perform an HTTP API extraction. For example, Status_Tracker, Financial_Expense, etc.

Start_PCD_Extract_2 depends on Status_Tracker.

Then, PCD_file_upload depends on all extraction ops and Start_PCD_Extract_2.

Finally, ETL_Notification depends on PCD_file_upload and all extraction ops? Wait, no. According to the original description, ETL_Notification executes after all upstream tasks complete. So ETL_Notification should depend on PCD_file_upload and all extraction tasks, but how?

Wait, the original steps:

After the parallel extraction tasks (all triggered by Start_PCD_Extract_1), Start_PCD_Extract_2 is triggered by Status_Tracker. Then PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. Then ETL_Notification depends on PCD_file_upload.

But according to the description, ETL_Notification should execute after all upstream tasks complete, regardless of their success. So in the original Airflow setup, ETL_Notification is set with trigger_rule=ALL_DONE, meaning it runs after all its upstream tasks are done, regardless of success or failure.

In Dagster, the ETL_Notification op would need to depend on all the upstream tasks. But if any of those tasks fail, the ETL_Notification op won't run. Therefore, to replicate the Airflow behavior, we need to ensure that ETL_Notification runs even if some upstream tasks fail.

This is a problem because Dagster's execution model doesn't support trigger rules like Airflow. So the solution would be to have all upstream tasks not fail but return a status, allowing ETL_Notification to run after all of them.

Therefore, each extraction op and the PCD_file_upload op would need to handle exceptions internally and return a success or failure status. Then, ETL_Notification can depend on all of them and check their outputs.

So, modifying the code:

Each extraction op (like Status_Tracker, Financial_Expense, etc.) would have a try-except block, returning a success boolean or similar. Similarly, the PCD_file_upload op would do the same.

Then, ETL_Notification would depend on all these ops and collect their results to determine the overall status.

But how to structure dependencies? Let's outline the dependencies:

Check_PCD_SFTP_Folder → Check_PCD_Shared_Folder → Start_PCD_Extract_1 → all extraction ops.

Status_Tracker → Start_PCD_Extract_2.

All extraction ops and Start_PCD_Extract_2 → PCD_file_upload.

PCD_file_upload → ETL_Notification.

But according to the original description, ETL_Notification should run after all upstream tasks, including all extraction ops and PCD_file_upload. So ETL_Notification must depend on all extraction ops and PCD_file_upload. But if any of those ops fail, ETL_Notification won't run. Therefore, the extraction ops and PCD_file_upload must not fail but return a status.

So, each extraction op would look like:

@op
def status_tracker(context):
    try:
        # perform extraction
        context.log.info("Extracting data from Status Tracker API")
        # if success:
        return {"status": "success", "data": ...}
    except Exception as e:
        context.log.error(f"Extraction failed: {e}")
        return {"status": "failed", "error": str(e)}

Similarly for other extraction ops.

Then, the PCD_file_upload op would depend on all extraction ops and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is triggered by Status_Tracker, which is one of the extraction ops, perhaps Start_PCD_Extract_2 depends on Status_Tracker, and PCD_file_upload depends on Start_PCD_Extract_2 and all other extraction ops.

Wait, the original description says:

Start_PCD_Extract_2 is triggered by the Status_Tracker task. So in Dagster terms, Start_PCD_Extract_2 op depends on Status_Tracker.

Then, PCD_file_upload depends on all extraction tasks and Start_PCD_Extract_2. So how to model that? For example, PCD_file_upload must wait until all extraction ops and Start_PCD_Extract_2 are done.

But in Dagster, an op can have multiple dependencies. So PCD_file_upload would depend on each extraction op and Start_PCD_Extract_2. That's a lot of dependencies, but possible.

Alternatively, create a dummy op that collects all extraction ops and Start_PCD_Extract_2, then PCD_file_upload depends on that collector. But that's more complex.

Alternatively, have PCD_file_upload depend on Start_PCD_Extract_2 and each extraction op. But since Start_PCD_Extract_2 already depends on Status_Tracker, which is one of the extraction ops, perhaps it's redundant but necessary to ensure all extraction ops are done before PCD_file_upload.

Wait, the original pipeline's PCD_file_upload is dependent on all previous extraction tasks. So in Dagster, PCD_file_upload must depend on each extraction op and Start_PCD_Extract_2. Because Start_PCD_Extract_2 is triggered by Status_Tracker, which is one of the extraction ops, but PCD_file_upload needs to wait for all extraction ops and Start_PCD_Extract_2.

So, for example:

@op
def pcd_file_upload(context, status_tracker_result, financial_expense_result, ...):
    # Wait for all extraction results and Start_PCD_Extract_2
    # process the results and perform upload
    pass

But how to structure the dependencies? Each extraction op's output is an input to pcd_file_upload. But that would require pcd_file_upload to have an input for each extraction op's output. That's possible but cumbersome with many ops.

Alternatively, use a fan-in approach where all extraction ops and Start_PCD_Extract_2 are upstream of pcd_file_upload. So in the job definition:

@job
def pcd_etl_job():
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)
    start_extract_1 = start_pcd_extract_1(shared_check)
    
    # Parallel extraction tasks
    status_tracker_result = status_tracker(start_extract_1)
    financial_expense_result = financial_expense(start_extract_1)
    # ... all other extraction ops
    
    start_extract_2 = start_pcd_extract_2(status_tracker_result)
    
    # PCD_file_upload depends on all extraction ops and start_extract_2
    upload_result = pcd_file_upload(
        status_tracker_result,
        financial_expense_result,
        # ... all other extraction results,
        start_extract_2
    )
    
    # ETL_Notification depends on upload_result and all extraction results?
    etl_notification(upload_result, status_tracker_result, financial_expense_result, ...)

But this approach would require the pcd_file_upload op to have all the extraction results as inputs, which may not be practical. Also, the ETL_Notification would need to depend on all these as well.

Alternatively, the extraction ops could not pass data but just signal completion. In that case, the dependencies can be set without passing data. For example, using the start_pcd_extract_1's output as a trigger.

Wait, in Dagster, if an op doesn't return anything, the downstream ops can still depend on it by using the output. So for example, if start_pcd_extract_1 returns nothing, the extraction ops can still depend on it, and their execution is triggered after start_pcd_extract_1 completes.

Similarly, the pcd_file_upload op can depend on all extraction ops and start_extract_2, even if they don't pass data. So perhaps the extraction ops don't need to return data, but just complete successfully. Then, the pcd_file_upload op depends on their completion.

But then, if any extraction op fails, pcd_file_upload won't run, which contradicts the original requirement where ETL_Notification runs regardless. So this brings us back to the earlier problem.

Therefore, perhaps the extraction ops should handle their own exceptions and not fail, allowing downstream ops to run. So each extraction op would catch exceptions and return a status instead of raising an error.

In that case, even if an extraction op encounters an error, it returns a failure status, and the downstream ops (like pcd_file_upload) can still run because the extraction op completed successfully (in terms of Dagster's execution).

Then, the pcd_file_upload op can proceed, using the statuses from the extraction ops to determine if it should proceed. Similarly, ETL_Notification can collect all statuses to send the email.

This approach requires modifying the extraction ops to handle errors and return a status instead of failing.

So, structuring each extraction op as:

@op
def status_tracker(context):
    try:
        # perform API call
        context.log.info("Extracting Status Tracker data")
        # on success:
        return {"status": "success", "data": ...}
    except Exception as e:
        context.log.error(f"Failed to extract Status Tracker data: {e}")
        return {"status": "failure", "error": str(e)}

Then, the pcd_file_upload op would take all these results as inputs and check their statuses. If any are failures, maybe the upload is skipped or handled accordingly.

Similarly, the ETL_Notification op would take all the results and the upload result to determine the overall status.

But how to collect all the results? For example, if there are 16 extraction ops, the ETL_Notification op would need to have 16 inputs plus the upload result. That's a lot, but manageable.

Alternatively, use a list input, but Dagster doesn't support dynamic inputs. So each input must be explicitly defined.

This could be tedious but necessary.

Putting it all together:

The job would be structured with dependencies as per the original steps. Each extraction op depends on start_pcd_extract_1. The start_pcd_extract_2 depends on status_tracker. The pcd_file_upload depends on all extraction ops and start_pcd_extract_2. The ETL_Notification depends on pcd_file_upload and all extraction ops.

But again, if any extraction op fails (returns a failure status), the pcd_file_upload would still run because the extraction op completed successfully (didn't raise an exception). Then, ETL_Notification would run after pcd_file_upload completes.

Wait, no. If an op returns a value, even if it's a failure status, Dagster considers it successful. So in this case, the extraction ops would complete successfully, allowing downstream ops to run.

So, in this setup:

- All extraction ops handle exceptions and return a status.
- pcd_file_upload depends on all extraction ops and start_extract_2. It can check the statuses and proceed accordingly.
- ETL_Notification depends on pcd_file_upload and can check the statuses of all ops to send the appropriate email.

This seems feasible.

Now, coding this:

First, define the Kubernetes job ops. Since the user mentioned to use simplified resources, perhaps mock them with a comment.

For example:

@op(required_resource_keys={"kubernetes"})
def check_pcd_sftp_folder(context):
    # Example Kubernetes job execution
    context.resources.kubernetes.run_job("check_sftp_job")

But since resources are needed, add a resource definition as a comment.

Then, the Check_PCD_Shared_Folder depends on Check_PCD_SFTP_Folder.

The Start_PCD_Extract_1 and Start_PCD_Extract_2 are empty ops:

@op
def start_pcd_extract_1(_):
    pass

@op
def start_pcd_extract_2(_):
    pass

The parallel extraction ops all depend on start_pcd_extract_1. For example:

@op
def status_tracker(context):
    try:
        # API call
        context.log.info("Extracting Status Tracker data")
        return {"status": "success"}
    except Exception as e:
        context.log.error(f"Extraction failed: {e}")
        return {"status": "failure"}

Similarly for other extraction ops.

Then, start_pcd_extract_2 depends on status_tracker.

The pcd_file_upload op depends on all extraction ops and start_pcd_extract_2:

@op
def pcd_file_upload(context, status_tracker_result, financial_expense_result, ...):
    # Check all results
    # If any failed, log and proceed or abort
    # Perform upload
    context.log.info("Uploading files")
    return {"status": "success"}

Finally, ETL_Notification depends on pcd_file_upload and all extraction ops:

@op
def etl_notification(context, upload_result, status_tracker_result, ...):
    # Check all results and upload_result
    # Determine overall success
    # Send email
    context.log.info("Sending notification email")

Now, structuring the job with all dependencies:

def pcd_etl_job():
    sftp = check_pcd_sftp_folder()
    shared = check_pcd_shared_folder(sftp)
    start1 = start_pcd_extract_1(shared)
    
    # Parallel extraction tasks
    st = status_tracker(start1)
    fe = financial_expense(start1)
    # ... all other extraction ops
    
    start2 = start_pcd_extract_2(st)
    
    upload = pcd_file_upload(
        st, fe, ..., start2  # All extraction results and start2
    )
    
    etl_notification(upload, st, fe, ...)

But this would require the pcd_file_upload op to have all extraction results as inputs, which is a lot. To simplify, perhaps the extraction ops don't pass data but just signal completion. So their outputs are not used, but their execution is required.

Wait, in Dagster, if an op doesn't return an output, you can still define dependencies by using the op's invocation. For example:

@op
def some_op(context):
    pass

@op
def another_op(context, _input):
    pass

Then, in the job:

def my_job():
    a = some_op()
    another_op(a)

But in this case, 'another_op' would receive 'a' as an input, which is None. But if 'another_op' doesn't need the input, this is allowed.

Alternatively, if the extraction ops don't produce outputs needed by downstream ops, their dependencies can be established without passing data.

So, for example:

@op
def status_tracker(context):
    # Do something, no output needed
    pass

Then, in the job:

start1 = start_pcd_extract_1(shared)
st = status_tracker(start1)

In this case, status_tracker depends on start1, but doesn't use its output.

This way, the pcd_file_upload op can depend on all extraction ops and start_extract_2 without needing to handle their outputs.

But then, how would ETL_Notification know the status of each extraction op? It wouldn't, unless each op logs its status in some way that ETL_Notification can access, like writing to a database or a shared context. But that complicates things.

Alternatively, the ETL_Notification op could check the Dagster event log for the job run's success or failure. But that would require accessing Dagster's APIs, which might not be straightforward.

Given the user's constraints, perhaps the simplest way is to model the ETL_Notification as an op that depends on all previous ops, and within the op, check the status of each upstream op's result. But since the extraction ops don't return their statuses (if we model them without outputs), this isn't possible.

Therefore, perhaps the extraction ops should return a status, and the ETL_Notification op collects all these statuses to determine the overall outcome.

But this requires ETL_Notification to have inputs from all extraction ops and the upload op. Given that there are many extraction ops, this would be tedious but manageable.

So, the code would have:

@op
def etl_notification(
    context,
    status_tracker_result,
    financial_expense_result,
    upcc_financial_reporting_result,
    # ... all other extraction results,
    pcd_file_upload_result,
):
    # Check all results and upload_result
    all_results = [
        status_tracker_result,
        financial_expense_result,
        upcc_financial_reporting_result,
        # ... others,
        pcd_file_upload_result,
    ]
    success = all(result["status"] == "success" for result in all_results)
    if success:
        context.log.info("All tasks succeeded")
        # Send success email
    else:
        context.log.error("Some tasks failed")
        # Send failure email

But this requires each extraction op to return a dictionary with a "status" key.

This approach seems feasible but requires each extraction op to return a status, and the ETL_Notification op to accept all these as inputs.

Now, structuring the job with all dependencies:

@job
def pcd_etl_job():
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)
    start_extract_1 = start_pcd_extract_1(shared_check)
    
    # Parallel extraction tasks
    status_tracker_result = status_tracker(start_extract_1)
    financial_expense_result = financial_expense(start_extract_1)
    upcc_financial_reporting_result = upcc_financial_reporting(start_extract_1)
    # ... all other extraction ops
    
    start_extract_2 = start_pcd_extract_2(status_tracker_result)
    
    # PCD_file_upload depends on all extraction results and start_extract_2
    pcd_upload_result = pcd_file_upload(
        status_tracker_result=status_tracker_result,
        financial_expense_result=financial_expense_result,
        upcc_financial_reporting_result=upcc_financial_reporting_result,
        # ... all other extraction results,
        start_extract_2=start_extract_2,
    )
    
    # ETL_Notification depends on all extraction results and pcd_upload_result
    etl_notification(
        status_tracker_result=status_tracker_result,
        financial_expense_result=financial_expense_result,
        upcc_financial_reporting_result=upcc_financial_reporting_result,
        # ... all other extraction results,
        pcd_file_upload_result=pcd_upload_result,
    )

This structure ensures that ETL_Notification runs after all extraction ops and the upload op. However, if any extraction op fails (i.e., raises an exception), the subsequent ops (like pcd_file_upload and ETL_Notification) won't run. Therefore, to ensure ETL_Notification runs regardless, the extraction ops must not raise exceptions but return a status.

Thus, each extraction op must handle exceptions internally and return a status. This way, even if an extraction fails, the op completes successfully, allowing downstream ops to run.

Now, implementing this:

Each extraction op:

@op
def status_tracker(context):
    try:
        # Perform extraction
        context.log.info("Extracting from Status Tracker API")
        return {"status": "success"}
    except Exception as e:
        context.log.error(f"Extraction failed: {e}")
        return {"status": "failure", "error": str(e)}

Similarly for other extraction ops.

The pcd_file_upload op:

@op
def pcd_file_upload(context, **kwargs):
    # Check all inputs (extraction results and start_extract_2)
    # For example:
    for key, value in kwargs.items():
        if key != "start_extract_2" and value.get("status") != "success":
            context.log.error(f"Extraction {key} failed, upload may be incomplete")
    
    # Proceed with upload
    context.log.info("Uploading files to destination")
    return {"status": "success"}

The ETL_Notification op:

@op
def etl_notification(context, **kwargs):
    success = all(
        result.get("status") == "success"
        for key, result in kwargs.items()
        if key != "pcd_file_upload_result"
    ) and kwargs["pcd_file_upload_result"].get("status") == "success"
    
    if success:
        context.log.info("ETL succeeded")
        # Send success email
    else:
        context.log.error("ETL failed")
        # Send failure email

But handling all the inputs in ETL_Notification would require explicitly listing them, which is not feasible with **kwargs. Therefore, each extraction op's result must be passed as individual arguments.

Alternatively, structure the inputs as a list, but Dagster doesn't support dynamic inputs. So each extraction op's result must be an input to ETL_Notification.

This leads to a very long list of inputs for ETL_Notification, but it's manageable.

Finally, the Kubernetes resource setup. The user mentioned to provide simplified resource examples. So:

# Example Kubernetes resource configuration
# resources:
#   kubernetes:
#     config:
#       cluster_config: ...
#       namespace: ...

But in code, we can define a resource stub.

Putting it all together, here's the code:
</think>

from dagster import op, job

# Example Kubernetes resource stub (configure in Dagster's resources)
# resources = {"kubernetes": ...}

@op(required_resource_keys={"kubernetes"})
def check_pcd_sftp_folder(context):
    context.log.info("Checking SFTP folder via Kubernetes job")
    # Example: context.resources.kubernetes.run_job("check_sftp_job")

@op(required_resource_keys={"kubernetes"})
def check_pcd_shared_folder(context):
    context.log.info("Checking shared folder via Kubernetes job")
    # Example: context.resources.kubernetes.run_job("check_shared_job")

@op
def start_pcd_extract_1(_):
    context.log.info("Starting parallel extraction phase 1")

@op
def status_tracker(context):
    try:
        context.log.info("Extracting Status Tracker data")
        return {"status": "success"}
    except Exception as e:
        context.log.error(f"Status Tracker extraction failed: {e}")
        return {"status": "failure"}

# Define similar extraction ops for other endpoints
@op
def financial_expense(context):
    try:
        context.log.info("Extracting Financial Expense data")
        return {"status": "success"}
    except Exception as e:
        context.log.error(f"Financial Expense extraction failed: {e}")
        return {"status": "failure"}

@op
def upcc_financial_reporting(context):
    try:
        context.log.info("Extracting UPCC Financial Reporting data")
        return {"status": "success"}
    except Exception as e:
        context.log.error(f"UPCC Financial Reporting extraction failed: {e}")
        return {"status": "failure"}

# Add remaining extraction ops with same pattern...

@op
def start_pcd_extract_2(_):
    context.log.info("Starting parallel extraction phase 2")

@op(required_resource_keys={"kubernetes"})
def pcd_file_upload(context):
    context.log.info("Performing file upload via Kubernetes job")
    # Example: context.resources.kubernetes.run_job("file_upload_job")
    return {"status": "success"}

@op
def etl_notification(context, upload_status, *extraction_results):
    all_success = upload_status["status"] == "success" and all(
        result["status"] == "success" for result in extraction_results
    )
    
    if all_success:
        context.log.info("ETL succeeded - sending success email")
    else:
        context.log.error("ETL failed - sending failure email")

@job
def pcd_etl_job():
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)
    start1 = start_pcd_extract_1(shared_check)
    
    st = status_tracker(start1)
    fe = financial_expense(start1)
    ufr = upcc_financial_reporting(start1)
    # Add remaining extraction ops...
    
    start2 = start_pcd_extract_2(st)
    upload_status = pcd_file_upload(start2)
    
    etl_notification(upload_status, st, fe, ufr)  # Add all extraction results

if __name__ == "__main__":
    pcd_etl_job.execute_in_process()