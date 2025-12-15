<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the steps and their dependencies.

First, the pipeline has four main steps: ingest_csv, quality_check, conditional branches (production_load and quarantine_and_alert followed by send_alert_email), and cleanup. The flow should orchestrate these tasks in order with branching.

The constraints mention using @task and @flow decorators. Each task should be a separate function. For parallel steps, like the two conditional branches, I should use .submit() to run them concurrently. But wait, in this case, the branches are mutually exclusive—either one or the other runs based on the quality score. So maybe they don't run in parallel? Hmm, but the description says "fan-out from quality_check to two parallel paths". Wait, maybe the idea is that both paths are defined but only one is executed? Or perhaps the tasks after the condition can run in parallel if both branches are possible. But according to the description, it's a branch point based on the score. So perhaps the tasks after the condition are not parallel but alternative paths. However, the orchestration part mentions "fan-out" with maximum width 2, which suggests that both branches could be executed in parallel. Wait, that doesn't make sense because the condition is either/or. Maybe the example is a bit conflicting here. Let me recheck the pipeline description.

The pipeline steps say: conditional branch execution where if score >=95%, production_load runs; else quarantine_and_alert runs, followed by send_alert_email. So the send_alert_email is sequential after quarantine_and_alert. Then both branches (production_load and quarantine_and_alert -> send_alert_email) converge to cleanup.

So the flow after quality_check branches into two possible paths. Only one path is taken based on the quality score. However, the orchestration mentions "fan-out" to two parallel paths. That's confusing. Wait, maybe the description is that after quality_check, both branches are possible, but only one is executed. So perhaps in Prefect, the tasks are conditional but the structure allows for both paths, but only one is taken. So in the flow, after getting the quality score, we check the condition and decide which tasks to run. Then, both paths need to converge to cleanup.

So the flow would look like:

ingest_csv → quality_check → (either production_load or quarantine_and_alert → send_alert_email) → cleanup.

But how to structure this in Prefect. Since the tasks are conditional, perhaps using if statements in the flow. Also, the cleanup task should run after both possible paths. So in Prefect, the flow would have:

After quality_check, check the score. If >=95, run production_load. Else, run quarantine_and_alert and then send_alert_email. Then, regardless of the path, run cleanup.

But how to ensure that cleanup runs after both possible branches. In Prefect, tasks are tracked via their dependencies. So if cleanup depends on both production_load and send_alert_email, but production_load and send_alert_email are in mutually exclusive branches, then Prefect would handle that by only waiting for the executed tasks. Wait, but if one branch isn't executed, would Prefect know to skip it? Or do we need to structure the dependencies properly.

Alternatively, perhaps using a merge of the two possible paths. For example, after the conditional branches, have a dummy task that both branches connect to, then cleanup depends on that. But maybe in Prefect, if cleanup is called after both branches, it will wait for whichever tasks were executed.

Wait, Prefect's dependency resolution is based on the execution path. So if in the flow function, the code is structured such that cleanup is called after both branches, but only one branch is taken, then Prefect will track the dependencies correctly. For example:

In the flow:

data, metadata = ingest_csv()
score, passed = quality_check(data)
if passed:
    prod_result = production_load(metadata)
else:
    quarantine_result = quarantine_and_alert(metadata)
    alert_result = send_alert_email(quarantine_result)
cleanup(metadata)

In this case, cleanup is called after the if-else block. So regardless of which path is taken, cleanup will run after the previous tasks. But how does Prefect track the dependencies? Because in the code, cleanup is called after the if-else, so it should depend on the last task in each branch. Wait, but in the else case, send_alert_email is called after quarantine_and_alert, so alert_result is the result of that. Then cleanup is called with metadata. But maybe the dependencies are not properly captured here. Because in the if case, cleanup depends on production_load, and in the else case, it depends on send_alert_email. But in the code as written, cleanup is called unconditionally after the if-else, but its argument is metadata, which comes from ingest_csv. So maybe the dependencies are not correctly set up. Because cleanup would only depend on ingest_csv, not on the tasks in the conditional branches.

Ah, that's a problem. So the cleanup task needs to run after both the conditional branches (whichever was executed) and after ingest_csv. But in the current code structure, cleanup is called with metadata, which is from ingest_csv. So Prefect would see that cleanup depends on ingest_csv, but not on the conditional tasks. Therefore, cleanup might run before the conditional tasks complete.

To fix this, the cleanup task should receive the results of the conditional tasks. But since the conditional tasks are in different branches, perhaps we need to pass the results of those tasks to cleanup. However, in the code, if one branch is taken, the other's task isn't executed, so passing their results would cause issues.

Alternatively, we can structure the code so that both branches return a result that is passed to cleanup. For example:

In the flow:

data, metadata = ingest_csv()
score, passed = quality_check(data)
if passed:
    prod_meta = production_load(metadata)
    cleanup(prod_meta)
else:
    quarantine_meta = quarantine_and_alert(metadata)
    alert_meta = send_alert_email(quarantine_meta)
    cleanup(alert_meta)

But this would result in cleanup being called in both branches, which might not be desired. Also, the cleanup function would need to handle different types of inputs. Alternatively, perhaps the cleanup task should depend on all possible previous tasks. But how?

Another approach is to have the cleanup task depend on a conditional task that runs after both branches. Wait, perhaps using a Prefect feature like creating a list of futures or using the wait= parameter. But I'm not sure.

Alternatively, in the flow, after the conditional branches, collect all the possible results and pass them to cleanup. For example:

data, metadata = ingest_csv()
score, passed = quality_check(data)
if passed:
    prod_result = production_load.submit(metadata)
    cleanup(prod_result, metadata)
else:
    quarantine_result = quarantine_and_alert.submit(metadata)
    alert_result = send_alert_email.submit(quarantine_result)
    cleanup(quarantine_result, alert_result, metadata)

But this complicates the cleanup function's parameters. Alternatively, maybe the cleanup task doesn't need the results of the previous tasks, but just needs to run after them. So how to ensure that cleanup runs after the conditional tasks?

In Prefect, when you call a task within a flow, the flow tracks the task as a dependency. So if you have:

data, metadata = ingest_csv()
score, passed = quality_check(data)
if passed:
    prod_result = production_load(metadata)
else:
    quarantine_result = quarantine_and_alert(metadata)
    alert_result = send_alert_email(quarantine_result)
cleanup(metadata)

In this case, the cleanup task is called after the if-else block. So in the case where passed is True, prod_result is created, and cleanup runs after that. If passed is False, quarantine_result and alert_result are created, and cleanup runs after those. But since in the code, cleanup is called after the if-else block, Prefect will wait for all tasks that were executed in the if or else block before running cleanup. Because the flow is procedural, the cleanup is called after the previous steps, so Prefect knows to wait for any tasks that were called before it in the flow function.

Wait, but in Prefect 2, the flow is dynamic. So the dependencies are determined by the order of function calls in the flow. So if in the flow function, you call task A, then task B, then task C, Prefect knows that C depends on B, which depends on A. But if within the flow, you have conditional branches, Prefect tracks the tasks that are actually executed and their dependencies.

So in the code example above, if passed is True, then production_load is called, and then cleanup is called. So cleanup depends on production_load. If passed is False, quarantine_and_alert and send_alert_email are called, then cleanup depends on send_alert_email. But in both cases, cleanup is also called with metadata from ingest_csv, which is a task. So cleanup would depend on both ingest_csv and whichever tasks were executed in the conditional branches.

Wait, no. Because in Prefect, each time you call a task in the flow, it creates a dependency on the previous tasks that are required to compute its inputs. So in the case where passed is True:

production_load is called with metadata (from ingest_csv). So production_load depends on ingest_csv. Then cleanup is called with metadata (from ingest_csv). So cleanup depends on production_load (since it's called after) and on ingest_csv (since metadata is an input). Wait, no. The order of execution in the flow function matters. So cleanup is called after production_load, so Prefect knows that cleanup should run after production_load. Additionally, since cleanup takes metadata as an argument, which is the output of ingest_csv, Prefect knows that cleanup depends on ingest_csv. But since production_load is called before cleanup, Prefect will ensure that production_load completes before cleanup starts.

Similarly, in the else case: quarantine_and_alert is called with metadata (depends on ingest_csv), then send_alert_email is called with the result of quarantine_and_alert, then cleanup is called with metadata. So cleanup depends on send_alert_email (since it's called after) and on ingest_csv (because of metadata). So in both cases, cleanup will wait for the previous tasks in the conditional branches.

Therefore, structuring the code as:

data, metadata = ingest_csv()
score, passed = quality_check(data)
if passed:
    production_load(metadata)
else:
    quarantine_result = quarantine_and_alert(metadata)
    send_alert_email(quarantine_result)
cleanup(metadata)

This should correctly set up the dependencies. Because cleanup is called after the if-else block, Prefect will wait for whichever tasks were executed in the if or else block before running cleanup. Additionally, since cleanup takes metadata as an argument, it also depends on ingest_csv. But since ingest_csv is the first task, it's already completed before any other tasks.

Wait, but in the code above, the 'data' from ingest_csv is passed to quality_check, which is a task. So quality_check depends on ingest_csv. Then, the conditional tasks depend on quality_check's result. So the dependencies are:

ingest_csv → quality_check → (production_load or quarantine_and_alert → send_alert_email) → cleanup.

So cleanup depends on the last task in whichever branch is taken, as well as ingest_csv.

But in the code, when you call cleanup(metadata), which is the output of ingest_csv, Prefect will create a dependency between cleanup and ingest_csv. But since the code calls cleanup after the conditional tasks, Prefect also knows that cleanup should run after those tasks. So the dependencies are correctly captured.

Now, for the parallel steps: the description mentions that the two paths (production_load and quarantine_and_alert) are parallel. But since they are mutually exclusive, perhaps they are not actually parallel. However, the orchestration says "fan-out from quality_check to two parallel paths (maximum width: 2)". This is confusing. Maybe the idea is that both paths are defined but only one is executed. Or perhaps the tasks can be submitted in parallel but only one will actually run. But that doesn't make sense. Alternatively, maybe the example expects that both branches are submitted asynchronously, but in reality, only one is executed based on the condition. But that's not possible. So perhaps the fan-out refers to the fact that there are two possible branches, but they are not executed in parallel. The term "parallel" here might be a misnomer.

Alternatively, perhaps the quarantine_and_alert and send_alert_email are sequential, but production_load and quarantine_and_alert are in parallel. But since they are mutually exclusive, only one path is taken. So maybe the fan-out is logical, not in execution.

In any case, according to the problem statement, for parallel steps, use .submit() to run tasks concurrently. But in this case, the steps are not parallel; they are conditional. So perhaps the production_load and quarantine_and_alert are not parallel. However, the send_alert_email comes after quarantine_and_alert, so those are sequential.

Wait, the pipeline description says: "fan-out from quality_check to two parallel paths (maximum width: 2)". So after quality_check, two paths are possible, and they are executed in parallel. But that contradicts the conditional logic. Unless the quality_check returns a boolean, and both branches are executed conditionally but in parallel. That doesn't make sense. So perhaps the user made a mistake in the description, and the fan-out refers to the two possible branches, not actual parallel execution.

Given this confusion, perhaps the correct approach is to model the conditional branches using normal task calls (not .submit()), since they are not meant to be executed in parallel. However, the problem statement says for parallel steps to use .submit(). So if the steps are meant to be parallel, but in reality, they are conditional, perhaps the .submit() is not needed here. Alternatively, maybe the two branches (production_load and quarantine_and_alert) are meant to be submitted in parallel, but only one is actually executed based on the condition. But that's not possible. So perhaps the problem statement's mention of parallel paths is incorrect, and the correct approach is to use normal task calls with conditional logic.

Given that, the code would structure the flow with normal task calls, using if-else to determine which path to take. Then, the send_alert_email task is called after quarantine_and_alert in the else branch.

Now, regarding retries: the pipeline requires single retry with 5-minute delay on failure. So each task should have a retry configuration. In Prefect, this can be done with the @task decorator parameters, like retries and retry_delay_seconds.

So each @task should have retries=1 and retry_delay_seconds=300 (5 minutes). But the problem says "single retry with 5-minute delay on failure". So retries=1, retry_delay_seconds=300.

Now, for the tasks:

ingest_csv: reads CSV, returns data and metadata.

quality_check: calculates score, returns score and a boolean (passed or not).

production_load: takes metadata, loads to production DB.

quarantine_and_alert: takes metadata, moves to quarantine.

send_alert_email: takes some input (maybe the result from quarantine), sends email.

cleanup: performs cleanup.

Now, the data flow:

ingest_csv returns data and metadata. The data is passed to quality_check. The metadata is passed to production_load or quarantine_and_alert.

But in the code, perhaps the data is a DataFrame, and metadata is a dictionary with file info.

The quality_check task returns a score and a boolean (whether it passed the threshold). Then, based on that, the flow decides which path to take.

Now, implementing this in Prefect:

Each task is decorated with @task, and the flow is @flow.

The flow function would be called something like data_quality_gate_flow.

Now, the code structure:

from prefect import flow, task
from typing import Tuple
import pandas as pd

@task(retries=1, retry_delay_seconds=300)
def ingest_csv() -> Tuple[pd.DataFrame, dict]:
    # Implementation here
    pass

@task(retries=1, retry_delay_seconds=300)
def quality_check(data: pd.DataFrame) -> Tuple[float, bool]:
    # Calculate score
    pass

@task(retries=1, retry_delay_seconds=300)
def production_load(metadata: dict) -> dict:
    pass

@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(metadata: dict) -> dict:
    pass

@task(retries=1, retry_delay_seconds=300)
def send_alert_email(quarantine_result: dict) -> dict:
    pass

@task(retries=1, retry_delay_seconds=300)
def cleanup(metadata: dict) -> None:
    pass

@flow
def data_quality_gate_flow():
    data, metadata = ingest_csv()
    score, passed = quality_check(data)
    if passed:
        production_load(metadata)
    else:
        quarantine_result = quarantine_and_alert(metadata)
        send_alert_email(quarantine_result)
    cleanup(metadata)

if __name__ == '__main__':
    data_quality_gate_flow()

But wait, the cleanup task is called after the if-else block. So in the case where passed is True, cleanup runs after production_load. In the else case, cleanup runs after send_alert_email. Also, since cleanup takes metadata from ingest_csv, it depends on ingest_csv. But Prefect will track the dependencies correctly because the flow is procedural.

Now, the problem mentions that the cleanup should converge from both paths. So regardless of which path is taken, cleanup should run after all previous tasks. The code above achieves this because cleanup is called after the conditional branches.

Regarding scheduling: the pipeline has a daily execution schedule. The user should include a comment about deployment/schedule configuration. So adding a comment like:

# Deployment configuration with schedule can be added here

But according to the problem statement, it's optional. So including a brief comment.

Now, the tasks need to have parameters. For example, ingest_csv might take a file path, but the problem says data source is local file system. So perhaps the code uses a default path, but in reality, parameters should be passed. However, the problem says to avoid placeholders and use minimal safe defaults. So perhaps the ingest_csv task reads from a default path, or the flow takes parameters.

But the problem says to make the code executable. So perhaps the functions have minimal implementation with pass statements, but the code structure is correct.

Wait, but the code must be executable. So the functions can't have pass statements. Need to have at least minimal code that doesn't raise errors. For example, returning dummy data.

For example, in ingest_csv:

@task(retries=1, retry_delay_seconds=300)
def ingest_csv() -> Tuple[pd.DataFrame, dict]:
    # Example implementation
    data = pd.DataFrame({'customer_id': [1, 2], 'name': ['Alice', 'Bob']})
    metadata = {'path': '/data/customers.csv', 'size': 1024}
    return data, metadata

Similarly, other tasks can have minimal implementations.

But the user might want the code to be executable, so including minimal implementations.

Putting it all together:

The code will have all tasks with retries, the flow function with conditional logic, and the cleanup task called after the branches.

Also, the problem mentions that send_alert_email is followed by cleanup. So in the else clause, quarantine_and_alert runs, then send_alert_email, then cleanup.

But in the code, cleanup is called outside the else block, so it runs after both branches. So the dependencies are correctly captured.

Now, testing if this code meets all constraints:

- Uses Prefect 2.x APIs: yes, @flow and @task.

- Each step is a @task, single @flow orchestrates them in order.

- For parallel steps: but in this case, the steps are conditional, not parallel. So no .submit() is used here. However, the problem says for parallel steps to use .submit(). But according to the pipeline description, the fan-out is from quality_check to two parallel paths. But since they are conditional, perhaps the intended approach is to submit both tasks but only one runs. That doesn't make sense. Alternatively, maybe the tasks are submitted asynchronously, but the condition determines which one to wait for. But that's not how Prefect works.

Alternatively, perhaps the two branches (production_load and quarantine_and_alert) are submitted in parallel, and the flow continues once either completes. But that's not possible with a condition. So perhaps the problem statement's mention of parallel paths is incorrect, and the correct approach is to use normal task calls without .submit().

Given that, the code above is correct.

The code includes the if __name__ == '__main__' guard.

The code is PEP 8 compliant, uses clear names, explicit dependencies.

The send_alert_email task depends on quarantine_and_alert, which depends on quality_check, etc.

The cleanup task is called after the conditional branches, so it depends on whichever tasks were executed.

Now, the problem mentions that the pipeline has a single retry with 5-minute delay on failure. Each task is decorated with retries=1 and retry_delay_seconds=300.

Thus, the code meets all constraints.
</think>

from prefect import flow, task
from typing import Tuple
import pandas as pd

@task(retries=1, retry_delay_seconds=300)
def ingest_csv() -> Tuple[pd.DataFrame, dict]:
    """Ingest raw customer CSV data from source location."""
    # Example implementation - replace with actual data loading
    data = pd.DataFrame({'customer_id': [1, 2], 'name': ['Alice', 'Bob']})
    metadata = {'path': '/data/customers.csv', 'size': 1024}
    return data, metadata

@task(retries=1, retry_delay_seconds=300)
def quality_check(data: pd.DataFrame) -> Tuple[float, bool]:
    """Calculate data quality score and determine pass/fail status."""
    # Example quality calculation - replace with actual metrics
    completeness = 1.0 if not data.empty else 0.0
    validity = 0.95  # Example validity score
    total_score = (completeness + validity) / 2
    return total_score, total_score >= 0.95

@task(retries=1, retry_delay_seconds=300)
def production_load(metadata: dict) -> dict:
    """Load validated data to production database."""
    # Example implementation - replace with actual database load
    print(f"Loading data from {metadata['path']} to production database")
    return {'status': 'loaded', **metadata}

@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(metadata: dict) -> dict:
    """Move low-quality data to quarantine storage."""
    # Example implementation - replace with actual quarantine logic
    print(f"Moving data from {metadata['path']} to quarantine")
    return {'status': 'quarantined', **metadata}

@task(retries=1, retry_delay_seconds=300)
def send_alert_email(quarantine_result: dict) -> dict:
    """Notify data stewards about quarantined data."""
    # Example implementation - replace with actual email integration
    print(f"Sending alert for quarantined file: {quarantine_result['path']}")
    return {'alert_sent': True, **quarantine_result}

@task(retries=1, retry_delay_seconds=300)
def cleanup(metadata: dict) -> None:
    """Perform final cleanup operations."""
    # Example implementation - replace with actual cleanup logic
    print(f"Cleaning up temporary files for {metadata['path']}")

@flow
def data_quality_gate_flow():
    """Main data quality assessment and routing flow."""
    # Deployment configuration with daily schedule would be added here
    data, metadata = ingest_csv()
    score, passed = quality_check(data)
    
    if passed:
        production_load(metadata)
    else:
        quarantine_result = quarantine_and_alert(metadata)
        alert_result = send_alert_email(quarantine_result)
    
    cleanup(metadata)

if __name__ == '__main__':
    data_quality_gate_flow()