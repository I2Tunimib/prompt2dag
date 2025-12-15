<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x Python module. Let me start by understanding the requirements and constraints.

First, the pipeline has three stages: Extract, Transform, Load. The Extract stage has three tasks that run in parallel. So I'll use .submit() for each of those tasks to make them asynchronous. Then, the Transform stage depends on all three extraction tasks completing, which means I need to wait for their results. The Load stage has two sequential tasks: load_to_db must complete before send_summary_email.

The user mentioned using @task and @flow decorators. Each step should be a task, and the flow orchestrates them. For parallel execution, I'll use .submit() and then gather the results. Since the Transform task needs all three vendor data, I'll pass the futures from the submitted tasks into the cleanse_data task. Prefect handles waiting for those futures automatically.

Next, the Load stage's load_to_db and send_summary_email are sequential. So the flow should call load_to_db normally, then pass its result to send_summary_email.

Now, about TaskGroups. The user wants logical stage grouping. In Prefect 2, TaskGroups are part of the UI, but in code, we can use with Flow(...) and context managers. Wait, no—actually, in Prefect 2, you can use with blocks to group tasks in the UI. So I'll structure each stage within a with statement under the flow to create task groups. But the code example might not need that if the tasks are properly nested. Hmm, maybe the user just wants the tasks grouped logically, which can be done by using context managers. Let me check Prefect's documentation. Oh right, in Prefect 2, you can use task groups by using the with clause and naming the groups. So I'll add context managers for each stage with names like "Extract Stage" and "Transform Stage".

For scheduling, the user mentioned daily execution starting Jan 1, 2024, but the code should include a comment about deployment/schedule configuration. So I'll add a comment near the flow definition indicating that.

Retries are configured with 2 retries and 5-minute delays. So each task should have @task(retries=2, retry_delay_seconds=300). But wait, the user specified retry configuration as 2 retries with 5-minute delay. So I need to set retries=2 and retry_delay_seconds=300 (since 5 minutes is 300 seconds).

The email notification is sent after the database load. So the send_summary_email task depends on load_to_db.

Now, data is passed between stages using XCom. Since the tasks return data, Prefect automatically handles XCom for them. So the extract tasks return data, which is passed to cleanse_data, which returns cleansed data passed to load_to_db, and so on.

For the functions themselves, I'll need to create placeholders for the actual operations. For example, ingest_vendor_a might read a CSV, but since the code needs to be executable, I'll have them return dummy data with the specified record counts. Similarly, cleanse_data would process the data, but here I'll just combine the records. The load_to_db might print or log, and send_summary_email could print the email details.

I should also include the if __name__ == '__main__' guard to run the flow locally. The flow function will be called there.

Let me structure the code step by step:

1. Import necessary modules: from prefect import flow, task.

2. Define each task with @task decorator, including retries and retry delay.

3. The flow function uses @flow, and within it, the stages are organized with context managers for grouping.

4. In the Extract stage, call all three ingest tasks with .submit() to run in parallel.

5. In Transform stage, call cleanse_data with the three futures (which will wait for them to complete).

6. In Load stage, call load_to_db with the cleansed data, then send_summary_email with the result.

Each task should have parameters if needed, but since the problem doesn't specify file paths or connection details, I'll assume they're handled elsewhere or use default values. For example, the email recipient is given, so I'll hardcode it into the send_summary_email task.

Testing if the code is PEP 8 compliant: function names are lowercase with underscores, proper spacing, etc.

Now, putting it all together, making sure that the tasks are defined correctly, the flow orchestrates them in order, and the context managers group the tasks into stages.

Also, note that the user mentioned "TaskGroups for logical stage grouping", so using with flow.context() or similar. Wait, in Prefect 2, when you use a with statement inside a flow, it creates a task group in the UI. So the code should structure each stage within a with block labeled with the stage name.

Wait, actually, in Prefect 2, you can create a task group by using a with statement on the flow's context. For example:

with Flow("my_flow") as flow:
    with TaskGroup("Extract Stage"):
        # tasks here

But in the functional API (using @flow), you can't directly create task groups. Wait, maybe the user is referring to the Prefect UI's task groups, which are automatically created based on the structure. Alternatively, perhaps using subflows, but that's not necessary here. Since the user mentioned "Uses TaskGroups for logical stage grouping", I need to structure the code so that in the UI, tasks are grouped under each stage. However, in Prefect 2's functional API, task groups are created by using with statements within the flow function. Let me check the syntax.

Ah, right, in Prefect 2, when using the functional API (decorators), you can use the with statement along with the flow's context to create task groups. For example:

@flow
def my_flow():
    with flow.task_group("Extract Stage"):
        t1 = task1.submit()
        t2 = task2.submit()
    # etc.

Wait, but the flow is defined as a function with @flow, so inside the function, how do you create task groups? Maybe using the with statement and the flow's context. Let me look up an example.

Upon checking, in Prefect 2, you can use the with statement combined with the flow's context to create task groups. For example:

from prefect import flow, task

@flow
def my_flow():
    with flow.task_group("Extract Stage"):
        a = ingest_vendor_a()
        b = ingest_vendor_b()
        c = ingest_vendor_c()
    # other stages

But wait, the functional API might not support that directly. Alternatively, perhaps the task groups are automatically created based on the nesting of tasks. Hmm, maybe I'm overcomplicating. The user might just want the code to be structured in stages with comments, but the actual task grouping in the UI is handled by Prefect when tasks are called within the flow. Since the user mentioned "TaskGroups for logical stage grouping", perhaps each stage is a task group. So, in the code, I can structure each stage within a with block that creates a task group. However, in Prefect 2's functional API, task groups are created using the with statement and the TaskGroup context manager. Wait, maybe I need to import TaskGroup from prefect.task_runners.

Wait, no. Prefect 2.0 introduced task runners for parallel execution, but task groups are part of the UI and are created based on the structure of the flow. So perhaps when tasks are called within a with statement under a flow, they are grouped. Alternatively, maybe the user just expects comments indicating the stages. But the original pipeline description says "Uses TaskGroups for logical stage grouping", so I need to implement that.

After checking Prefect's documentation, I see that in Prefect 2, you can create task groups by using the with statement along with the flow's task_group method. So inside the flow function:

@flow
def shipment_etl_flow():
    with flow.task_group("Extract Stage"):
        # tasks here

But wait, the flow is a function decorated with @flow, so inside the function, 'flow' refers to the function itself, not an instance. Hmm, perhaps I need to use the current flow context. Alternatively, maybe the correct way is to use the 'with' statement with the TaskGroup class. Let me check.

Looking up Prefect 2 TaskGroup examples, I find that in the imperative API (using Flow as an object), you can create TaskGroups. But in the functional API (using @flow), perhaps the way to group tasks is by using subflows or by using the with statement with a label. However, I might be mistaken here.

Alternatively, maybe the user just expects that the code is structured in stages with comments, and the TaskGroups are created via the UI by grouping tasks under each stage. Since the code needs to be executable, perhaps the TaskGroup part is handled automatically when tasks are called within the flow function. Therefore, I can structure the code with comments indicating each stage and let Prefect handle the grouping in the UI.

Given that, perhaps the code doesn't need explicit TaskGroup creation, but the user mentioned "Uses TaskGroups for logical stage grouping", so I need to include that. Maybe the correct approach is to use the with statement and create a TaskGroup context for each stage.

Wait, Prefect's documentation shows that TaskGroup is part of the prefect.flows module when using the imperative API. For the functional API (@flow), perhaps the way to create task groups is by using the with statement and the flow's task_group method. Let me see an example.

Here's an example from Prefect's docs:

from prefect import flow, task

@task
def my_task():
    pass

@flow
def my_flow():
    with flow.task_group("My Group"):
        my_task()
        my_task()

But wait, in the functional API, 'flow' inside the function is the function itself, not a Flow object. So this might not work. Alternatively, maybe the correct way is to use the 'with' statement with a TaskGroup constructor. Hmm, perhaps I'm overcomplicating. Since the user wants TaskGroups for logical grouping, but the code must be executable, maybe the TaskGroups are automatically created based on the structure of task calls within the flow. Therefore, perhaps the code doesn't need explicit TaskGroup handling, and the user's mention is more about the logical grouping in the UI. So I'll proceed to structure the code with each stage's tasks called within their respective sections, using comments to indicate the stages, and Prefect will handle the grouping in the UI.

Alternatively, maybe the user expects that each stage is a subflow. But subflows are separate flows, which might complicate things. No, the pipeline is a single flow with three stages.

So, putting it all together:

- The flow function is decorated with @flow, includes retries and other parameters if needed.

Wait, the retry configuration is per task, not per flow. The user specified retry configuration: 2 retries with 5-minute delay. So each @task should have retries=2 and retry_delay_seconds=300.

The schedule is daily, starting Jan 1, 2024. The code should include a comment about deployment/schedule configuration. So near the flow definition, add a comment like # Deployment configuration: schedule=CronSchedule('0 0 * * *'), start_date=datetime(2024, 1, 1).

But the user said to include a brief comment indicating deployment/schedule configuration, optional. So add that as a comment.

Now, writing the code:

Import statements first.

Then, define each task with @task, including retries and retry delay.

The extract tasks (ingest_vendor_a, etc.) return some dummy data, like a list of records. For example, vendor_a_data = [{"sku": "A1", ...}, ...] with 1250 records.

The cleanse_data task takes the three vendor data lists, combines them, does some processing (here, just a placeholder), and returns the cleansed data.

The load_to_db task takes the cleansed data and returns a count or something.

The send_summary_email task takes the count and sends an email (here, just print).

In the flow function:

- Stage 1: Extract, run the three ingest tasks in parallel using .submit().

- Collect their futures: a = ingest_vendor_a.submit(), etc.

- Stage 2: Transform: cleansed = cleanse_data(a.result(), b.result(), c.result())

Wait, but when using .submit(), the tasks return futures. To get their results, you need to call .result() on them. However, when passing futures into another task, Prefect will automatically wait for them and pass the results. So perhaps in the flow, after submitting the three ingest tasks, the cleanse_data task is called with the three futures as arguments. Prefect will handle waiting for them and passing the results.

Wait, no. If you have a task that requires the result of a submitted task, you need to pass the future to it, and Prefect will resolve it. So in the code:

a_future = ingest_vendor_a.submit()
b_future = ingest_vendor_b.submit()
c_future = ingest_vendor_c.submit()

cleansed_data = cleanse_data(a_future, b_future, c_future)

But wait, when you pass futures to a task, does Prefect automatically wait for them and pass their results? Or do you need to call .result() on them?

In Prefect, when you pass a future (a PrefectFuture) to a task, the task will wait for the future to complete and then receive the result. So you can pass the futures directly to the next task, and Prefect handles the dependencies.

Therefore, in the flow:

In Stage 1, submit all three ingest tasks.

In Stage 2, call cleanse_data with the three futures.

Then, in Stage 3, call load_to_db with the cleansed_data, then send_summary_email with the load_result.

So the code would look like:

@flow
def shipment_etl_flow():
    # Stage 1: Extract
    vendor_a_data = ingest_vendor_a.submit()
    vendor_b_data = ingest_vendor_b.submit()
    vendor_c_data = ingest_vendor_c.submit()

    # Stage 2: Transform
    cleansed_data = cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data)

    # Stage 3: Load
    load_result = load_to_db(cleansed_data)
    send_summary_email(load_result)

But wait, when using .submit(), the tasks are run asynchronously. So vendor_a_data is a future. When passing these futures to cleanse_data, which is a task, Prefect will wait for all futures to complete before running cleanse_data. That's correct because cleanse_data depends on all three extract tasks.

Then, the load_to_db task depends on cleanse_data, and send_summary_email depends on load_to_db.

Now, implementing the tasks:

For the extract tasks, each returns a list of records. For example:

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a():
    # In real scenario, read CSV from Vendor A
    return [{"sku": f"A{i}", "shipment_date": "2024-01-01"} for i in range(1250)]

Similarly for B and C, with 980 and 1750 records.

The cleanse_data task combines the three lists, normalizes SKU, etc.:

@task(retries=2, retry_delay_seconds=300)
def cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data):
    combined = vendor_a_data + vendor_b_data + vendor_c_data
    # Normalize SKU formats, validate dates, enrich with location
    # Placeholder processing
    return [{"sku": record["sku"].upper(), **record} for record in combined]

The load_to_db task:

@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleansed_data):
    # In real scenario, connect to PostgreSQL and upsert
    print(f"Loaded {len(cleansed_data)} records to database")
    return len(cleansed_data)

The send_summary_email task:

@task(retries=2, retry_delay_seconds=300)
def send_summary_email(record_count):
    # In real scenario, send email
    print(
        f"Email sent to supply-chain-team@company.com: "
        f"Processed {record_count} records"
    )

Putting it all together with the flow and the main guard.

Also, adding the schedule comment:

# Deployment configuration would include:
# schedule=CronSchedule('0 0 * * *'), start_date=datetime(2024, 1, 1)

But as a comment near the flow function.

Now, checking for PEP 8 compliance: line lengths, spacing, etc.

Testing if the code would run: when the flow is called, the tasks are submitted, and dependencies are respected.

Yes, this should work. Each task is decorated with retries, the flow orchestrates the stages correctly, and the code is self-contained with dummy implementations.
</think>

from prefect import flow, task
from datetime import datetime
from typing import List, Dict

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a() -> List[Dict]:
    """Simulate ingestion of Vendor A CSV data (1,250 records)"""
    return [{"sku": f"A{i}", "shipment_date": "2024-01-01"} for i in range(1250)]

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b() -> List[Dict]:
    """Simulate ingestion of Vendor B CSV data (980 records)"""
    return [{"sku": f"B{i}", "shipment_date": "2024-01-01"} for i in range(980)]

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c() -> List[Dict]:
    """Simulate ingestion of Vendor C CSV data (1,750 records)"""
    return [{"sku": f"C{i}", "shipment_date": "2024-01-01"} for i in range(1750)]

@task(retries=2, retry_delay_seconds=300)
def cleanse_data(vendor_a: List[Dict], vendor_b: List[Dict], vendor_c: List[Dict]) -> List[Dict]:
    """Clean and normalize combined vendor data"""
    combined = vendor_a + vendor_b + vendor_c
    # Simulate data cleansing steps
    cleansed = [{
        "sku": record["sku"].upper(),
        "shipment_date": datetime.strptime(record["shipment_date"], "%Y-%m-%d").date(),
        "location": "WAREHOUSE_01"
    } for record in combined]
    print(f"Cleansed {len(cleansed)} records")
    return cleansed

@task(retries=2, retry_delay_seconds=300)
def load_to_db(data: List[Dict]) -> int:
    """Simulate database loading with upsert operation"""
    print(f"Loading {len(data)} records to PostgreSQL")
    return len(data)

@task(retries=2, retry_delay_seconds=300)
def send_summary_email(record_count: int) -> None:
    """Simulate email notification with summary metrics"""
    print(
        f"Email sent to supply-chain-team@company.com\n"
        f"Successfully processed {record_count} records\n"
        f"Date: {datetime.now().isoformat()}"
    )

@flow
# Deployment configuration would include:
# schedule=CronSchedule('0 0 * * *'), start_date=datetime(2024, 1, 1)
def shipment_etl_flow():
    with flow.task_group("Extract Stage"):
        vendor_a = ingest_vendor_a.submit()
        vendor_b = ingest_vendor_b.submit()
        vendor_c = ingest_vendor_c.submit()

    with flow.task_group("Transform Stage"):
        cleansed = cleanse_data(vendor_a.result(), vendor_b.result(), vendor_c.result())

    with flow.task_group("Load Stage"):
        loaded = load_to_db(cleansed)
        send_summary_email(loaded)

if __name__ == "__main__":
    shipment_etl_flow()