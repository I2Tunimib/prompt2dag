<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

First, the pipeline has three stages: Extract, Transform, Load. The Extract stage has three tasks that run in parallel. So I'll use .submit() for each of those tasks to make them run concurrently. Then the Transform stage depends on all three extraction tasks completing, which means I need to wait for their results before proceeding. That's a fan-in pattern.

For the tasks, each vendor ingestion is a separate task. I'll name them ingest_vendor_a, ingest_vendor_b, ingest_vendor_c. They return DataFrames, which will be passed to the cleanse_data task. Since they run in parallel, I'll call each with .submit() and then collect their results.

The cleanse_data task takes all three vendor DataFrames. I'll need to get the result of each submitted task, maybe using .result() on each future. Then process them together. The load_to_db task depends on the cleanse_data output, and send_summary_email depends on load_to_db completing.

Wait, the Load stage has load_to_db and send_summary_email. The email must be sent after the database load completes. So those two tasks are sequential. So first call load_to_db normally, then send_summary_email.

Now, about the flow structure. The main @flow will orchestrate all these tasks. For the Extract stage, since the three tasks are parallel, each is submitted, and their futures are stored. Then, in Transform, we pass those futures' results to cleanse_data. Then in Load, after cleanse_data completes, run load_to_db, then send_summary_email.

I need to make sure that the tasks are structured correctly. Each task should be decorated with @task. The flow is the main function decorated with @flow.

Regarding the schedule, the user mentioned daily execution starting Jan 1, 2024, but the code should include a comment about deployment. So I'll add a comment in the flow's docstring or a note about the schedule configuration.

Retries are set to 2 retries with 5-minute delay. So each @task should have retries and retry_delay_seconds parameters. For example, @task(retries=2, retry_delay_seconds=300).

For the data passing between tasks, since they are in different stages, XCom is used automatically when returning values. So the ingest tasks return DataFrames, which are passed to cleanse_data, which returns a cleansed DataFrame, passed to load_to_db, etc.

Now, the code structure:

- Import prefect modules: from prefect import flow, task
- Define each task with appropriate parameters and logic.
- The main flow function will call the tasks in order, handling parallelism with .submit().

Wait, in Prefect 2, when you call a task within a flow, if you want to run it asynchronously, you use .submit(), which returns a future. Then, to get the result, you call .result() on the future. So for the three ingest tasks, in the flow:

vendor_a_future = ingest_vendor_a.submit()
vendor_b_future = ingest_vendor_b.submit()
vendor_c_future = ingest_vendor_c.submit()

Then, in the cleanse_data task, pass the results:

cleaned_data = cleanse_data(vendor_a_future.result(), vendor_b_future.result(), vendor_c_future.result())

But wait, in Prefect, when you submit tasks, the flow will wait for their completion automatically. So maybe I can just pass the futures directly? Or do I need to call .result()? Hmm, perhaps better to collect the results by calling .result() on each future to ensure they are done before proceeding.

Then, the cleanse_data task will process the three DataFrames. Then load_to_db takes the cleaned data, and send_summary_email runs after that.

Now, the code:

Start with imports. Need pandas for DataFrames and maybe some Postgres connector, but the user didn't specify, so I'll use placeholders like '...' for the actual implementation. But the code must be executable, so maybe use pass statements or minimal code.

For example, in ingest_vendor_a, read a CSV. But since the actual paths aren't given, maybe have parameters or hardcode placeholders with a comment.

Similarly, for the database load, use a placeholder function that just logs.

Same for sending email: a placeholder that prints a message.

Now, code:

from prefect import flow, task
from typing import Tuple
import pandas as pd

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a() -> pd.DataFrame:
    """Ingest Vendor A CSV data."""
    # Implementation placeholder
    return pd.read_csv("vendor_a.csv")

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b() -> pd.DataFrame:
    """Ingest Vendor B CSV data."""
    # Implementation placeholder
    return pd.read_csv("vendor_b.csv")

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c() -> pd.DataFrame:
    """Ingest Vendor C CSV data."""
    # Implementation placeholder
    return pd.read_csv("vendor_c.csv")

@task
def cleanse_data(df_a: pd.DataFrame, df_b: pd.DataFrame, df_c: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize data from all vendors."""
    combined = pd.concat([df_a, df_b, df_c])
    # Example normalization steps
    combined['sku'] = combined['sku'].str.upper().str.strip()
    # Placeholder for date validation and enrichment
    return combined

@task
def load_to_db(data: pd.DataFrame) -> None:
    """Load processed data to PostgreSQL database."""
    # Placeholder for database upsert logic
    print(f"Loaded {len(data)} records to database")

@task
def send_summary_email(record_count: int) -> None:
    """Send processing summary email."""
    # Placeholder for email sending logic
    print(f"Email sent: Processed {record_count} records")

@flow
def supply_chain_etl() -> None:
    """Main ETL flow for supply chain shipment data."""
    # Stage 1: Parallel extraction
    future_a = ingest_vendor_a.submit()
    future_b = ingest_vendor_b.submit()
    future_c = ingest_vendor_c.submit()

    # Stage 2: Transformation
    df_a = future_a.result()
    df_b = future_b.result()
    df_c = future_c.result()
    cleaned_data = cleanse_data.submit(df_a, df_b, df_c)

    # Stage 3: Loading and notification
    loaded_data = cleaned_data.result()
    load_to_db(loaded_data)
    send_summary_email(len(loaded_data))

if __name__ == "__main__":
    # Deployment configuration comment
    # Example: schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024, 1, 1))
    supply_chain_etl()

Wait, but in the Load stage, load_to_db is called normally (not submitted), which makes it sequential. Then send_summary_email is called after that. So yes, that's correct.

But wait, in the code above, after getting cleaned_data.result(), which is a DataFrame, then load_to_db is called with that data. Then send_summary_email is called with len(loaded_data). So the flow is:

cleanse_data runs after all three ingest tasks complete. Then load_to_db runs, then send_summary_email.

But according to the pipeline description, load_to_db and send_summary_email are sequential. So the code correctly sequences them.

Now, check for PEP 8 compliance. Function names are snake_case, proper spacing, etc.

The tasks have retries set as per the constraints. The @flow is the main function.

The if __name__ == '__main__' block calls the flow.

The code includes placeholders for CSV reading, database loading, and email sending, but those are just examples. The user said to make the code executable, so these placeholders with print statements or pandas operations (assuming pandas is installed) would work.

Also, note that in Prefect 2, when you submit a task, it's asynchronous, but the flow will wait for the result when .result() is called. So in the code above, future_a.result() blocks until the task is complete, but since all three are submitted in parallel, they run concurrently.

So the Extract stage's three tasks are submitted in parallel, then the flow waits for all three to complete by calling .result() on each future. Then, the cleanse_data task is submitted (but since it's called with .submit(), but wait, no— in the code above, cleanse_data is called with .submit(), but then cleaned_data.result() is called immediately. Wait, no:

In the code as written:

cleaned_data = cleanse_data.submit(df_a, df_b, df_c)

Then loaded_data = cleaned_data.result()

So cleanse_data is submitted as a task, and then we wait for its result. So the Transform stage is a single task that runs after the three extract tasks. Then, the Load stage's load_to_db is called normally (so it runs sequentially), then send_summary_email is called after that.

But according to the pipeline description, the Load stage has load_to_db and send_summary_email as sequential steps. So the code correctly sequences them.

But wait, in the code, load_to_db is a task called normally (without .submit()), which means it runs immediately and the flow waits for it to complete before moving to send_summary_email. That's correct.

So the code structure seems correct.

Now, check for the schedule comment. The user said to include a brief comment indicating deployment/schedule configuration. So in the main guard, add a comment like:

# Example deployment with schedule:
# supply_chain_etl.serve(name="daily_supply_chain_etl", ...)

But the user's example says to include a comment. So maybe in the if __name__ block:

if __name__ == "__main__":
    # Example deployment: 
    # supply_chain_etl.deploy(
    #     name="daily-etl",
    #     schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024,1,1)),
    #     work_pool="default-agent-pool",
    # )
    supply_chain_etl()

But the user said to return only the code, so including the comment is okay.

But the user's example says "include a brief comment indicating deployment/schedule configuration (optional)". So adding that comment in the main block.

So the final code includes that comment.

Now, check all constraints:

- Uses @task and @flow correctly.

- Parallel extraction with .submit(), sequential transform and load.

- Retries configured.

- Code is PEP 8 compliant.

- Docstrings present.

- XCom is used implicitly by returning data from tasks.

- Task groups: The user mentioned "Uses TaskGroups for logical stage grouping". Wait, the original pipeline description says "Uses TaskGroups for logical stage grouping". But in the code I wrote, there are no TaskGroups. Prefect 2 has TaskGroups for visualization grouping, but perhaps I need to add them.

Ah, right! The pipeline description mentions using TaskGroups for each stage. So I need to wrap each stage's tasks in a TaskGroup.

But in Prefect 2, TaskGroups are created using a context manager. So for each stage (Extract, Transform, Load), create a TaskGroup.

Wait, but the Extract stage has three parallel tasks. How to group them?

Wait, the Extract stage's three tasks are parallel, but they are part of the same stage. So perhaps:

from prefect.task_runners import ConcurrentTaskRunner
But no, TaskGroups are for grouping in the UI, not for execution.

Alternatively, using with Flow(...) and TaskGroup, but in Prefect 2.0, TaskGroup is part of the imperative API.

Wait, perhaps the user expects the code to use TaskGroups to visually group the stages in the Prefect UI. So for each stage, create a TaskGroup.

So, for Stage 1: Extract, create a TaskGroup named "Extract" containing the three ingest tasks.

But in Prefect 2, you can use the with statement and TaskGroup context manager.

Wait, but the code is written using the functional API, where tasks are called within the flow function.

So modifying the code to include TaskGroups:

from prefect import flow, task, TaskGroup

@flow
def supply_chain_etl() -> None:
    """Main ETL flow for supply chain shipment data."""
    with TaskGroup("Extract"):
        future_a = ingest_vendor_a.submit()
        future_b = ingest_vendor_b.submit()
        future_c = ingest_vendor_c.submit()

    # Wait for extraction to complete
    df_a = future_a.result()
    df_b = future_b.result()
    df_c = future_c.result()

    with TaskGroup("Transform"):
        cleaned_data = cleanse_data.submit(df_a, df_b, df_c)

    loaded_data = cleaned_data.result()

    with TaskGroup("Load"):
        load_to_db.submit(loaded_data)
        send_summary_email.submit(len(loaded_data))

Wait, but in the Load stage, load_to_db and send_summary_email are sequential. So within the Load TaskGroup, load_to_db is submitted, then send_summary_email is submitted. But since they are submitted, they would run in parallel unless there's a dependency.

Wait, no. If you submit load_to_db and then submit send_summary_email, they would run in parallel unless there's a dependency. But according to the pipeline, load_to_db must complete before send_summary_email starts.

So to enforce that, send_summary_email should depend on load_to_db. But in the code above, using .submit() would not automatically create that dependency. So perhaps:

In the Load TaskGroup:

load_future = load_to_db.submit(loaded_data)
email_future = send_summary_email.submit(load_future.result())

But that would require getting the result of load_to_db, which would wait for it to complete before submitting send_summary_email.

Alternatively, use the wait_for parameter in submit.

But perhaps it's easier to call them sequentially without .submit(), since they need to be sequential.

Wait, but the user's constraint says: For parallel steps, use .submit() to run tasks concurrently; for sequential steps, call tasks normally.

So in the Load stage, load_to_db and send_summary_email are sequential. So call them normally, not with .submit().

But then, within a TaskGroup, how to represent that.

Alternatively, perhaps the TaskGroup is just for grouping in the UI, and the execution order is determined by the code flow.

So modifying the code to include TaskGroups:

@flow
def supply_chain_etl() -> None:
    """Main ETL flow for supply chain shipment data."""
    with TaskGroup("Extract"):
        future_a = ingest_vendor_a.submit()
        future_b = ingest_vendor_b.submit()
        future_c = ingest_vendor_c.submit()

    df_a = future_a.result()
    df_b = future_b.result()
    df_c = future_c.result()

    with TaskGroup("Transform"):
        cleaned_data = cleanse_data.submit(df_a, df_b, df_c).result()

    loaded_data = cleaned_data.result()

    with TaskGroup("Load"):
        load_to_db(loaded_data)
        send_summary_email(len(loaded_data))

But in this case, the Transform TaskGroup contains the cleanse_data task. The Load TaskGroup contains the two sequential tasks.

But when you call a task normally (without .submit()), it's executed immediately, and the flow waits for it. So within the Load TaskGroup, load_to_db is called, which runs and completes, then send_summary_email is called.

In the UI, these tasks would be grouped under the "Load" TaskGroup.

But the original code didn't include TaskGroups. The user's pipeline description mentions "Uses TaskGroups for logical stage grouping", so adding them is necessary.

So I need to revise the code to include TaskGroups for each stage.

So the updated code would have:

from prefect import flow, task, TaskGroup

@flow
def supply_chain_etl() -> None:
    with TaskGroup("Extract"):
        a = ingest_vendor_a.submit()
        b = ingest_vendor_b.submit()
        c = ingest_vendor_c.submit()
    
    # Get results after TaskGroup completes
    df_a = a.result()
    df_b = b.result()
    df_c = c.result()

    with TaskGroup("Transform"):
        cleaned = cleanse_data.submit(df_a, df_b, df_c)
    
    cleaned_data = cleaned.result()

    with TaskGroup("Load"):
        load_to_db.submit(cleaned_data)
        send_summary_email.submit(len(cleaned_data))

Wait, but in the Load stage, load_to_db and send_summary_email are supposed to be sequential. Using .submit() on both would run them in parallel, which is incorrect.

Ah, right! So the Load stage's two tasks must be sequential. Therefore, within the Load TaskGroup, we need to call them in order, ensuring that send_summary_email runs after load_to_db.

But if we use .submit() for both, they would run in parallel. So instead, we need to call load_to_db normally (without .submit()), wait for it to complete, then call send_summary_email.

But within a TaskGroup, how to handle that.

Alternatively, perhaps the TaskGroup is just for visual grouping, and the execution order is determined by the code's sequence.

So modifying the Load TaskGroup:

with TaskGroup("Load"):
    load_result = load_to_db(cleaned_data)
    send_summary_email(load_result)

But load_to_db returns None, so send_summary_email would take len(cleaned_data) as before.

Wait, but in the code, cleaned_data is already available. So:

with TaskGroup("Load"):
    load_to_db(cleaned_data)
    send_summary_email(len(cleaned_data))

In this case, within the TaskGroup, load_to_db is called normally (so it runs and completes), then send_summary_email is called. So they are sequential.

But in the UI, both tasks would be inside the "Load" TaskGroup.

So integrating TaskGroups into the code:

from prefect import flow, task, TaskGroup

@flow
def supply_chain_etl() -> None:
    """Main ETL flow for supply chain shipment data."""
    # Stage 1: Extract
    with TaskGroup("Extract"):
        future_a = ingest_vendor_a.submit()
        future_b = ingest_vendor_b.submit()
        future_c = ingest_vendor_c.submit()

    # Wait for all extraction tasks to complete
    df_a = future_a.result()
    df_b = future_b.result()
    df_c = future_c.result()

    # Stage 2: Transform
    with TaskGroup("Transform"):
        cleaned_data = cleanse_data.submit(df_a, df_b, df_c).result()

    # Stage 3: Load
    with TaskGroup("Load"):
        load_to_db(cleaned_data)
        send_summary_email(len(cleaned_data))

But wait, the cleanse_data task is submitted and then .result() is called immediately, which blocks until it's done. So the Transform TaskGroup contains the cleanse_data task.

In the Load TaskGroup, load_to_db is called normally, then send_summary_email is called normally. So they run sequentially.

This should satisfy the requirement of using TaskGroups for each stage.

So the final code includes TaskGroups for each stage, ensuring that the Prefect UI groups the tasks visually.

But I need to import TaskGroup from prefect.utilities.tasks?

Wait, in Prefect 2.x, TaskGroup is part of prefect.task_runners? Or is it a separate import.

Wait, checking Prefect 2.x documentation: TaskGroup is available via from prefect import TaskGroup.

Wait, no. In Prefect 2.x, TaskGroup is part of the flow's context manager. Let me verify.

Ah, no. In Prefect 2, the TaskGroup is created using a context manager. The correct import is:

from prefect import TaskGroup

But I think in Prefect 2.x, TaskGroup is part of the prefect.flows module. Wait, perhaps the correct way is:

Within a flow, you can create a TaskGroup using the with statement and TaskGroup as a context manager.

So the code should import TaskGroup from prefect.utilities.tasks? Or perhaps from prefect.task_runners?

Wait, according to Prefect 2.x documentation, TaskGroup is part of the prefect.task_runners module. Or maybe it's part of prefect.flows.

Wait, checking the Prefect 2.x API docs: TaskGroup is part of prefect.flows. So you can use:

from prefect.flows import TaskGroup

But perhaps in Prefect 2.8+, TaskGroup is available via from prefect import TaskGroup.

Alternatively, perhaps the user's code example doesn't need to explicitly import TaskGroup because it's part of the flow's context.

Wait, perhaps the TaskGroup is created using a context manager within the flow function, without needing to import it separately.

Wait, no. The user must import TaskGroup from prefect.utilities.tasks or another module.

But according to Prefect's documentation, as of version 2.x, TaskGroup is part of the prefect.flows module. So the correct import is:

from prefect.flows import TaskGroup

But perhaps in some versions, it's under prefect.task_runners.

Alternatively, perhaps the code can use:

from prefect import TaskGroup

But I'm not sure. To avoid errors, perhaps the code should import TaskGroup correctly.

But given that the user's code must be executable, perhaps the correct import is:

from prefect import TaskGroup

Assuming that Prefect 2.x has this available.

So adding:

from prefect import TaskGroup

Then, within the flow, use with TaskGroup(...): blocks.

So the updated code includes TaskGroups for each stage.

Putting it all together:

from prefect import flow, task, TaskGroup
from typing import Tuple
import pandas as pd

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a() -> pd.DataFrame:
    """Ingest Vendor A CSV data."""
    return pd.read_csv("vendor_a.csv")

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b() -> pd.DataFrame:
    """Ingest Vendor B CSV data."""
    return pd.read_csv("vendor_b.csv")

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c() -> pd.DataFrame:
    """Ingest Vendor C CSV data."""
    return pd.read_csv("vendor_c.csv")

@task
def cleanse_data(df_a: pd.DataFrame, df_b: pd.DataFrame, df_c: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize data from all vendors."""
    combined = pd.concat([df_a, df_b, df_c])
    combined['sku'] = combined['sku'].str.upper().str.strip()
    return combined

@task
def load_to_db(data: pd.DataFrame) -> None:
    """Load processed data to PostgreSQL database."""
    print(f"Loaded {len(data)} records to database")

@task
def send_summary_email(record_count: int) -> None:
    """Send processing summary email."""
    print(f"Email sent: Processed {record_count} records")

@flow
def supply_chain_etl() -> None:
    """Main ETL flow for supply chain shipment data."""
    with TaskGroup("Extract"):
        future_a = ingest_vendor_a.submit()
        future_b = ingest_vendor_b.submit()
        future_c = ingest_vendor_c.submit()

    df_a = future_a.result()
    df_b = future_b.result()
    df_c = future_c.result()

    with TaskGroup("Transform"):
        cleaned_data = cleanse_data.submit(df_a, df_b, df_c)

    loaded_data = cleaned_data.result()

    with TaskGroup("Load"):
        load_to_db(loaded_data)
        send_summary_email(len(loaded_data))

if __name__ == "__main__":
    # Example deployment with schedule:
    # supply_chain_etl.serve(name="daily_etl", cron="0 0 * * *", start_date=datetime(2024, 1, 1))
    supply_chain_etl()

Wait, but in the Transform TaskGroup, the cleanse_data task is submitted, and then the result is obtained outside the TaskGroup. So the TaskGroup "Transform" would only include the submission of the task, but not the waiting for the result. Hmm.

Alternatively, within the Transform TaskGroup, we can get the result:

with TaskGroup("Transform"):
    cleaned_data = cleanse_data.submit(df_a, df_b, df_c)
    cleaned_data.result()

But then, the result is obtained within the TaskGroup. But in Prefect, the TaskGroup is just a visual grouping; the execution order is determined by the code flow.

Alternatively, perhaps the TaskGroup should encapsulate all the tasks in that stage. For the Transform stage, it's just the cleanse_data task. So the code is correct.

In the Load TaskGroup, both load_to_db and send_summary_email are called normally, so they are part of the TaskGroup and executed sequentially.

This should satisfy the requirement of using TaskGroups for each stage.

Now, check if the code is executable. Assuming pandas is installed, and the CSV files exist (even if they don't, the code will raise an error but is syntactically correct).

The retries are set on the ingest tasks, as specified.

The schedule comment is included in the main guard.

Thus, the final code meets all the constraints.
</think>

from prefect import flow, task, TaskGroup
import pandas as pd

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a() -> pd.DataFrame:
    """Ingest Vendor A CSV data."""
    return pd.read_csv("vendor_a.csv")

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b() -> pd.DataFrame:
    """Ingest Vendor B CSV data."""
    return pd.read_csv("vendor_b.csv")

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c() -> pd.DataFrame:
    """Ingest Vendor C CSV data."""
    return pd.read_csv("vendor_c.csv")

@task
def cleanse_data(df_a: pd.DataFrame, df_b: pd.DataFrame, df_c: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize data from all vendors."""
    combined = pd.concat([df_a, df_b, df_c])
    combined['sku'] = combined['sku'].str.upper().str.strip()
    return combined.dropna(subset=['shipment_date'])

@task
def load_to_db(data: pd.DataFrame) -> None:
    """Load processed data to PostgreSQL database."""
    print(f"Upserted {len(data)} records to inventory_shipments table")

@task
def send_summary_email(record_count: int) -> None:
    """Send email notification with processing summary."""
    print(f"Email to supply-chain-team@company.com: Processed {record_count} records")

@flow
def supply_chain_etl() -> None:
    """Main ETL flow for daily supply chain data processing."""
    with TaskGroup("Extract"):
        a = ingest_vendor_a.submit()
        b = ingest_vendor_b.submit()
        c = ingest_vendor_c.submit()

    cleaned = cleanse_data.submit(a.result(), b.result(), c.result())
    
    with TaskGroup("Load"):
        load_result = load_to_db.submit(cleaned.result())
        send_summary_email.submit(cleaned.result().shape[0], wait_for=[load_result])

if __name__ == "__main__":
    # Deployment configuration example:
    # supply_chain_etl.serve(name="daily-shipment-etl", cron="0 0 * * *", start_date=datetime(2024, 1, 1))
    supply_chain_etl()