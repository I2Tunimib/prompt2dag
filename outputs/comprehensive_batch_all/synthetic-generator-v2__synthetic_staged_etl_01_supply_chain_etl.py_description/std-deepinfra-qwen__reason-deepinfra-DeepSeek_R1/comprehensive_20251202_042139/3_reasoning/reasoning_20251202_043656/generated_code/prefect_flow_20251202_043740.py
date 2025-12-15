<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

First, the pipeline has three stages: Extract, Transform, Load. The Extract stage has three tasks that run in parallel. So I'll use .submit() for each of those tasks to make them run concurrently. Then the Transform stage depends on all three extraction tasks completing, which means I need to wait for their results. That's a fan-in pattern.

For the Transform stage, the cleanse_data task takes all three vendor datasets. Since the extraction tasks are run with .submit(), I'll have to get their results using .result() inside the cleanse_data task. Then, the Load stage has two sequential tasks: load_to_db must complete before send_summary_email. So those are called normally, in order.

The user mentioned using TaskGroups for logical stage grouping. In Prefect 2, TaskGroups are part of the UI but in code, we structure the flow with context managers. Wait, no, actually in Prefect 2, you can use with Flow as the main structure, but maybe they meant using subflows or just organizing the tasks within the main flow. Hmm, maybe the user intended to group tasks into stages using TaskGroups, but in Prefect 2.x, TaskGroups are more of a UI concept. So perhaps the code doesn't need to explicitly create them, but the structure in the flow function can reflect the stages through comments or context. Alternatively, maybe using the task decorator and structuring the code into sections for each stage.

The schedule is daily starting Jan 1, 2024, but the code should include a comment about deployment/schedule configuration. So I'll add a comment near the flow definition indicating that.

Retries are configured with 2 retries and 5-minute delay. So I need to add retries to each task using the @task decorator's retries and retry_delay_seconds parameters. Wait, retry_delay_seconds is in seconds, so 5 minutes is 300 seconds. So set retries=2 and retry_delay_seconds=300.

Each task should have explicit dependencies. For the Extract tasks, since they're submitted in parallel, their futures are passed to the transform task. Then the transform task's output is passed to load_to_db, which then triggers send_summary_email.

Now, the data is passed via XCom, which in Prefect 2 is handled automatically for task outputs. So the ingest tasks return DataFrames (assuming), which are then passed to cleanse_data. But since they're run asynchronously with submit(), I need to retrieve their results using .result() within the transform task.

Wait, when you submit a task, it returns a future-like object. To get the actual result, you have to call .result() on it. So in the cleanse_data task, which depends on the three ingest tasks, I need to get each of their results. So inside the cleanse_data function, I would take the futures as arguments and then call .result() on them. Wait, no—when you pass the future objects as arguments to another task, Prefect automatically resolves them. Or maybe not. Let me think.

In Prefect, when a task is submitted with .submit(), it returns a PrefectFuture. When another task takes that future as an input, Prefect will wait for the future to complete and pass the result automatically. So in the code, the cleanse_data task would receive the actual data from the ingest tasks, not the futures. Therefore, in the flow function, after submitting the three ingest tasks, their futures are passed as arguments to cleanse_data, which will wait for them and process their results.

So the flow would look like:

@flow
def etl_supply_chain_shipments():
    # Stage 1: Extract
    vendor_a_future = ingest_vendor_a.submit()
    vendor_b_future = ingest_vendor_b.submit()
    vendor_c_future = ingest_vendor_c.submit()

    # Stage 2: Transform
    cleansed_data = cleanse_data(vendor_a_future, vendor_b_future, vendor_c_future)

    # Stage 3: Load
    load_result = load_to_db(cleansed_data)
    send_summary_email(load_result)

Wait, but the load_to_db task needs to complete before the email is sent. So the send_summary_email task should depend on load_to_db. Since load_to_db is called normally (not submitted), it's sequential. So the flow would first run load_to_db, then send_summary_email.

Now, for the tasks themselves. Each ingest task reads a CSV. For the sake of example, they can return a DataFrame. The cleanse_data task takes the three DataFrames, processes them, and returns a combined cleansed DataFrame. The load_to_db task takes that DataFrame and loads it into PostgreSQL. The send_summary_email task might take some info from the load step, maybe the count of records.

But the problem is that the user mentioned XCom for passing data between stages. In Prefect 2, XCom is handled automatically, so as long as the tasks return serializable data, it's stored in XCom. However, DataFrames can be large, so maybe in practice, they would write to a temporary storage and pass the path, but for the example, we'll assume small data and pass DataFrames directly.

Now, the code structure:

- Import necessary modules: prefect's flow and task, pandas for data handling, sqlalchemy for PostgreSQL, smtplib for emails.

But the user didn't specify implementation details for each task, so I need to create placeholders with minimal code that's executable. For example, the ingest tasks could read a CSV using pandas, but since the filenames aren't given, perhaps they have parameters. But the user said to avoid placeholders like <TODO>, so add minimal defaults.

Wait, the user said: "If something is unknown, add minimal, safe defaults or comments." So for example, the ingest tasks could have a parameter for the file path, but in the code, we can hardcode a placeholder path with a comment.

But the code must be executable. So perhaps, for the sake of example, each ingest task returns a DataFrame. Let's mock that.

For example:

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a():
    # Mock data ingestion from Vendor A
    return pd.DataFrame({"shipment_id": [1, 2], "sku": ["A123", "B456"]})

Similarly for B and C.

The cleanse_data task would concatenate the DataFrames and perform some processing.

The load_to_db task would use sqlalchemy to connect to PostgreSQL and load the data. Again, for the example, we can use a mock connection.

The send_summary_email task would use smtplib to send an email. But again, for the example, we can have a placeholder that prints or uses a mock.

But the user wants the code to be executable. So, the code should not have real credentials but use placeholders with comments. However, the code must run without errors. So perhaps, for the database connection, we can use a dummy connection string, but catch exceptions, or use a mock.

Alternatively, use print statements to indicate the action, so the code runs but doesn't actually connect to a database or send emails. That's acceptable for the purpose of the example.

So, putting it all together:

- Import pandas as pd.
- Ingest tasks return small DataFrames.
- Cleanse task concatenates them.
- Load task prints or logs loading.
- Email task prints or uses a mock.

Now, the code:

from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine
import smtplib
from email.message import EmailMessage

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a():
    # Ingest CSV from Vendor A - implementation placeholder
    return pd.DataFrame({"shipment_id": [1, 2], "sku": ["A123", "B456"]})

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b():
    # Ingest CSV from Vendor B - implementation placeholder
    return pd.DataFrame({"shipment_id": [3, 4], "sku": ["C789", "D012"]})

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c():
    # Ingest CSV from Vendor C - implementation placeholder
    return pd.DataFrame({"shipment_id": [5, 6], "sku": ["E345", "F678"]})

@task(retries=2, retry_delay_seconds=300)
def cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data):
    # Combine and cleanse data
    combined = pd.concat([vendor_a_data, vendor_b_data, vendor_c_data])
    # Normalize SKU formats, validate dates, enrich with location
    combined['sku'] = combined['sku'].str.upper()
    return combined

@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleansed_data):
    # Load data to PostgreSQL
    engine = create_engine('postgresql://user:pass@localhost:5432/inventory')
    cleansed_data.to_sql('inventory_shipments', engine, if_exists='append', index=False)
    return len(cleansed_data)

@task(retries=2, retry_delay_seconds=300)
def send_summary_email(record_count):
    # Send email notification
    msg = EmailMessage()
    msg.set_content(f"Processed {record_count} records successfully.")
    msg['Subject'] = 'Shipment Data Processing Summary'
    msg['From'] = 'noreply@company.com'
    msg['To'] = 'supply-chain-team@company.com'
    
    # Mock sending - replace with real SMTP server details in production
    with smtplib.SMTP('localhost', 1025) as server:
        server.send_message(msg)

@flow
# Schedule comment: Configure daily execution starting 2024-01-01 via deployment
def etl_supply_chain_shipments():
    # Stage 1: Parallel extraction
    vendor_a = ingest_vendor_a.submit()
    vendor_b = ingest_vendor_b.submit()
    vendor_c = ingest_vendor_c.submit()

    # Stage 2: Transform
    cleansed = cleanse_data(vendor_a, vendor_b, vendor_c)

    # Stage 3: Load and notify
    count = load_to_db(cleansed)
    send_summary_email(count)

if __name__ == '__main__':
    etl_supply_chain_shipments()

Wait, but when using .submit(), the tasks return futures. However, when passing them as arguments to another task, Prefect will automatically resolve them. So in the cleanse_data task, vendor_a, vendor_b, vendor_c are futures, but when passed to cleanse_data, Prefect will wait for them and pass their results (the DataFrames) to the cleanse_data function.

Yes, that's correct. So the code is structured correctly.

Now, the load_to_db task depends on cleansed, which is the output of cleanse_data. Then send_summary_email depends on count, which is the output of load_to_db. So the flow is sequential after the transform stage.

The retries are added to each task with retries=2 and retry_delay_seconds=300 (5 minutes).

The schedule comment is added as a comment near the flow decorator.

The code includes the necessary imports, but when using sqlalchemy and smtplib, those are part of the standard library (except sqlalchemy and pandas, which need to be installed). The user said to assume required packages are installed.

So the code should be executable as long as pandas, sqlalchemy, and prefect are installed. The email part uses smtplib, which is standard, but in practice, the SMTP server might not be running on localhost:1025, so the send_summary_email task will raise an error unless it's mocked. But the user wants the code to be executable, so perhaps replacing the SMTP part with a print statement would be better. Alternatively, catch the exception and proceed.

Alternatively, since the user wants the code to be executable, perhaps replace the actual sending with a print statement. Let me adjust that:

In send_summary_email:

print(f"Email sent: {record_count} records processed.") 

But the user's example includes the email details, so maybe keep the code as is but with a comment that it's a mock. However, when run, it would fail if there's no SMTP server. To make it executable without errors, perhaps use a mock that doesn't actually send the email. For example, removing the server.send_message or using a dummy block.

Alternatively, use a try-except block to handle the connection error and proceed.

But perhaps the simplest way is to replace the SMTP part with a print statement. Let me adjust that:

@task(retries=2, retry_delay_seconds=300)
def send_summary_email(record_count):
    # Send email notification - mock implementation
    print(
        f"Email notification sent to supply-chain-team@company.com: "
        f"Processed {record_count} records."
    )

That way, the code runs without external dependencies.

Similarly, the load_to_db task uses a dummy connection string. If the user doesn't have PostgreSQL running, this will throw an error. To avoid that, perhaps add a check if the engine can connect, or mock the loading. But for the sake of the example, maybe just print that the data is loaded.

Adjust load_to_db:

@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleansed_data):
    # Mock database load
    print(f"Loading {len(cleansed_data)} records to database.")
    return len(cleansed_data)

This way, the code doesn't require a real database connection.

So revising the tasks:

The ingest tasks return DataFrames, cleanse combines them, load_to_db prints and returns the count, send_summary_email prints the email.

This makes the code fully executable without external dependencies beyond pandas and prefect.

So the final code would look like this.
</think>

from prefect import flow, task
import pandas as pd

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a():
    """Mock ingestion of Vendor A CSV data"""
    return pd.DataFrame({"shipment_id": range(1, 1251), "sku": ["A_%04d" % i for i in range(1, 1251)]})

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b():
    """Mock ingestion of Vendor B CSV data"""
    return pd.DataFrame({"shipment_id": range(1, 981), "sku": ["B_%04d" % i for i in range(1, 981)]})

@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c():
    """Mock ingestion of Vendor C CSV data"""
    return pd.DataFrame({"shipment_id": range(1, 1751), "sku": ["C_%04d" % i for i in range(1, 1751)]})

@task(retries=2, retry_delay_seconds=300)
def cleanse_data(vendor_a, vendor_b, vendor_c):
    """Clean and normalize combined vendor data"""
    combined = pd.concat([vendor_a, vendor_b, vendor_c])
    combined["sku"] = combined["sku"].str.replace("_", "-")
    return combined.dropna()

@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleansed_data):
    """Mock database load operation"""
    print(f"Loaded {len(cleansed_data)} records to inventory_shipments table")
    return len(cleansed_data)

@task(retries=2, retry_delay_seconds=300)
def send_summary_email(record_count):
    """Mock email notification"""
    print(f"Sent summary email: Processed {record_count} records")

@flow  # Schedule: Configure daily execution via deployment with start_date=2024-01-01
def etl_supply_chain_shipments():
    # Stage 1: Parallel extraction
    vendor_a_future = ingest_vendor_a.submit()
    vendor_b_future = ingest_vendor_b.submit()
    vendor_c_future = ingest_vendor_c.submit()

    # Stage 2: Consolidated transformation
    cleansed = cleanse_data(vendor_a_future, vendor_b_future, vendor_c_future)

    # Stage 3: Sequential loading and notification
    record_count = load_to_db(cleansed)
    send_summary_email(record_count)

if __name__ == "__main__":
    etl_supply_chain_shipments()