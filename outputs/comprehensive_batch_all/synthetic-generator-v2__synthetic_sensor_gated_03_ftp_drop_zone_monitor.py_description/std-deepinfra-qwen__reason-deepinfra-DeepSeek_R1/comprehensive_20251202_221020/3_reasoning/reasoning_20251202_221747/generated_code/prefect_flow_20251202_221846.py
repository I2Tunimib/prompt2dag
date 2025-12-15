<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

First, the pipeline has a strict linear sequence. Each step depends on the previous one. The main steps are: Wait For FTP File, Download Vendor File, Cleanse Vendor Data, Merge With Internal Inventory.

The user mentioned using Prefect 2.x style with @task and @flow decorators. Each step should be a task, and the flow orchestrates them. For the sensor task, which is the first step, Prefect doesn't have a built-in sensor like Airflow, so I'll need to implement a custom polling mechanism. The sensor needs to check every 30 seconds and timeout after 5 minutes. So maybe a loop that sleeps for 30 seconds between checks, with a total timeout.

Wait, but Prefect tasks are supposed to be asynchronous? Or should I handle the polling within the task itself. Since the task needs to block until the file is found or timeout, I'll have to implement that logic inside the task. So the Wait For FTP File task will run a loop with time.sleep(30), and raise an exception if it times out.

Next, the Download Vendor File task. That's straightforward—once the sensor task completes, this task runs to download the file. Then Cleanse Vendor Data processes it, and Merge With Internal Inventory does the final step.

The pipeline is scheduled daily, but the user mentioned adding a comment about deployment/schedule. So maybe include a comment at the end about setting up a deployment with a schedule.

Retries: The flow should have retries configured. In Prefect, retries can be set at the flow level with the retries and retry_delay_seconds parameters. The description says 2 retries with 5-minute delay, so I'll add those to the @flow decorator.

Now, structuring the code. Start by importing necessary modules: prefect.flow and prefect.task, time for the sensor, maybe ftplib for FTP operations, and pandas for data processing. But since the user didn't specify the actual implementation of the FTP functions, I'll need to create placeholders with comments indicating where the actual code would go.

Each task should have a docstring explaining its purpose. The sensor task will have parameters for timeout and poke_interval. The loop checks for the file, sleeps if not found, and raises a TimeoutError if the timeout is reached.

Wait, but in Prefect, tasks can have parameters. So the wait_for_ftp_file task will take timeout and poke_interval as parameters. The flow will pass these values (5 minutes and 30 seconds) when calling the task.

For the download task, it needs the FTP connection details. Maybe the flow passes the ftp_server details as parameters. But the user didn't specify, so I'll assume that the FTP details are handled within the tasks, perhaps via configuration or environment variables. For simplicity, I'll include placeholders with comments.

The tasks after the sensor are straightforward: each depends on the previous task's output. Since they are in a linear sequence, each task is called normally (not submitted), except if there's parallelism, but here everything is sequential.

The flow function will call each task in order, passing the necessary parameters. The @flow decorator includes the retries and retry delay.

Testing if __name__ == '__main__' to call the flow for local execution.

Now, putting it all together. Let's outline the code structure:

1. Import statements.
2. Define the tasks with @task decorators.
3. Define the flow with @flow, including retry settings.
4. In the flow, call each task in sequence.
5. Add the if __name__ guard.

For the sensor task, the code will look something like this:

@task
def wait_for_ftp_file(timeout: int, poke_interval: int):
    start_time = time.time()
    while True:
        # Check FTP for file
        if file_exists():
            return
        if time.time() - start_time > timeout:
            raise TimeoutError("File not found within timeout")
        time.sleep(poke_interval)

But the actual implementation of file_exists() would require FTP connection logic. Since the user didn't provide details, I'll add a placeholder with a comment.

Similarly, the download task would connect to FTP and download the file. Again, placeholder functions with comments.

The cleanse task uses pandas to process the data, and the merge task joins with internal data.

I need to make sure that each task's output is passed to the next task if needed. For example, the download task might return the file path, which is then used by the cleanse task. But the description doesn't specify, so I'll assume that each task handles the data in a way that the next task can pick it up, perhaps by saving to a temporary directory and passing the file path.

Wait, the description says the download task saves to a local temp directory. So maybe the cleanse task reads from that path. But in Prefect, tasks can pass data via parameters. So perhaps each task returns the data or the path, which is passed to the next task.

But since the example is linear, each task is called in sequence, and the output of one is input to the next. For example:

file_path = download_vendor_file()
cleansed_data = cleanse_vendor_data(file_path)
merge_with_internal_inventory(cleansed_data)

But the original description doesn't mention passing data between tasks, just the order. So maybe each task doesn't take inputs but instead works on files in a known location. However, for Prefect's task dependencies, it's better to pass the data as parameters to ensure the execution order.

Alternatively, if the tasks don't take arguments, their dependencies are established by the order in which they're called in the flow. But in Prefect, even if tasks don't take parameters, calling them in sequence within the flow will create a dependency. So:

@flow
def inventory_pipeline():
    wait_for_ftp_file()
    download_vendor_file()
    cleanse_vendor_data()
    merge_with_internal_inventory()

This would create the correct dependencies because each task is called after the previous one, so Prefect knows to wait for each to complete before starting the next.

But for the sensor task, if it's a long-running task that waits for the file, then once it completes, the next tasks run.

Now, handling retries: the flow has retries=2 and retry_delay_seconds=300 (5 minutes). So the @flow decorator should include those parameters.

Putting it all together, the code would look like:

from prefect import flow, task
import time
# Other necessary imports like ftplib, pandas, etc.

@task
def wait_for_ftp_file(timeout: int = 300, poke_interval: int = 30):
    # Implementation that polls FTP every poke_interval seconds
    # Raises TimeoutError if not found within timeout
    pass

@task
def download_vendor_file():
    # Connect to FTP and download the file
    pass

@task
def cleanse_vendor_data():
    # Cleanse data using pandas
    pass

@task
def merge_with_internal_inventory():
    # Merge with internal system
    pass

@flow(retries=2, retry_delay_seconds=300)
def inventory_pipeline():
    wait_for_ftp_file(timeout=300, poke_interval=30)
    download_vendor_file()
    cleanse_vendor_data()
    merge_with_internal_inventory()

if __name__ == "__main__":
    inventory_pipeline()

But the sensor task needs to actually implement the polling. Let's flesh that out with a loop.

Wait, but in Prefect, tasks are asynchronous by default when using .submit(), but here we're calling them sequentially. However, the sensor task is a blocking task that waits until the file is found or times out. So the code inside wait_for_ftp_file task would be a loop that checks for the file.

Implementing that:

@task
def wait_for_ftp_file(timeout: int = 300, poke_interval: int = 30):
    """Poll FTP server for vendor_inventory.csv until found or timeout."""
    start_time = time.time()
    while True:
        # Check if file exists on FTP
        # Placeholder for actual FTP check
        file_found = False  # Replace with actual check
        if file_found:
            return True
        if time.time() - start_time > timeout:
            raise TimeoutError("File not found within the timeout period")
        time.sleep(poke_interval)

But the time.sleep would block the task, which is okay since it's a synchronous task. However, in Prefect, using time.sleep in a task might not be the best practice if the task is async, but since we're not using .submit() here, it's okay.

The other tasks would have placeholder implementations, perhaps with comments indicating where to add the actual code.

For the retries, the flow is set to retry twice with 5-minute delays. That's handled in the @flow decorator.

The schedule is daily, so a comment can be added near the flow definition about deployment configuration.

Putting all together, here's the code:

from prefect import flow, task
import time
# import ftplib
# import pandas as pd
# from some_internal_system import get_internal_inventory

@task
def wait_for_ftp_file(timeout: int = 300, poke_interval: int = 30):
    """Poll FTP server every poke_interval seconds for vendor_inventory.csv."""
    start_time = time.time()
    while True:
        # Example FTP check logic (implement with actual FTP client)
        # with ftplib.FTP('ftp.example.com') as ftp:
        #     ftp.login(user='user', passwd='pass')
        #     files = ftp.nlst()
        # if 'vendor_inventory.csv' in files:
        #     return
        print("Checking FTP for vendor_inventory.csv...")
        file_exists = False  # Simulated check; replace with actual implementation
        if file_exists:
            print("File found.")
            return
        if time.time() - start_time > timeout:
            raise TimeoutError(f"File not found after {timeout} seconds")
        time.sleep(poke_interval)

@task
def download_vendor_file():
    """Download vendor_inventory.csv from FTP server."""
    # Implement FTP download logic
    # Save to local temporary directory
    print("Downloading vendor file...")
    return "/tmp/vendor_inventory.csv"  # Example path

@task
def cleanse_vendor_data(file_path: str):
    """Clean data by removing nulls in key columns."""
    # df = pd.read_csv(file_path)
    # columns = ['product_id', 'quantity', 'price']
    # df_clean = df.dropna(subset=columns)
    # cleaned_path = "/tmp/cleaned_vendor_inventory.csv"
    # df_clean.to_csv(cleaned_path, index=False)
    print("Cleansing vendor data...")
    return "/tmp/cleaned_vendor_inventory.csv"  # Example path

@task
def merge_with_internal_inventory(cleaned_path: str):
    """Merge cleansed data with internal inventory system."""
    # internal_df = get_internal_inventory()
    # vendor_df = pd.read_csv(cleaned_path)
    # merged_df = pd.merge(internal_df, vendor_df, on='product_id')
    # Update internal system with merged data
    print("Merging with internal inventory...")

@flow(retries=2, retry_delay_seconds=300)
def inventory_pipeline():
    # To deploy with a daily schedule:
    # prefect deploy --name inventory --interval 86400
    wait_for_ftp_file(timeout=300, poke_interval=30)
    file_path = download_vendor_file()
    cleaned_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleaned_path)

if __name__ == "__main__":
    inventory_pipeline()

This code includes all the necessary parts: tasks decorated with @task, a flow with retries, parameters for timeout and poke_interval, and sequential execution. The tasks pass data through parameters (file_path), ensuring dependencies. The sensor task has a loop with sleep and timeout. Placeholder print statements and comments indicate where actual implementation would go. The flow includes a comment about deployment with a schedule. The if __name__ block runs the flow locally.
</think>

from prefect import flow, task
import time
# import ftplib
# import pandas as pd
# from some_internal_system import get_internal_inventory

@task
def wait_for_ftp_file(timeout: int = 300, poke_interval: int = 30):
    """Poll FTP server every poke_interval seconds for vendor_inventory.csv."""
    start_time = time.time()
    while True:
        # Example FTP check logic (implement with actual FTP client):
        # with ftplib.FTP('ftp.example.com') as ftp:
        #     ftp.login(user='user', passwd='pass')
        #     files = ftp.nlst()
        # if 'vendor_inventory.csv' in files:
        #     break
        print("Checking FTP for vendor_inventory.csv...")
        simulated_file_found = time.time() - start_time > 10  # Remove in real implementation
        if simulated_file_found:
            print("File found.")
            return
        if time.time() - start_time > timeout:
            raise TimeoutError(f"File not found after {timeout} seconds")
        time.sleep(poke_interval)

@task
def download_vendor_file():
    """Download vendor_inventory.csv from FTP server."""
    # Implement FTP download logic here
    print("Downloading vendor file...")
    return "/tmp/vendor_inventory.csv"

@task
def cleanse_vendor_data(file_path: str):
    """Clean data by removing nulls in key columns."""
    # df = pd.read_csv(file_path)
    # columns = ['product_id', 'quantity', 'price']
    # df_clean = df.dropna(subset=columns)
    print("Cleansing vendor data...")
    return "/tmp/cleaned_vendor_inventory.csv"

@task
def merge_with_internal_inventory(cleaned_path: str):
    """Merge cleansed data with internal inventory system."""
    # internal_df = get_internal_inventory()
    # vendor_df = pd.read_csv(cleaned_path)
    # merged_df = pd.merge(internal_df, vendor_df, on='product_id')
    print("Merging with internal inventory...")

@flow(retries=2, retry_delay_seconds=300)
def inventory_pipeline():
    # To deploy with daily schedule: use prefect deploy with --interval 86400
    wait_for_ftp_file(timeout=300, poke_interval=30)
    file_path = download_vendor_file()
    cleaned_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleaned_path)

if __name__ == "__main__":
    inventory_pipeline()