<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

First, the pipeline has a strict linear sequence. So all tasks will run one after another. The first task is a sensor that waits for a file on an FTP server. Then download, cleanse, and merge steps follow.

The user mentioned using Prefect 2.x APIs, so I'll import flow and task decorators. Each step should be a @task. The main flow will orchestrate them.

The Wait For Ftp File step is a sensor. In Prefect, sensors might be implemented with tasks that poll until a condition is met. But Prefect 2 doesn't have a built-in Sensor like Airflow. So I need to create a custom task that polls the FTP server. The task should check for the file every 30 seconds (poke_interval) and timeout after 5 minutes. Since Prefect tasks can have retries, maybe using retries with a delay could simulate the polling. Alternatively, implement a loop within the task with sleep, but that's not ideal. Hmm, maybe using timeout and retries with appropriate parameters.

Wait, the user specified that the sensor task should block downstream processing until the file is detected. So the task itself must not complete until the file is found or it times out. So the task function will loop, checking the FTP server every 30 seconds, and either return when the file is found or raise an exception after 5 minutes.

But Prefect tasks are designed to be asynchronous. However, in this case, the task needs to run synchronously because the next tasks depend on its completion. So the sensor task will run in a loop, checking periodically, and only completes when the file is found. If it times out, the task fails, which would trigger retries based on the pipeline's retry configuration.

The pipeline has a retry mechanism: 2 retries with 5-minute delay. So the flow or tasks should have retries set. Since the sensor is the first task, maybe the flow itself is retried, but the description says the pipeline is scheduled daily. Wait, the orchestration section says it's scheduled daily, so the flow runs once a day, and within that run, the sensor tries for up to 5 minutes. If the sensor times out, the flow fails and can retry up to 2 times with 5-minute delays. So the retries are at the flow level.

But Prefect allows task-level retries as well. However, the sensor task itself has a timeout of 5 minutes. So if the sensor doesn't find the file within 5 minutes, it fails. Then the flow can retry the entire process, including the sensor, up to 2 times. So maybe the flow is decorated with retries=2 and retry_delay_seconds=300.

Wait, the pipeline's retry mechanism allows 2 retries with 5-minute delay between attempts. So the flow should be configured with retries and retry_delay. So in the @flow decorator, I can set retries=2 and retry_delay_seconds=300.

Now, the sensor task needs to poll every 30 seconds (poke_interval) and timeout after 5 minutes. So within the sensor task, I can have a loop that runs for a maximum of 5 minutes, checking every 30 seconds. If the file is found during any check, the task completes. Otherwise, after 5 minutes, it raises an exception.

Implementing that, the wait_for_ftp_file task would have a loop with time.sleep(30), but since tasks are supposed to be asynchronous, using sleep might block the event loop. However, in Prefect, if the task is running in a synchronous context, using time.sleep is okay. Alternatively, use asynchronous sleep, but that complicates things. For simplicity, since the user is asking for a minimal example, perhaps using a loop with time.sleep and tracking the elapsed time.

So the sensor task function would look something like:

@task
def wait_for_ftp_file():
    start_time = time.time()
    timeout = 300  # 5 minutes
    poke_interval = 30  # 30 seconds
    while time.time() - start_time < timeout:
        if check_ftp_file_exists():
            return
        time.sleep(poke_interval)
    raise Exception("File not found within timeout")

But the actual FTP check function (check_ftp_file_exists) needs to be implemented. Since the user mentioned it's a custom task, I'll leave a placeholder with a comment. Maybe use ftplib to connect to the server and check for the file.

But the code needs to be executable, so I should include minimal implementations. For example, the download_vendor_file function would need to connect to FTP and retrieve the file. But since the exact FTP details aren't provided, I'll use placeholders with comments indicating where the actual implementation would go.

Next, the Download Vendor File task. The function download_vendor_file would connect to the FTP server and download the file. Again, using ftplib, but in the code, I'll write a skeleton function that simulates this.

Cleanse Vendor Data task: the function reads the downloaded file, removes nulls in specific columns. Using pandas, perhaps. So the code would read the CSV, drop rows where product_id, quantity, or price are null.

Merge With Internal Inventory task: this function would read the internal inventory (maybe from a database or another file), merge with the cleansed data on product_id, update stock and pricing. Again, a placeholder implementation using pandas for demonstration.

Now, putting it all together. Each step is a @task, and the @flow orchestrates them in order. Since the steps are sequential, each task is called normally, not with .submit(), except if there's parallelism. But the pipeline is linear, so no need for .submit().

The schedule is daily. Prefect 2 uses deployments for scheduling. The user says to include a comment about deployment/schedule configuration. So add a comment at the top about creating a deployment with a schedule.

The code should have an if __name__ == '__main__': block to run the flow locally.

Now, let's outline the code structure:

1. Import necessary modules: prefect, time, ftplib, pandas, etc.
2. Define the tasks with @task decorator.
3. Define the flow with @flow, including retries and retry delay.
4. In the flow function, call the tasks in order.
5. Add the main guard.

Wait, but the retry mechanism: the user says the pipeline allows 2 retries with 5-minute delay. So the flow should have retries=2 and retry_delay_seconds=300. So the @flow decorator would include retries=2, retry_delay_seconds=300.

The sensor task (wait_for_ftp_file) has its own timeout of 5 minutes. So even if the flow retries, each attempt allows the sensor to wait up to 5 minutes. So each flow run will have the sensor trying for 5 minutes, and if it fails, the flow retries after 5 minutes, up to 2 times.

Now, code:

Import statements:

from prefect import flow, task
import time
# Other imports like ftplib, pandas, etc.

Then the tasks:

@task
def wait_for_ftp_file():
    # Implementation with polling

@task
def download_vendor_file():
    # Download code

@task
def cleanse_vendor_data(file_path):
    # Cleanse data

@task
def merge_with_internal_inventory(cleansed_data):
    # Merge code

Flow:

@flow(retries=2, retry_delay_seconds=300)
def vendor_inventory_pipeline():
    # Call tasks in order

But wait, the sensor task's timeout is 5 minutes per flow run. Each retry would be a new flow run, so each retry would again run the sensor for up to 5 minutes. That aligns with the requirement.

Now, implementing the sensor task. Let's write a function that loops for 5 minutes, checking every 30 seconds. For the FTP check, use ftplib:

def check_ftp_file_exists():
    # Connect to FTP and check for file
    # Return True if found, else False
    # For example:
    from ftplib import FTP
    ftp = FTP('ftp.example.com')
    ftp.login(user='user', passwd='pass')
    files = ftp.nlst()
    ftp.quit()
    return 'vendor_inventory.csv' in files

But including actual FTP code would require credentials, which we can't hardcode. So in the code, perhaps use placeholders with comments, or simulate with a mock. However, the user wants the code to be executable. So maybe simulate the check with a mock for demonstration purposes. For example, in the sensor task, after a few iterations, return successfully. But that's not real. Alternatively, assume that the file appears within the first check for the sake of the example. But the user might want the code to be functional, so perhaps use a placeholder with a comment indicating where real FTP code would go.

Alternatively, use a simulated condition. For example, in the sensor task, after 30 seconds, return. But that's not realistic. Hmm.

Alternatively, in the code, have the sensor task check a local file for testing. But the problem states that it's an FTP server. So perhaps the code should include the FTP logic but with placeholders for server details. For example:

@task
def wait_for_ftp_file():
    timeout = 300  # 5 minutes
    poke_interval = 30
    start_time = time.monotonic()
    while True:
        # Simulated check; replace with actual FTP logic
        print("Checking FTP server for vendor_inventory.csv...")
        # Example FTP check (commented out)
        # from ftplib import FTP
        # with FTP('host') as ftp:
        #     ftp.login('user', 'pass')
        #     if 'vendor_inventory.csv' in ftp.nlst():
        #         break
        # For demonstration, assume file is found after 2 checks
        if time.monotonic() - start_time > 60:
            break
        else:
            time.sleep(poke_interval)
    elapsed = time.monotonic() - start_time
    if elapsed > timeout:
        raise TimeoutError("File not found within 5 minutes")
    return True

But this is a simulation. However, the user wants the code to be executable. So perhaps this approach allows the code to run without real FTP, for testing. Alternatively, use a mock that randomly decides. But maybe the simplest way is to have the sensor task wait for 30 seconds once, then proceed, just to make the code runnable.

But the problem states that the sensor task should time out after 5 minutes if the file isn't found. So the code should have a loop that runs for up to 5 minutes, checking every 30 seconds. However, in practice, this would make the task take 5 minutes to fail, which is not ideal for testing. So perhaps in the example code, the sensor task immediately finds the file, allowing the rest of the pipeline to proceed. But include comments indicating where the actual FTP check would go.

So, the wait_for_ftp_file task could be implemented as:

@task
def wait_for_ftp_file():
    timeout = 300  # 5 minutes
    poke_interval = 30
    start_time = time.time()
    while True:
        # Check FTP server for file existence
        # Replace with actual FTP connection and file check
        file_found = False  # Simulated result
        # Example:
        # file_found = check_ftp_file()
        if file_found:
            return True
        if time.time() - start_time >= timeout:
            raise TimeoutError("File not found within 5 minutes")
        time.sleep(poke_interval)

But this would loop indefinitely unless the simulated file_found is changed. To make the code runnable without getting stuck, perhaps in the example code, set file_found to True after the first check. But that's not realistic. Alternatively, simulate a successful check after a few seconds.

Alternatively, for the sake of the example, have the task immediately return, assuming the file is present. But that doesn't demonstrate the polling logic. Hmm.

This is a bit tricky. The code needs to be executable, but without actual FTP access, the sensor task can't really check. So perhaps the code includes the structure with comments indicating where the actual implementation would go, and for the purposes of the example, the task immediately proceeds, allowing the rest of the pipeline to run.

So, in the wait_for_ftp_file task:

@task
def wait_for_ftp_file():
    # Implementation would connect to FTP and check for file
    # Here's a mock implementation that assumes file exists
    print("Checking FTP server...")
    # Simulate waiting for 30 seconds
    time.sleep(30)
    print("File found.")
    return True

But this doesn't handle the timeout. However, the user's code needs to be executable. So perhaps the sensor task is simplified for the example, with a comment explaining that the actual implementation would include the polling and timeout logic.

But the problem states that the sensor task has a timeout of 5 minutes and poke interval of 30 seconds. So the code must include that logic, even if it's simulated.

Alternatively, use a loop that runs for a shorter duration for demonstration. For example, loop 2 times with 1-second intervals, then succeed. That way, the code runs quickly but demonstrates the structure.

But the user might want the code to be as per the description, even if it's not fully functional without real FTP. So include the loop structure with comments.

So, the wait_for_ftp_file task:

@task
def wait_for_ftp_file():
    timeout = 300  # 5 minutes in seconds
    poke_interval = 30  # Check every 30 seconds
    start_time = time.monotonic()

    while True:
        # Actual implementation would use ftplib to check for file
        # For demonstration, assume file exists after 30 seconds
        print(f"Checking FTP server for vendor_inventory.csv... (Elapsed: {time.monotonic() - start_time:.0f}s)")
        file_exists = False  # Replace with actual check

        # Simulate file found after 30 seconds
        if time.monotonic() - start_time >= 30:
            file_exists = True

        if file_exists:
            print("File found.")
            return
        elif time.monotonic() - start_time >= timeout:
            raise TimeoutError("File not found within 5 minutes")
        else:
            time.sleep(poke_interval)

This way, the task will simulate waiting for 30 seconds (one poke interval), then find the file. This makes the code runnable without taking 5 minutes, while demonstrating the structure. The timeout is still set to 5 minutes, but in practice, the task completes after 30 seconds.

For the download task, perhaps write a function that creates a dummy file, since we can't actually connect to FTP.

@task
def download_vendor_file():
    # Actual implementation would download from FTP
    # Here, create a dummy file for demonstration
    import pandas as pd
    from pathlib import Path

    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / "vendor_inventory.csv"
    
    # Create sample data
    data = pd.DataFrame({
        "product_id": [1, 2, 3],
        "quantity": [10, 20, None],
        "price": [15.5, None, 25.0]
    })
    data.to_csv(file_path, index=False)
    print(f"Downloaded file to {file_path}")
    return file_path

The cleanse task would read this file and drop rows with nulls in the specified columns.

@task
def cleanse_vendor_data(file_path):
    import pandas as pd

    df = pd.read_csv(file_path)
    # Drop rows where product_id, quantity, or price is null
    cleansed_df = df.dropna(subset=["product_id", "quantity", "price"])
    cleansed_path = file_path.parent / "cleansed_vendor_inventory.csv"
    cleansed_df.to_csv(cleansed_path, index=False)
    print(f"Cleansed data saved to {cleansed_path}")
    return cleansed_path

The merge task would read the cleansed data and merge with a mock internal inventory.

@task
def merge_with_internal_inventory(cleansed_data_path):
    import pandas as pd

    # Mock internal inventory
    internal_inventory = pd.DataFrame({
        "product_id": [1, 2, 3],
        "stock": [5, 10, 15],
        "price": [15.0, 20.0, 25.0]
    })

    vendor_data = pd.read_csv(cleansed_data_path)
    merged = pd.merge(internal_inventory, vendor_data, on="product_id", suffixes=('_internal', '_vendor'))
    # Update stock and price (example logic)
    merged['stock'] += merged['quantity']
    merged['price'] = merged['price_vendor']  # Override with vendor price
    # Save or update internal system (mock)
    print("Merged inventory data:")
    print(merged)
    return merged

Putting it all together in the flow:

@flow(retries=2, retry_delay_seconds=300)
def vendor_inventory_pipeline():
    # To schedule daily, create a deployment with:
    # prefect deploy --name inventory-pipeline --schedule "0 0 * * *"
    wait_for_ftp_file()
    file_path = download_vendor_file()
    cleansed_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleansed_path)

if __name__ == "__main__":
    vendor_inventory_pipeline()

This code should be executable, assuming pandas is installed. The sensor task simulates waiting for 30 seconds (one poke interval) before proceeding. The download task creates a dummy CSV, cleanse processes it, and merge merges with mock data.

I need to make sure all tasks are called in sequence, and parameters are passed correctly. The download task returns a file path, which is passed to cleanse, which returns the cleansed path, passed to merge.

The retries are set at the flow level with 2 retries and 5-minute delay. The sensor task's timeout is handled within its own loop.

This should meet all the constraints: uses Prefect 2.x tasks and flow, linear sequence, retries, includes main guard, minimal docstrings, and executable code.
</think>

from prefect import flow, task
import time
from pathlib import Path
import pandas as pd

@task
def wait_for_ftp_file():
    """Poll FTP server for vendor_inventory.csv with 30s interval and 5m timeout"""
    timeout = 300  # 5 minutes
    poke_interval = 30
    start_time = time.monotonic()

    while True:
        # Actual implementation would use ftplib here
        print(f"Checking FTP server... (Elapsed: {time.monotonic()-start_time:.0f}s)")
        file_exists = False  # Replace with real FTP check
        
        # Simulation: File found after 30 seconds
        if time.monotonic() - start_time >= 30:
            file_exists = True

        if file_exists:
            print("File detected")
            return True
        elif time.monotonic() - start_time >= timeout:
            raise TimeoutError("File not found within 5 minutes")
        time.sleep(poke_interval)

@task
def download_vendor_file():
    """Retrieve file from FTP server and save locally"""
    # Actual implementation would use ftplib here
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / "vendor_inventory.csv"
    
    # Create sample data
    pd.DataFrame({
        "product_id": [1, 2, 3],
        "quantity": [10, 20, None],
        "price": [15.5, None, 25.0]
    }).to_csv(file_path, index=False)
    
    print(f"Downloaded file to {file_path}")
    return file_path

@task
def cleanse_vendor_data(file_path):
    """Clean null values from key columns"""
    df = pd.read_csv(file_path)
    cleansed_df = df.dropna(subset=["product_id", "quantity", "price"])
    cleansed_path = file_path.parent / "cleansed_vendor.csv"
    cleansed_df.to_csv(cleansed_path, index=False)
    print(f"Cleansed data saved to {cleansed_path}")
    return cleansed_path

@task
def merge_with_internal_inventory(cleansed_path):
    """Merge vendor data with internal inventory system"""
    internal_df = pd.DataFrame({
        "product_id": [1, 2, 3],
        "stock": [5, 10, 15],
        "price": [15.0, 20.0, 25.0]
    })
    
    vendor_df = pd.read_csv(cleansed_path)
    merged = pd.merge(internal_df, vendor_df, on="product_id", suffixes=('_internal', '_vendor'))
    
    # Example merge logic
    merged['stock'] += merged['quantity']
    merged['price'] = merged['price_vendor']
    
    print("Merged inventory update:")
    print(merged[['product_id', 'stock', 'price']])
    return merged

@flow(retries=2, retry_delay_seconds=300)
def vendor_inventory_pipeline():
    """Main pipeline orchestrating inventory processing workflow"""
    # To schedule daily: prefect deploy with cron schedule "0 0 * * *"
    wait_for_ftp_file()
    raw_file = download_vendor_file()
    cleansed_file = cleanse_vendor_data(raw_file)
    merge_with_internal_inventory(cleansed_file)

if __name__ == "__main__":
    vendor_inventory_pipeline()