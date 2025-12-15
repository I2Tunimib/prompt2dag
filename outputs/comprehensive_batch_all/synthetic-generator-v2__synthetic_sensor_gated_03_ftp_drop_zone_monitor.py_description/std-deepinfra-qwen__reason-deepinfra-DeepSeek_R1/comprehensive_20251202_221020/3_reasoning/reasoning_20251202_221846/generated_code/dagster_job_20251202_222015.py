<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline uses a sensor to check for an FTP file, then proceeds through several steps in sequence. 

First, the sensor. Dagster has sensors, but the user mentioned using ops and a job. Wait, the problem says to use ops and a job with dependencies. But the first step is a sensor that polls the FTP. Hmm, maybe the sensor isn't part of the job itself but triggers the job. But the user's constraints specify to model everything within the job using ops. Wait, the original description mentions a sensor-gated pipeline where the sensor task blocks processing. So perhaps the sensor is an op that waits for the file. But in Dagster, sensors are typically used to launch runs when a condition is met. However, the user wants the sensor as part of the job. That might not be standard. Maybe they want to simulate the sensor within an op.

Wait, the user's constraints say: "Use Dagster ops (@op) to represent each step and a job (@job) that sets dependencies." So the sensor task (Wait For Ftp File) is an op. So perhaps the first op is a sensor-like op that polls the FTP server. But how to handle the waiting within an op? Normally, ops are supposed to execute and return, not block. But maybe in this case, the op is designed to check once and succeed if the file exists, otherwise fail. But the description says the sensor polls every 30 seconds and times out after 5 minutes. So perhaps the op needs to retry with a delay until the file is found or timeout.

Dagster allows configuring retries with a delay. So maybe the Wait For Ftp File op is set to retry with a delay of 30 seconds, up to a maximum of 10 retries (since 5 minutes is 300 seconds, divided by 30 seconds per retry gives 10 retries). If after 10 retries the file isn't found, it fails. That would model the timeout.

So the first op would check for the file. If not found, raise a failure, which triggers a retry with delay. The op's retry policy would be set to allow 10 retries with 30 seconds delay. That way, each retry is a 'poke', and after 10 retries (5 minutes), it fails.

So the Wait For Ftp File op would have a retry policy. Then, the subsequent ops depend on it succeeding.

Next, the Download Vendor File op would run after the sensor op succeeds. Then Cleanse Vendor Data, then Merge With Internal Inventory.

For the retry mechanism mentioned in the orchestration, each task can have its own retry policy. The problem states the pipeline allows 2 retries with 5-minute delay. So each op should have a retry policy with max_retries=2 and delay=300 seconds.

Now, the code structure. Start by importing necessary modules: dagster's op, job, RetryPolicy, etc.

Each op is decorated with @op, and the job sets the dependencies via the order of execution. The retry policies can be set at the job level or per op. Since the problem says the pipeline is configured with a retry mechanism allowing 2 retries with 5-minute delay, perhaps each op uses this policy. So the job can specify a default retry policy.

Wait, the user's constraints mention if resources/config are needed, provide stubs. The FTP server connection details would be a resource. So create an FTPResource class, maybe as a stub.

The Wait For Ftp File op would take the FTP resource as a parameter. Similarly, the Download Vendor File op would use the FTP resource to download the file.

The functions inside the ops (like download_vendor_file, cleanse_vendor_data, merge_with_internal_inventory) are stubbed since the actual implementation details aren't provided. But the code should be executable, so minimal implementations with pass or dummy data.

Putting it all together:

- Define the retry policy: RetryPolicy(max_retries=2, delay=300).
- Define the FTP resource as a stub.
- Create each op with the necessary parameters and dependencies.
- The job uses the default retry policy and includes the resource.

Wait, but the sensor (Wait For Ftp File) needs a different retry policy. Because the sensor's retry is for polling every 30 seconds up to 5 minutes. That's 10 retries with 30 seconds delay. But the pipeline's retry mechanism allows 2 retries with 5-minute delay. So perhaps the sensor op has its own retry policy, and the other ops use the default.

So in the job, we can set a default retry policy of 2 retries with 5 minutes delay. Then, override the sensor op's retry policy to have max_retries=10, delay=30. But how to do that in Dagster?

In Dagster, you can set retry policies at the op level or job level. So for the sensor op, specify retry_policy=RetryPolicy(max_retries=10, delay=30), and the other ops inherit the job's default retry policy.

Wait, the job can have a default_retry_policy, but individual ops can override it. So in the job decorator, set the default, and in the sensor op, set its own retry policy.

So:

@job(
    resource_defs={"ftp": FTPResource},
    config={
        "ops": {
            "wait_for_ftp_file": {
                "config": {
                    "file_name": "vendor_inventory.csv",
                    "timeout": 300,
                }
            }
        }
    },
    # Default retry policy for all ops unless overridden
    default_retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def vendor_inventory_job():
    wait = wait_for_ftp_file()
    download = download_vendor_file(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But the sensor op (wait_for_ftp_file) needs a retry policy that allows 10 retries with 30 seconds delay. So in the @op decorator for wait_for_ftp_file, specify retry_policy=RetryPolicy(max_retries=10, delay=30). That would override the job's default.

Yes. So the code would have:

@op(
    required_resource_keys={"ftp"},
    retry_policy=RetryPolicy(max_retries=10, delay=30),
)
def wait_for_ftp_file(context):
    # code that checks for the file
    # if not found, raise Failure, which triggers retry
    file_name = context.op_config["file_name"]
    if not context.resources.ftp.check_file_exists(file_name):
        raise Failure("File not found")
    return file_name

But how does the op know to retry until the file is found or until max_retries is reached? Because each time the op fails (raises a Failure), Dagster will retry according to the retry policy. So if the op has a retry policy with max_retries=10, and each retry happens after 30 seconds, then after 10 retries (total time 10*30=300 seconds =5 minutes), it will stop retrying and mark the op as failed.

So the logic inside the op is: check if the file exists. If not, raise Failure. The retry policy will cause it to retry up to 10 times with 30s delay. If after 10 retries it's still not found, the op fails, and the job doesn't proceed.

Once the file is found, the op succeeds, and the next ops proceed.

For the FTPResource, create a stub class with a check_file_exists method and a download_file method.

Now, the Download Vendor File op takes the file name from the previous op, uses the FTP resource to download it, saves to a temp dir, and returns the path.

The Cleanse Vendor Data op reads the file, removes nulls in specified columns, returns cleansed data.

The Merge op takes the cleansed data and merges with internal inventory (another stub function).

Implementing all this with minimal code, using pandas for data processing (assuming it's installed), but since the user said to make the code executable, need to include import statements.

Wait, but the user didn't mention specific packages. The original pipeline uses PythonOperator, which implies Airflow, but here we're using Dagster. So for data processing, using pandas would make sense, but need to ensure that the code doesn't have actual dependencies beyond Dagster unless necessary. However, the user said to assume required packages are installed. So including pandas is okay.

So code outline:

- Import necessary modules: dagster, pandas as pd, maybe os for temp dir.
- Define FTPResource stub.
- Define the ops with their logic.
- Define the job with dependencies and config.
- Add the main block to execute the job.

Putting it all together:

from dagster import op, job, RetryPolicy, Failure, ResourceParam
import pandas as pd
import os
from datetime import datetime

# Stub FTP resource
class FTPResource:
    def check_file_exists(self, file_name):
        # Implement actual check here
        return False  # Simulated; would check FTP in real scenario

    def download_file(self, remote_path, local_path):
        # Implement actual download here
        print(f"Simulating download from {remote_path} to {local_path}")

# Resources can be configured with connection details
# Example config for FTP resource (commented):
# resources:
#   ftp:
#     config:
#       host: "ftp.example.com"
#       username: "user"
#       password: "pass"

@op(
    required_resource_keys={"ftp"},
    retry_policy=RetryPolicy(max_retries=10, delay=30),
)
def wait_for_ftp_file(context) -> str:
    file_name = context.op_config["file_name"]
    if not context.resources.ftp.check_file_exists(file_name):
        raise Failure(f"File {file_name} not found on FTP server")
    context.log.info(f"File {file_name} detected")
    return file_name

@op(
    required_resource_keys={"ftp"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def download_vendor_file(context, file_name: str) -> str:
    temp_dir = "/tmp/vendor_files"
    os.makedirs(temp_dir, exist_ok=True)
    local_path = os.path.join(temp_dir, file_name)
    context.resources.ftp.download_file(file_name, local_path)
    context.log.info(f"Downloaded file to {local_path}")
    return local_path

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def cleanse_vendor_data(context, file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    required_columns = ["product_id", "quantity", "price"]
    for col in required_columns:
        df = df.dropna(subset=[col])
    context.log.info(f"Cleansed data: {len(df)} rows remaining")
    return df

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def merge_with_internal_inventory(context, vendor_df: pd.DataFrame):
    # Stub internal inventory system integration
    internal_df = pd.read_csv("internal_inventory.csv")  # Assume exists
    merged_df = pd.merge(vendor_df, internal_df, on="product_id", how="inner")
    # Simulate updating stock and pricing
    context.log.info(f"Merged {len(merged_df)} products")
    return merged_df

@job(
    resource_defs={"ftp": ResourceParam(FTPResource)},
    config={
        "ops": {
            "wait_for_ftp_file": {
                "config": {
                    "file_name": "vendor_inventory.csv",
                }
            }
        }
    },
    default_retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def vendor_inventory_job():
    file_name = wait_for_ftp_file()
    file_path = download_vendor_file(file_name)
    cleansed_data = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleansed_data)

if __name__ == "__main__":
    result = vendor_inventory_job.execute_in_process(run_config={
        "ops": {
            "wait_for_ftp_file": {
                "config": {
                    "file_name": "vendor_inventory.csv",
                }
            }
        }
    })

Wait, but in the job config, the timeout isn't used. The original sensor task times out after 5 minutes. But in our implementation, the retry policy of the wait_for_ftp_file op is set to max_retries=10 and delay=30, which totals 10*30=300 seconds (5 minutes). So the timeout is handled via the retry policy. The op's config doesn't need a timeout parameter. So the config for wait_for_ftp_file only needs the file_name.

The FTPResource is a class that's provided as a resource. In the job, resource_defs={"ftp": ResourceParam(FTPResource)} might not be correct. Wait, in Dagster, resources are instances or definitions. To provide a resource, you can use a resource definition. But in the code above, using ResourceParam might not be right. Instead, perhaps the resource is a class that's instantiated. Wait, the correct way is to define the resource as a resource definition. For a stub, maybe using a resource that's a class.

Wait, the FTPResource is a class, and in the resource_defs, we can provide a function that returns an instance. Or use a configured resource. Alternatively, since it's a stub, perhaps just pass the class as a resource. But Dagster resources are typically functions that return the resource object. So maybe:

@resource
def ftp_resource(context):
    return FTPResource()

Then, in the job, resource_defs={"ftp": ftp_resource}.

But the user's constraints say to provide simplified resource examples as comments or minimal stubs. So perhaps the code includes the FTPResource class and uses it as a resource.

Wait, the code currently has:

class FTPResource:
    def check_file_exists(self, file_name):
        # Implement actual check here
        return False  # Simulated; would check FTP in real scenario

    def download_file(self, remote_path, local_path):
        # Implement actual download here
        print(f"Simulating download from {remote_path} to {local_path}")

And in the job definition:

resource_defs={"ftp": ResourceParam(FTPResource)}

But ResourceParam is used when you want to pass a resource as a parameter to an op. Wait, no. Resources are defined using @resource or as objects, and then provided to the job via resource_defs. So perhaps the correct way is to create a resource definition for FTPResource.

But to keep it simple, maybe the code uses a class-based resource without a decorator. However, in Dagster, resources are typically defined using @resource. So perhaps:

from dagster import resource

@resource
def ftp_resource(_):
    return FTPResource()

Then, in the job, resource_defs={"ftp": ftp_resource}.

But the user's code should be executable. So including the @resource decorator.

Updating the code:

from dagster import op, job, RetryPolicy, Failure, resource

class FTPResource:
    def check_file_exists(self, file_name):
        # Stub implementation
        # In real scenario, connect to FTP and check
        return True  # For testing, assume file exists

    def download_file(self, remote_path, local_path):
        # Stub implementation
        print(f"Simulated download from {remote_path} to {local_path}")

@resource
def ftp_resource(_):
    return FTPResource()

Then, in the job:

@job(
    resource_defs={"ftp": ftp_resource},
    config={...},
    ...
)

This way, the resource is properly defined.

But in the original problem, the sensor task times out after 5 minutes. The wait_for_ftp_file op's retry policy is set to max_retries=10, delay=30, which gives 10*30=300 seconds=5 minutes. So if after 10 retries the file isn't found, the op fails, and the job doesn't proceed.

The other ops have the default retry policy of 2 retries with 5-minute delay.

Now, testing the code: when the wait_for_ftp_file op's check_file_exists returns False, it raises Failure, triggering a retry. After 10 retries, it stops.

But in the stub FTPResource, check_file_exists returns True for testing. So in the sample code, the wait_for_ftp_file op would succeed immediately.

But to simulate a case where the file isn't present, the stub could return False. However, for the code to be executable, perhaps the stub returns True so that the job can run without failure.

But the user's code must be executable. So the FTPResource's check_file_exists method returns True, allowing the op to proceed. The download method is a stub.

The other ops use pandas. So the code should import pandas and handle the data.

Putting it all together, the code would look like this.
</think>

from dagster import op, job, RetryPolicy, Failure, resource
import pandas as pd
import os
from typing import Any

class FTPConnection:
    def check_file_exists(self, file_name: str) -> bool:
        # Stub implementation - replace with actual FTP check
        return True  # Always return True for example execution

    def download_file(self, remote_path: str, local_path: str) -> None:
        # Stub implementation - replace with actual download logic
        print(f"Simulating download from {remote_path} to {local_path}")
        with open(local_path, "w") as f:
            f.write("product_id,quantity,price\n123,5,19.99")

@resource
def ftp_resource(_) -> FTPConnection:
    return FTPConnection()

@op(
    required_resource_keys={"ftp"},
    retry_policy=RetryPolicy(max_retries=10, delay=30)
)
def wait_for_ftp_file(context) -> str:
    file_name = "vendor_inventory.csv"
    if not context.resources.ftp.check_file_exists(file_name):
        raise Failure(f"File {file_name} not found")
    context.log.info(f"Detected file: {file_name}")
    return file_name

@op(
    required_resource_keys={"ftp"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def download_vendor_file(context, file_name: str) -> str:
    local_dir = "/tmp/vendor_data"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, file_name)
    context.resources.ftp.download_file(file_name, local_path)
    context.log.info(f"Downloaded to {local_path}")
    return local_path

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def cleanse_vendor_data(context, file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    required_cols = ["product_id", "quantity", "price"]
    clean_df = df.dropna(subset=required_cols)
    context.log.info(f"Cleaned {len(clean_df)} valid records")
    return clean_df

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def merge_with_internal_inventory(context, vendor_data: pd.DataFrame) -> Any:
    # Stub internal inventory merge
    context.log.info(f"Merged {len(vendor_data)} vendor records")
    return {"status": "success", "merged_records": len(vendor_data)}

@job(
    resource_defs={"ftp": ftp_resource},
    default_retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def vendor_inventory_pipeline():
    merge_with_internal_inventory(
        cleanse_vendor_data(
            download_vendor_file(
                wait_for_ftp_file()
            )
        )
    )

if __name__ == "__main__":
    result = vendor_inventory_pipeline.execute_in_process()