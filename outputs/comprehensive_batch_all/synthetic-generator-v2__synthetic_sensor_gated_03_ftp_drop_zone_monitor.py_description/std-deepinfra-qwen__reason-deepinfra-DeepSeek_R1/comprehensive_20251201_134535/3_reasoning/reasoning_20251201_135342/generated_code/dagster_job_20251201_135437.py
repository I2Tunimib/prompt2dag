<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let me start by understanding the requirements. The pipeline is sensor-gated, so the first step is creating a sensor that checks for the vendor_inventory.csv file on an FTP server. But wait, Dagster uses sensors differently than Airflow. In Dagster, sensors are separate from jobs; they can trigger runs when certain conditions are met. However, the user wants the job to execute only when the sensor detects the file. Hmm, maybe I should model the sensor as part of the job's ops. But sensors in Dagster are more about triggering runs, not within a job's op. Wait, the original description mentions a sensor task that blocks processing until the file is found. So perhaps the sensor is part of the job's execution flow. But in Dagster, sensors are not ops. So maybe I need to simulate the waiting within an op. Alternatively, use a sensor to launch the job once the file is detected. But the user's constraints say to use ops and a job with dependencies. So perhaps the 'Wait For Ftp File' is an op that polls the FTP server. But how to handle the polling within an op? Because ops are supposed to be units of computation, and if an op is supposed to wait until a file is present, that might not be straightforward. The original description mentions the sensor task times out after 5 minutes. So maybe the op needs to implement a loop that checks for the file every 30 seconds, up to 5 minutes. That could be done within the op's logic. Let me proceed with that approach.

So the first op is wait_for_ftp_file. It will check the FTP server every 30 seconds, up to 5 minutes. If the file is found, it proceeds; otherwise, it raises an exception. Then, the next ops download, cleanse, and merge follow in sequence.

Now, for the FTP operations. The user mentioned using a custom FTPFileSensor in the original pipeline. In Dagster, I'll need to implement the FTP checking within the op. So the wait_for_ftp_file op will use some FTP client library, like ftplib, to connect and check for the file. But since the user wants minimal resource/config examples, perhaps I should include a config schema for FTP connection details. Let me add a config schema for the FTP server settings (host, username, password, etc.). The op can use these config values to connect.

Next, the download_vendor_file op will take the FTP config and download the file. Then cleanse_vendor_data will process it, and merge_with_internal_inventory will merge with internal data.

The dependencies are linear: each op depends on the previous one. So in the job definition, the order is set accordingly.

Regarding retries: the user mentioned a retry mechanism with 2 retries and 5-minute delay. In Dagster, you can set retry policies on ops. So I'll add a RetryPolicy to each op with max_retries=2 and delay=300 seconds.

The scheduling part says the DAG is scheduled daily, but since Dagster jobs can be scheduled separately, but the user wants the code for the job module. The sensor-gated pattern here is handled by the wait_for_ftp_file op, which ensures that the job doesn't proceed unless the file is present. However, in Dagster, if the job is run on a schedule, the wait_for_ftp_file op would check each time. But the original pipeline uses a sensor to gate execution. So perhaps the job is triggered by a sensor that checks for the file, but the user's constraints specify to model the sensor as an op. Wait, the constraints say: "If you need resources/config, provide simplified resource or config examples as comments or minimal stubs." So maybe the sensor is part of the job's ops. But in Dagster, sensors are not ops. So perhaps the initial op is simulating the sensor's behavior by waiting and checking.

Putting it all together:

- Create an op wait_for_ftp_file that checks for the file, with retries.
- Then the subsequent ops in sequence.
- Each op has a retry policy.
- The job sets the dependencies between the ops.

Now, implementing the wait_for_ftp_file op. The op needs to loop, checking every 30 seconds for up to 5 minutes. Let's calculate the number of checks: 5 minutes is 300 seconds, divided by 30 seconds interval gives 10 checks. So a loop that runs 10 times, each time checking if the file exists. If found, return; else, after 10 attempts, raise an exception.

For FTP operations, use ftplib. But in code, need to handle the connection. The config will have the necessary parameters.

The download op will then download the file. Cleanse op processes it, merge op combines with internal data.

Now, code structure:

- Imports: from dagster import op, job, RetryPolicy, get_dagster_logger.
- Each op is decorated with @op, and the job with @job.
- The config for FTP is handled via the op's config_schema.
- The wait_for_ftp_file op uses the config to connect to FTP, checks for the file, retries as per the policy.
- The other ops are straightforward, with placeholders for the actual processing (since the user didn't provide implementation details for functions like download_vendor_file, etc.).
- The job sets the linear dependencies.

Retry policies: Add a RetryPolicy to each op with max_retries=2 and delay=300 seconds (5 minutes). Wait, the user's retry mechanism allows 2 retries with 5-minute delay. So each op should have this policy. So in the job definition, when composing the ops, perhaps set the retry policy via the op's config. Alternatively, use the @op decorator's retry_policy parameter. Yes, each op can have a retry_policy specified.

Wait, but the user's pipeline has a retry mechanism allowing 2 retries with 5-minute delay between attempts. So each op should have this. So in each @op decorator, add retry_policy=RetryPolicy(max_retries=2, delay=300).

But in the code, how to apply this? Let me check:

Yes, when defining the op, you can set the retry_policy parameter. So:

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_for_ftp_file(context):
    ...

Similarly for the other ops. But wait, the original pipeline's retry is for task failures. So each op should have this retry policy.

Now, the FTP config. The wait_for_ftp_file and download_vendor_file ops need FTP connection details. So they can have a config schema that includes host, username, password, etc. Let's define a config schema using the Dagster ConfigMapping or the op's config_schema.

For simplicity, each op that needs FTP config can have a config_schema. For example:

@op(config_schema={
    "host": str,
    "username": str,
    "password": str,
    "remote_path": str,
    "local_path": str,
})
def wait_for_ftp_file(context):
    host = context.op_config["host"]
    ...

But perhaps using a resource for FTP connection would be better, but the user said to provide minimal stubs. So using config is acceptable here.

Wait, but the download_vendor_file op also needs to connect to FTP. So both ops would have similar config schemas. Alternatively, create a shared config structure. But for simplicity, perhaps each op has its own config, but that's redundant. Alternatively, the job's config can include the FTP settings, and the ops access them via context.op_config. But in Dagster, each op can have its own config. However, if the config is shared between ops, the job's config would need to include those parameters. Hmm, perhaps better to have each op's config_schema include the necessary parameters. But that might lead to duplication. Alternatively, use a resource for the FTP connection. Let me think: resources in Dagster are for external connections, so creating an FTP resource would make sense. But the user's constraints say to provide simplified resource examples as comments or minimal stubs. So perhaps create a resource for FTP, but in the code, just stub it out.

But maybe it's easier to handle via config for this example. So each op that needs FTP access will have config parameters. For example:

The wait_for_ftp_file op's config includes host, username, password, remote_file (which is vendor_inventory.csv), and maybe timeout and poke_interval. Wait, the timeout is 5 minutes, poke_interval is 30 seconds. But these could be hard-coded in the op, or set via config. The original description says the sensor is configured with poke_interval 30s and timeout 5m. So perhaps those are parameters for the wait_for_ftp_file op. So the config for that op could include:

poke_interval: 30
timeout: 300

But maybe those are fixed, so the op's code can have them as constants. Since the user's example is to be minimal, perhaps hard-code those values in the op's logic, while the FTP connection details come from config.

So, the wait_for_ftp_file op's config_schema includes host, username, password, remote_path (maybe the path where the file is located). The op will check for the existence of vendor_inventory.csv in that remote path.

Then, the download_vendor_file op's config includes local_path (where to save the file), and the same FTP connection details. So its config_schema is similar.

But this leads to duplication. Alternatively, the job's config could have an 'ftp' section with host, username, password, remote_path, local_path, etc., and each op accesses the relevant parts. But in Dagster, each op's config is nested under its own name unless using a shared configuration. Hmm, perhaps using a resource is better here, but given the user's constraints, maybe proceed with config.

Alternatively, for simplicity, have each op take the necessary config parameters. For example:

Wait_for_ftp_file op needs host, user, password, remote_file_path (default to 'vendor_inventory.csv'), poke_interval (30), timeout (300).

Download_vendor_file needs host, user, password, remote_file_path, local_directory.

But maybe the remote_file_path is the same for both ops. So in the job's config, these parameters can be specified once, and each op's config references them. But in Dagster, each op's config is separate. So perhaps the job's configuration includes these parameters under each op's config key. For example:

{
  "ops": {
    "wait_for_ftp_file": {
      "host": "ftp.example.com",
      "username": "user",
      "password": "pass",
      "remote_file": "vendor_inventory.csv",
      "timeout": 300,
      "poke_interval": 30
    },
    "download_vendor_file": {
      "host": "ftp.example.com",
      "username": "user",
      "password": "pass",
      "remote_file": "vendor_inventory.csv",
      "local_path": "/tmp"
    }
  }
}

But this would require each op to have its own config section. That's acceptable, but perhaps for brevity, the example can have minimal config with comments indicating where to fill in the details.

Alternatively, use default values in the config_schema so that the user can omit them if they match the defaults. For example, remote_file could default to 'vendor_inventory.csv', timeout to 300, etc.

So, in code:

For wait_for_ftp_file op:

@op(
    config_schema={
        "host": str,
        "username": str,
        "password": str,
        "remote_file": {"type": str, "default": "vendor_inventory.csv"},
        "timeout": {"type": int, "default": 300},
        "poke_interval": {"type": int, "default": 30},
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def wait_for_ftp_file(context):
    # Implementation here

Similarly for download_vendor_file:

@op(
    config_schema={
        "host": str,
        "username": str,
        "password": str,
        "remote_file": {"type": str, "default": "vendor_inventory.csv"},
        "local_path": {"type": str, "default": "/tmp"},
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def download_vendor_file(context):
    # Download code here

But this approach requires the same host, username, password to be specified in each op's config. Alternatively, use a resource to manage the FTP connection. Let me think: creating an FTP resource would encapsulate the connection details, and the ops can use the resource. For example:

from dagster import resource

@resource(config_schema={
    "host": str,
    "username": str,
    "password": str,
})
def ftp_resource(init_context):
    # Return an FTP client configured with the details
    return FTPClient(init_context.resource_config["host"], ...)

Then, the ops can require the resource. But the user's constraints mention to provide simplified resource examples as comments or stubs. So perhaps include the resource as a commented example or a stub.

But given the code needs to be executable, perhaps proceed with the config approach, even if it's a bit redundant.

Now, implementing the wait_for_ftp_file op. The code needs to connect to FTP, check for the file, and wait until it's found or timeout.

Using ftplib, the code would be something like:

from ftplib import FTP
import time

def wait_for_ftp_file(context):
    config = context.op_config
    host = config["host"]
    username = config["username"]
    password = config["password"]
    remote_file = config["remote_file"]
    timeout = config["timeout"]
    poke_interval = config["poke_interval"]

    start_time = time.time()
    attempts = 0
    max_attempts = timeout // poke_interval

    logger = get_dagster_logger()

    while time.time() - start_time < timeout:
        try:
            with FTP(host) as ftp:
                ftp.login(username, password)
                files = ftp.nlst()
                if remote_file in files:
                    logger.info(f"File {remote_file} found.")
                    return
                else:
                    logger.info(f"File not found, waiting {poke_interval} seconds.")
                    time.sleep(poke_interval)
                    attempts += 1
        except Exception as e:
            logger.error(f"FTP connection failed: {e}")
            raise

    raise Exception(f"File {remote_file} not found after {timeout} seconds.")

But this code has a few issues. The FTP connection is established each time, which may not be efficient, but for the sake of example, it's acceptable. Also, the ftp.nlst() may throw an error if the file is not present, but in the example, we check if the file is in the list. Alternatively, use a try-except around ftp.size(remote_file) to check if the file exists.

Wait, ftp.nlst() returns a list of filenames in the current directory. But if the remote_file is in a different directory, we need to change to that directory first. The original pipeline description mentions the file is named vendor_inventory.csv, but the path isn't specified. So perhaps the config should include a remote_path parameter. Let me adjust the config_schema for wait_for_ftp_file to include remote_path.

So, updating the config_schema:

"remote_path": {"type": str, "default": "."},
"remote_file": {"type": str, "default": "vendor_inventory.csv"},

Then, in the code:

ftp.cwd(config["remote_path"])
files = ftp.nlst()

But ftp.nlst() may not work for all servers; some may require passive mode, etc. But for the example, this is acceptable.

Now, the download_vendor_file op. It needs to download the file from FTP to a local path. The code would be:

@op(
    config_schema={
        "host": str,
        "username": str,
        "password": str,
        "remote_path": {"type": str, "default": "."},
        "remote_file": {"type": str, "default": "vendor_inventory.csv"},
        "local_path": {"type": str, "default": "/tmp"},
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def download_vendor_file(context):
    config = context.op_config
    host = config["host"]
    username = config["username"]
    password = config["password"]
    remote_path = config["remote_path"]
    remote_file = config["remote_file"]
    local_path = config["local_path"]

    logger = get_dagster_logger()

    local_filepath = f"{local_path}/{remote_file}"

    try:
        with FTP(host) as ftp:
            ftp.login(username, password)
            ftp.cwd(remote_path)
            with open(local_filepath, 'wb') as f:
                ftp.retrbinary(f"RETR {remote_file}", f.write)
        logger.info(f"Downloaded {remote_file} to {local_filepath}")
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        raise

    return local_filepath

This op returns the local file path, which can be passed to the next op.

The cleanse_vendor_data op takes the file path, processes it (removing nulls in product_id, quantity, price), and returns the cleansed data. Since the user didn't provide the actual functions, I'll stub this with pandas.

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def cleanse_vendor_data(context, file_path):
    import pandas as pd

    logger = get_dagster_logger()
    logger.info("Cleansing vendor data")

    df = pd.read_csv(file_path)
    # Drop rows where product_id, quantity, or price is null
    df = df.dropna(subset=['product_id', 'quantity', 'price'])
    cleansed_path = "/tmp/cleansed_vendor_inventory.csv"
    df.to_csv(cleansed_path, index=False)
    return cleansed_path

The merge_with_internal_inventory op takes the cleansed data and merges with internal inventory. Again, using a stub.

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def merge_with_internal_inventory(context, cleansed_file):
    import pandas as pd

    logger = get_dagster_logger()
    logger.info("Merging with internal inventory")

    # Assume internal inventory is in a fixed path for example
    internal_df = pd.read_csv("/path/to/internal_inventory.csv")
    vendor_df = pd.read_csv(cleansed_file)

    merged_df = pd.merge(internal_df, vendor_df, on='product_id', how='left')
    # Update stock levels and pricing (example)
    merged_df['stock'] = merged_df['quantity']
    merged_df['price'] = merged_df['price_y'].fillna(merged_df['price_x'])
    # Save or update internal system (stubbed)
    merged_df.to_csv("/path/to/updated_inventory.csv", index=False)
    return "/path/to/updated_inventory.csv"

Now, the job definition:

@job
def inventory_pipeline():
    file_path = download_vendor_file(wait_for_ftp_file())
    cleansed = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleansed)

Wait, but in Dagster, the dependencies are set by passing the output of one op to another. So the job function should call the ops in order, passing their outputs. So:

def inventory_pipeline():
    wait_result = wait_for_ftp_file()
    download_result = download_vendor_file(wait_result)
    cleanse_result = cleanse_vendor_data(download_result)
    merge_result = merge_with_internal_inventory(cleanse_result)

But wait, the wait_for_ftp_file op doesn't return anything except signaling that the file is present. The download_vendor_file op doesn't need input from wait_for_ftp_file except the order. So perhaps the dependencies are set by having download_vendor_file depend on wait_for_ftp_file, even if no data is passed. In Dagster, if an op doesn't take inputs from another op, you can use start_after to enforce order. But the standard way is to have the output of one op be the input of the next. However, in this case, the wait_for_ftp_file doesn't produce an output that download_vendor_file needs, except triggering the next step. So perhaps the download_vendor_file doesn't take any input, but depends on wait_for_ftp_file. But how to model that?

In Dagster, ops can have no inputs. So the job can be structured as:

@job
def inventory_pipeline():
    cleanse_data = cleanse_vendor_data(download_vendor_file())
    merge_with_internal_inventory(cleanse_data)
    download_vendor_file().after(wait_for_ftp_file())

Wait, no. The correct way is to define the dependencies via the data flow. If download_vendor_file must run after wait_for_ftp_file, but doesn't take any input from it, then you can have wait_for_ftp_file() followed by download_vendor_file(), but since download_vendor_file doesn't take any input, you can use .alias to chain them. Alternatively, have the wait_for_ftp_file return a value (like a success flag) that is passed to download_vendor_file. For example:

@op
def wait_for_ftp_file(context):
    # ... code ...
    return True  # or some token

@op
def download_vendor_file(context, _):
    # ... code ...

Then, in the job:

download_vendor_file(wait_for_ftp_file())

But in the current code, the download_vendor_file op doesn't take any inputs. So perhaps modify the ops to pass a token. Alternatively, structure the job to enforce order via data dependencies. For example, have wait_for_ftp_file return a value that download_vendor_file requires, even if it's just a trigger.

In the current code, the wait_for_ftp_file op doesn't return anything. So perhaps modify it to return a success message or a timestamp, which is then passed to download_vendor_file as an input. But download_vendor_file doesn't need that input. So perhaps the download_vendor_file op is defined to take no inputs but depends on wait_for_ftp_file via Dagster's dependency system.

Wait, in Dagster, if an op doesn't have any inputs, it can run in parallel with others unless there's an explicit dependency. To enforce order, you can use the .after() method. So in the job definition:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file().after(wait)
    cleanse = cleanse_vendor_data(download)
    merge = merge_with_internal_inventory(cleanse)

But how to pass the output of download to cleanse? Because download_vendor_file returns the local file path, which cleanse_vendor_data takes as input. So the code should be:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

Wait, but wait_for_ftp_file returns a value (like True), which is passed to download_vendor_file. But download_vendor_file's function doesn't take any input. So this would cause an error. Therefore, the download_vendor_file op should not depend on the output of wait_for_ftp_file. Instead, the dependency is purely based on execution order, not data flow. So the correct way is to use .after().

So:

@job
def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file().after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But download_vendor_file produces an output (the file path), which cleanse_vendor_data takes as input. So the code should be:

download = download_vendor_file()
cleanse = cleanse_vendor_data(download)

But then, how to ensure download runs after wait?

Ah, right. In Dagster, if you want to enforce order between ops that don't have data dependencies, you have to use .after(). But in this case, download_vendor_file doesn't take any input from wait_for_ftp_file. So the correct approach is to have download_vendor_file depend on wait_for_ftp_file via .after(). But since download_vendor_file's function doesn't take any input, the code would be:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file().after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But wait, download_vendor_file's op doesn't have any inputs, so when we call download_vendor_file(), it's an op without dependencies. Then, .after(wait) adds the dependency. Then, cleanse_vendor_data depends on download's output. So this should work.

But in the code I wrote earlier, the download_vendor_file op returns the local file path, which is passed to cleanse_vendor_data. So the code should be:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file()
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)
    download.after(wait)

But in Dagster, the syntax is different. You have to structure the dependencies via the function calls. So perhaps the correct way is:

def inventory_pipeline():
    wait_result = wait_for_ftp_file()
    download_result = download_vendor_file(start_after=wait_result)
    cleanse_result = cleanse_vendor_data(download_result)
    merge_with_internal_inventory(cleanse_result)

Wait, no. The start_after is not a valid parameter. The correct way is to use the function's return value to define the data flow. If download_vendor_file doesn't take any input from wait_for_ftp_file, then you can't pass wait_result to it. Therefore, you need to enforce the execution order using .after().

So the correct job definition would be:

@job
def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file()
    download = download.after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But in Dagster's Python API, you can't reassign download like that. Instead, you need to use the method within the job function. The correct syntax is:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file()
    download = download.after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But this may not work as intended. Alternatively, use the following structure:

def inventory_pipeline():
    download = download_vendor_file().after(wait_for_ftp_file())
    cleanse_vendor_data(download).alias("cleanse")  # If needed
    merge_with_internal_inventory(cleanse_vendor_data(download))

But this may not capture the dependencies correctly. Alternatively, chain the dependencies properly:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file().after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

Yes, this should work. Because download_vendor_file is set to run after wait_for_ftp_file, and cleanse_vendor_data depends on download_vendor_file's output.

But wait, download_vendor_file returns the local file path, which is passed as input to cleanse_vendor_data. So the code should be:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file().after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But in this case, download_vendor_file is called with no inputs, but depends on wait via .after(). Then, its output is passed to cleanse_vendor_data. That should work because download_vendor_file's output is the local file path, which cleanse_vendor_data takes as input.

Wait, no. The download_vendor_file op is called with no inputs, but its execution is after wait. Its output is generated by its own execution, which is correct. Then, cleanse_vendor_data takes the output of download_vendor_file as input. So the dependencies are:

wait -> download -> cleanse -> merge.

Yes, that's correct.

So the job function is correctly defined.

Now, the main block:

if __name__ == "__main__":
    inventory_pipeline.execute_in_process(run_config={
        "ops": {
            "wait_for_ftp_file": {
                "config": {
                    "host": "ftp.example.com",
                    "username": "user",
                    "password": "pass",
                    "remote_path": "/remote/dir",
                    "remote_file": "vendor_inventory.csv"
                }
            },
            "download_vendor_file": {
                "config": {
                    "host": "ftp.example.com",
                    "username": "user",
                    "password": "pass",
                    "remote_path": "/remote/dir",
                    "local_path": "/tmp"
                }
            }
        }
    })

This provides the necessary config for the ops. Note that each op's config is under their own key in the run_config's "ops" section.

Putting all together, the code should be:

from dagster import op, job, RetryPolicy, get_dagster_logger
from ftplib import FTP
import time
import pandas as pd

@op(
    config_schema={
        "host": str,
        "username": str,
        "password": str,
        "remote_path": {"type": str, "default": "."},
        "remote_file": {"type": str, "default": "vendor_inventory.csv"},
        "timeout": {"type": int, "default": 300},
        "poke_interval": {"type": int, "default": 30},
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def wait_for_ftp_file(context):
    logger = get_dagster_logger()
    config = context.op_config
    host = config["host"]
    username = config["username"]
    password = config["password"]
    remote_path = config["remote_path"]
    remote_file = config["remote_file"]
    timeout = config["timeout"]
    poke_interval = config["poke_interval"]

    start_time = time.time()
    max_duration = timeout

    while time.time() - start_time < max_duration:
        try:
            with FTP(host) as ftp:
                ftp.login(username, password)
                ftp.cwd(remote_path)
                files = ftp.nlst()
                if remote_file in files:
                    logger.info(f"Found {remote_file}")
                    return True  # Signal to proceed
                else:
                    logger.info(f"File not found, waiting {poke_interval} seconds")
                    time.sleep(poke_interval)
        except Exception as e:
            logger.error(f"FTP error: {str(e)}")
            time.sleep(poke_interval)

    raise Exception(f"File {remote_file} not found after {timeout} seconds")

@op(
    config_schema={
        "host": str,
        "username": str,
        "password": str,
        "remote_path": {"type": str, "default": "."},
        "remote_file": {"type": str, "default": "vendor_inventory.csv"},
        "local_path": {"type": str, "default": "/tmp"},
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def download_vendor_file(context):
    logger = get_dagster_logger()
    config = context.op_config
    host = config["host"]
    username = config["username"]
    password = config["password"]
    remote_path = config["remote_path"]
    remote_file = config["remote_file"]
    local_path = config["local_path"]

    local_file = f"{local_path}/{remote_file}"

    try:
        with FTP(host) as ftp:
            ftp.login(username, password)
            ftp.cwd(remote_path)
            with open(local_file, 'wb') as f:
                ftp.retrbinary(f"RETR {remote_file}", f.write)
        logger.info(f"Downloaded {remote_file} to {local_file}")
        return local_file
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        raise

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def cleanse_vendor_data(context, file_path):
    logger = get_dagster_logger()
    logger.info("Cleansing data")

    df = pd.read_csv(file_path)
    df = df.dropna(subset=['product_id', 'quantity', 'price'])
    cleansed_path = "/tmp/cleansed_vendor_inventory.csv"
    df.to_csv(cleansed_path, index=False)
    return cleansed_path

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def merge_with_internal_inventory(context, cleansed_path):
    logger = get_dagster_logger()
    logger.info("Merging with internal inventory")

    # Assume internal inventory path; in real scenario, this would be configurable
    internal_df = pd.read_csv("/path/to/internal_inventory.csv")
    vendor_df = pd.read_csv(cleansed_path)

    merged_df = pd.merge(internal_df, vendor_df, on='product_id', how='left')
    # Example merge logic
    merged_df['stock'] = merged_df['quantity'].fillna(merged_df['stock'])
    merged_df['price'] = merged_df['price_y'].fillna(merged_df['price_x'])
    merged_df.to_csv("/path/to/updated_inventory.csv", index=False)
    return "/path/to/updated_inventory.csv"

@job
def inventory_pipeline():
    merge_with_internal_inventory(
        cleanse_vendor_data(
            download_vendor_file().after(
                wait_for_ftp_file()
            )
        )
    )

if __name__ == "__main__":
    inventory_pipeline.execute_in_process(run_config={
        "ops": {
            "wait_for_ftp_file": {
                "config": {
                    "host": "ftp.example.com",
                    "username": "user",
                    "password": "pass",
                    "remote_path": "/uploads",
                    "remote_file": "vendor_inventory.csv"
                }
            },
            "download_vendor_file": {
                "config": {
                    "host": "ftp.example.com",
                    "username": "user",
                    "password": "pass",
                    "remote_path": "/uploads",
                    "local_path": "/tmp"
                }
            }
        }
    })

Wait, but in the job definition, the way the dependencies are structured is a bit complex. Let me re-express the job function more clearly:

@job
def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file().after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But how to write this in the function? The function body should express the dependencies via the data flow. So:

def inventory_pipeline():
    wait_result = wait_for_ftp_file()
    download_result = download_vendor_file(start=wait_result)
    cleanse_result = cleanse_vendor_data(download_result)
    merge_with_internal_inventory(cleanse_result)

But no, the .after() method is used to set dependencies between ops that don't have data dependencies. However, in this case, download_vendor_file doesn't take any input from wait_for_ftp_file, but needs to run after it. So the correct way is:

def inventory_pipeline():
    wait = wait_for_ftp_file()
    download = download_vendor_file()
    download = download.after(wait)
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

But in Python, reassigning 'download' may not be the right approach. Alternatively, structure it as:

def inventory_pipeline():
    download = download_vendor_file().after(wait_for_ftp_file())
    cleanse = cleanse_vendor_data(download)
    merge_with_internal_inventory(cleanse)

Yes, this should work. Because download_vendor_file is set to run after wait_for_ftp_file, and cleanse