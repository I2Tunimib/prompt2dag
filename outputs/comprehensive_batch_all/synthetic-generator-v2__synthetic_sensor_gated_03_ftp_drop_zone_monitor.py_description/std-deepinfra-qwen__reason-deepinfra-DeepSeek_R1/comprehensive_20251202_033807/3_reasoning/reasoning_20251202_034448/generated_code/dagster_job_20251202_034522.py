<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline uses a sensor to check for a file on an FTP server, then proceeds with downloading, cleansing, and merging the data. The steps are linear, so dependencies are straightforward.

First, Dagster uses ops and jobs instead of Airflow's operators and DAGs. The sensor here is a bit tricky because Dagster sensors are separate from jobs. Wait, the user mentioned a sensor-gated pipeline, so maybe the sensor triggers the job when the file is found. But the original description says the sensor is a task within the pipeline. Hmm, in Airflow, sensors are tasks that wait for a condition. In Dagster, sensors are more like triggers that launch runs when a condition is met. So perhaps the sensor isn't part of the job itself but is an external trigger. But the problem states to model the pipeline as a job with ops, including the sensor as an op. Wait, but in Dagster, sensors aren't ops. Ops are the computational steps. So maybe the user wants the sensor-like behavior within an op.

Wait, the constraints say to use @op and @job. So the "Wait For Ftp File" step must be an op that polls the FTP server. But how to make an op that waits until the file is present? Normally, ops in Dagster run once and complete. To have an op that polls, perhaps we need to implement a loop inside the op. But that might block and not be ideal. Alternatively, maybe use a sensor outside the job to trigger the job when the file is present. But the problem says to model the pipeline as a job with ops, including the sensor as a task. So perhaps the first op is a sensor-like op that checks for the file and either succeeds or fails.

But the original description says the sensor task times out after 5 minutes. So the op needs to handle retries with a timeout. Dagster ops can have retries with delays. So maybe configure the op with retries to simulate polling. Let's see: if the op checks for the file, and if not found, raises a failure, and is configured to retry a certain number of times with a delay, until it either finds the file or exhausts retries.

The user's pipeline has the sensor task as part of the job, so the first op is Wait For Ftp File. Then the subsequent ops depend on it. So the job's structure is linear: wait_op -> download_op -> cleanse_op -> merge_op.

For the wait_op: It needs to check the FTP server for the file. If not found, it should fail, but with retries. The retry configuration can be set with @op with retry policy. The problem says the sensor has a poke interval of 30 seconds and a 5-minute timeout. So total retries would be (5*60)/30 = 10 retries. So retry policy with max_retries=10, delay=30 seconds. But if the op fails after 10 retries, the job fails. Otherwise, if during any retry the file is found, the op succeeds, and the job proceeds.

So the wait_op would be an op that checks for the file. If found, returns success; else, raises an exception. The retry policy allows it to retry up to 10 times with 30-second delays. That way, it mimics the sensor behavior.

Now, the code structure:

First, define the ops with @op, each representing a step. The wait_op uses a retry policy. Then, the job sets the dependencies in order.

For FTP configuration, perhaps use a resource. The user mentioned FTP server as an external dependency, so maybe create an FTPResource with config. But the problem says to provide simplified resource examples as comments or stubs. So perhaps add a resource config.

Wait, the problem says if resources/config are needed, provide simplified examples as comments or minimal stubs. So maybe define a resource for FTP connection details.

But the code needs to be executable. So let's create a config schema for the FTP server. The wait_op and download_op would require the FTP resource.

So steps:

1. Define an FTPResource as a configurable resource. Maybe with host, username, password, etc.

2. The wait_op uses this resource to check for the file. The op's logic is: connect to FTP, check if vendor_inventory.csv exists. If yes, return some value (maybe the filename). If not, raise an exception, which triggers a retry.

3. The download_op uses the FTP resource to download the file once the wait_op succeeds.

But how to pass the filename from wait_op to download_op? Since the wait_op's output can be passed as an input to download_op.

Wait, in the pipeline description, the sensor (wait_op) just detects the presence, and the download_op then retrieves it. So the download_op doesn't need the filename as input, since it's fixed as vendor_inventory.csv. But maybe the wait_op's output is a signal, and the download_op uses the resource to connect and download.

Alternatively, the wait_op could return the filename, but since it's fixed, maybe not necessary. So the download_op can just proceed once the wait_op completes.

So code outline:

from dagster import op, job, ResourceParam, RetryPolicy

class FTPResource:
    def __init__(self, config):
        self.host = config['host']
        self.username = config['username']
        self.password = config['password']
        # etc.

@op(retry_policy=RetryPolicy(max_retries=10, delay=30))
def wait_for_ftp_file(context, ftp: FTPResource):
    # Check if vendor_inventory.csv exists on FTP
    # If not, raise exception
    # Else, return
    import time
    start_time = time.time()
    # Simulate FTP check
    # For example, using ftplib
    from ftplib import FTP
    ftp_conn = FTP(ftp.host)
    ftp_conn.login(ftp.username, ftp.password)
    files = ftp_conn.nlst()
    if 'vendor_inventory.csv' not in files:
        context.log.info('File not found, retrying...')
        raise Exception('File not found')
    return 'vendor_inventory.csv'

@op
def download_vendor_file(context, ftp: FTPResource, filename: str):
    # Use ftp to download the file
    from ftplib import FTP
    import tempfile
    ftp_conn = FTP(ftp.host)
    ftp_conn.login(ftp.username, ftp.password)
    local_path = tempfile.gettempdir() + '/' + filename
    with open(local_path, 'wb') as f:
        ftp_conn.retrbinary('RETR ' + filename, f.write)
    context.log.info(f'Downloaded to {local_path}')
    return local_path

@op
def cleanse_vendor_data(context, file_path: str):
    import pandas as pd
    df = pd.read_csv(file_path)
    # Cleanse data: remove nulls in specific columns
    cols = ['product_id', 'quantity', 'price']
    df_clean = df.dropna(subset=cols)
    clean_path = file_path.replace('.csv', '_clean.csv')
    df_clean.to_csv(clean_path, index=False)
    context.log.info(f'Cleansed data saved to {clean_path}')
    return clean_path

@op
def merge_with_internal_inventory(context, clean_path: str):
    # Assume internal inventory is a CSV or database
    # For simplicity, read internal data and merge
    import pandas as pd
    df_vendor = pd.read_csv(clean_path)
    # Load internal data (stub)
    df_internal = pd.read_csv('internal_inventory.csv')  # Assume exists
    merged = pd.merge(df_internal, df_vendor, on='product_id', how='outer')
    # Update stock and price (example logic)
    merged['stock'] = merged['quantity'].fillna(merged['stock'])
    merged['price'] = merged['price_y'].fillna(merged['price_x'])
    # Save or update internal system (stub)
    merged.to_csv('merged_inventory.csv', index=False)
    context.log.info('Merged inventory updated')

@job(
    config={
        'resources': {
            'ftp': {
                'config': {
                    'host': 'ftp.example.com',
                    'username': 'user',
                    'password': 'pass',
                }
            }
        }
    }
)
def vendor_inventory_pipeline():
    filename = wait_for_ftp_file()
    file_path = download_vendor_file(filename)
    clean_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(clean_path)

if __name__ == '__main__':
    result = vendor_inventory_pipeline.execute_in_process()

Wait, but in Dagster, resources are provided via resource_defs, not config. So the above approach may not be correct. The config is for run-specific configuration, while resources are dependencies that ops can use. So perhaps define the FTP resource as a resource, and the job is configured with it.

But the code needs to be executable. Let's adjust:

In Dagster, resources are specified separately. So the FTPResource should be defined as a resource, and the job uses it. However, the user's constraints say to provide simplified resource examples as comments or minimal stubs. So perhaps use a stubbed resource.

Alternatively, use a configured resource. Let me adjust:

from dagster import op, job, resource, RetryPolicy

@resource(config_schema={
    'host': str,
    'username': str,
    'password': str,
})
def ftp_resource(context):
    # Return an FTP client configured with the resource config
    from ftplib import FTP
    ftp = FTP(context.resource_config['host'])
    ftp.login(context.resource_config['username'], context.resource_config['password'])
    return ftp

@op(retry_policy=RetryPolicy(max_retries=10, delay=30))
def wait_for_ftp_file(context):
    ftp = context.resources.ftp
    files = ftp.nlst()
    if 'vendor_inventory.csv' not in files:
        context.log.info('File not found, retrying...')
        raise Exception('File not found')
    return 'vendor_inventory.csv'

@op
def download_vendor_file(context, filename: str):
    ftp = context.resources.ftp
    import tempfile
    local_path = tempfile.gettempdir() + '/' + filename
    with open(local_path, 'wb') as f:
        ftp.retrbinary('RETR ' + filename, f.write)
    context.log.info(f'Downloaded to {local_path}')
    return local_path

# Similarly for other ops...

@job(resource_defs={'ftp': ftp_resource})
def vendor_inventory_pipeline():
    download_vendor_file(wait_for_ftp_file())

Wait, but the dependencies need to be set. The wait_for_ftp_file op's output is passed to download_vendor_file. So the job should chain the ops.

Wait, the code would be:

@job(resource_defs={'ftp': ftp_resource})
def vendor_inventory_pipeline():
    filename = wait_for_ftp_file()
    file_path = download_vendor_file(filename)
    clean_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(clean_path)

But each op that needs the FTP resource must request it via context. So the download_vendor_file op would need to have the resource. Wait, in the code above, the download_vendor_file op uses context.resources.ftp. So the op doesn't need to take ftp as a parameter, but accesses it via context.

So the ops wait_for_ftp_file and download_vendor_file both require the 'ftp' resource. So in Dagster, resources are made available via context.resources, provided the op requires the resource.

Thus, the ops need to specify required resources. Wait, in Dagster, ops can declare required resources using the 'required_resource_keys' parameter. So for example:

@op(required_resource_keys={'ftp'}, retry_policy=...)
def wait_for_ftp_file(context):
    ...

Similarly for download_vendor_file:

@op(required_resource_keys={'ftp'})
def download_vendor_file(context, filename: str):
    ...

So putting it all together:

The code would start with imports, then define the ftp_resource as a resource. Then each op that uses FTP (wait, download) requires the 'ftp' resource key.

But the original code example didn't include the required_resource_keys. That's a mistake. So correcting that:

@op(required_resource_keys={'ftp'}, retry_policy=RetryPolicy(max_retries=10, delay=30))
def wait_for_ftp_file(context):
    ftp = context.resources.ftp
    files = ftp.nlst()
    if 'vendor_inventory.csv' not in files:
        context.log.info('File not found, retrying...')
        raise Exception('File not found')
    return 'vendor_inventory.csv'

Similarly for download_vendor_file:

@op(required_resource_keys={'ftp'})
def download_vendor_file(context, filename: str):
    ftp = context.resources.ftp
    ...

Now, the job is defined with the resource_defs, and the ops have the required_resource_keys.

The RetryPolicy for wait_for_ftp_file is set to max_retries=10 and delay=30 seconds, which totals 10*30=300 seconds (5 minutes), matching the problem's timeout.

The other ops (cleanse and merge) don't require FTP, so they don't need the resource.

The main job's resource configuration is provided via the resource_defs, and the config for the ftp_resource can be specified when running the job. However, in the example, the user wants a minimal launch pattern. So in the __main__ block, when executing in process, the config needs to be provided.

So the job's config would include the resource configuration for 'ftp'. The example code can include a sample config in the job's execute_in_process call.

Putting it all together:

The complete code would look like:

from dagster import op, job, resource, RetryPolicy

@resource(config_schema={
    'host': str,
    'username': str,
    'password': str,
})
def ftp_resource(context):
    from ftplib import FTP
    ftp = FTP(context.resource_config['host'])
    ftp.login(context.resource_config['username'], context.resource_config['password'])
    return ftp

@op(required_resource_keys={'ftp'}, retry_policy=RetryPolicy(max_retries=10, delay=30))
def wait_for_ftp_file(context):
    ftp = context.resources.ftp
    files = ftp.nlst()
    if 'vendor_inventory.csv' not in files:
        raise Exception("File not found")
    return 'vendor_inventory.csv'

@op(required_resource_keys={'ftp'})
def download_vendor_file(context, filename: str):
    ftp = context.resources.ftp
    import tempfile
    local_path = tempfile.gettempdir() + '/' + filename
    with open(local_path, 'wb') as f:
        ftp.retrbinary(f'RETR {filename}', f.write)
    context.log.info(f"Downloaded {filename} to {local_path}")
    return local_path

@op
def cleanse_vendor_data(context, file_path: str):
    import pandas as pd
    df = pd.read_csv(file_path)
    cols = ['product_id', 'quantity', 'price']
    df_clean = df.dropna(subset=cols)
    clean_path = file_path.replace('.csv', '_clean.csv')
    df_clean.to_csv(clean_path, index=False)
    return clean_path

@op
def merge_with_internal_inventory(context, clean_path: str):
    import pandas as pd
    # Assuming internal inventory is in a CSV file
    df_vendor = pd.read_csv(clean_path)
    df_internal = pd.read_csv('internal_inventory.csv')  # Stub for internal data
    merged = pd.merge(df_internal, df_vendor, on='product_id', how='left')
    # Update logic (simplified)
    merged['stock'] = merged['quantity'].fillna(merged['stock'])
    merged['price'] = merged['price_y'].fillna(merged['price_x'])
    merged.to_csv('merged_inventory.csv', index=False)
    context.log.info("Merged inventory updated")

@job(resource_defs={'ftp': ftp_resource})
def vendor_inventory_pipeline():
    filename = wait_for_ftp_file()
    file_path = download_vendor_file(filename)
    clean_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(clean_path)

if __name__ == '__main__':
    # Example configuration with FTP credentials
    run_config = {
        'resources': {
            'ftp': {
                'config': {
                    'host': 'ftp.example.com',
                    'username': 'user',
                    'password': 'passwd',
                }
            }
        }
    }
    result = vendor_inventory_pipeline.execute_in_process(run_config=run_config)

This code defines the FTP resource, the ops with required resources, and sets up the job with dependencies. The wait_for_ftp_file op retries up to 10 times with 30-second delays. The main block includes an example configuration for the FTP resource.

Note: The FTP connection in the resource might not be persistent across retries. Each retry would re-initialize the FTP connection. Alternatively, the FTP connection should be established each time. The current code logs in each time the resource is created, which happens once per op execution. Since the wait_for_ftp_file op retries, each retry would re-initialize the resource, leading to a new FTP connection each time. This might not be efficient but works for the example.

Also, error handling for FTP connections (like network issues) is minimal here but meets the problem's requirements for a minimal example.

The merge_with_internal_inventory op assumes the existence of 'internal_inventory.csv', which is a stub. In a real scenario, this would connect to an internal system, perhaps via another resource.

The code is PEP 8 compliant, uses clear function names, and includes minimal docstrings where helpful. The launch pattern in the __main__ block allows execution with sample config.
</think>

from dagster import op, job, resource, RetryPolicy

@resource(config_schema={
    'host': str,
    'username': str,
    'password': str,
})
def ftp_resource(context):
    from ftplib import FTP
    ftp = FTP(context.resource_config['host'])
    ftp.login(context.resource_config['username'], context.resource_config['password'])
    return ftp

@op(required_resource_keys={'ftp'}, retry_policy=RetryPolicy(max_retries=10, delay=30))
def wait_for_ftp_file(context):
    ftp = context.resources.ftp
    if 'vendor_inventory.csv' not in ftp.nlst():
        context.log.info('File not found - retrying...')
        raise Exception("Required file not detected")
    return 'vendor_inventory.csv'

@op(required_resource_keys={'ftp'})
def download_vendor_file(context, filename: str):
    ftp = context.resources.ftp
    import tempfile
    local_path = tempfile.gettempdir() + '/' + filename
    with open(local_path, 'wb') as f:
        ftp.retrbinary(f'RETR {filename}', f.write)
    context.log.info(f"Downloaded file to {local_path}")
    return local_path

@op
def cleanse_vendor_data(context, file_path: str):
    import pandas as pd
    df = pd.read_csv(file_path)
    clean_df = df.dropna(subset=['product_id', 'quantity', 'price'])
    clean_path = file_path.replace('.csv', '_clean.csv')
    clean_df.to_csv(clean_path, index=False)
    return clean_path

@op
def merge_with_internal_inventory(context, clean_path: str):
    import pandas as pd
    vendor_df = pd.read_csv(clean_path)
    internal_df = pd.read_csv('internal_inventory.csv')  # Stub for actual integration
    merged = pd.merge(internal_df, vendor_df, on='product_id', how='outer')
    merged['stock'] = merged['quantity'].combine_first(merged['stock'])
    merged['price'] = merged['price_y'].combine_first(merged['price_x'])
    merged[['product_id', 'stock', 'price']].to_csv('updated_inventory.csv', index=False)
    context.log.info("Inventory merged and updated")

@job(resource_defs={'ftp': ftp_resource})
def vendor_inventory_pipeline():
    file_name = wait_for_ftp_file()
    local_file = download_vendor_file(file_name)
    cleaned_file = cleanse_vendor_data(local_file)
    merge_with_internal_inventory(cleaned_file)

if __name__ == '__main__':
    sample_config = {
        'resources': {
            'ftp': {
                'config': {
                    'host': 'ftp.vendor.com',
                    'username': 'data_user',
                    'password': 'secure123',
                }
            }
        }
    }
    result = vendor_inventory_pipeline.execute_in_process(run_config=sample_config)