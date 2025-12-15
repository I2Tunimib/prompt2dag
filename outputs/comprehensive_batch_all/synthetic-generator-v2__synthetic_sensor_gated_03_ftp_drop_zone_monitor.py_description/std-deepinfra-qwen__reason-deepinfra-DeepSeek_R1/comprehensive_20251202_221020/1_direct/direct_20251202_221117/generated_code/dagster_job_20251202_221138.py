from dagster import job, op, resource, RetryPolicy, Failure, success_hook, failure_hook
import time
import ftplib
import pandas as pd
import os

# Resources
@resource
def ftp_resource():
    return ftplib.FTP('ftp.example.com', 'user', 'password')

@resource
def internal_inventory_resource():
    return pd.read_csv('internal_inventory.csv')

# Hooks
@success_hook
def notify_success(context):
    context.log.info("Pipeline executed successfully.")

@failure_hook
def notify_failure(context):
    context.log.error("Pipeline failed.")

# Ops
@op(required_resource_keys={"ftp"})
def wait_for_ftp_file(context):
    file_name = 'vendor_inventory.csv'
    start_time = time.time()
    timeout = 300  # 5 minutes
    poke_interval = 30  # 30 seconds

    while (time.time() - start_time) < timeout:
        try:
            context.resources.ftp.cwd('/')
            if file_name in context.resources.ftp.nlst():
                context.log.info(f"File {file_name} found.")
                return file_name
        except ftplib.error_perm:
            context.log.error("FTP error occurred.")
        time.sleep(poke_interval)

    raise Failure(f"File {file_name} not found within the timeout period.")

@op
def download_vendor_file(context, file_name):
    local_path = '/tmp/vendor_inventory.csv'
    with open(local_path, 'wb') as file:
        context.resources.ftp.retrbinary(f'RETR {file_name}', file.write)
    context.log.info(f"File {file_name} downloaded to {local_path}.")
    return local_path

@op
def cleanse_vendor_data(context, file_path):
    df = pd.read_csv(file_path)
    df.dropna(subset=['product_id', 'quantity', 'price'], inplace=True)
    df.to_csv(file_path, index=False)
    context.log.info("Vendor data cleansed.")
    return file_path

@op(required_resource_keys={"internal_inventory"})
def merge_with_internal_inventory(context, file_path):
    vendor_df = pd.read_csv(file_path)
    internal_df = context.resources.internal_inventory
    merged_df = pd.merge(internal_df, vendor_df, on='product_id', how='left')
    merged_df.to_csv('merged_inventory.csv', index=False)
    context.log.info("Vendor data merged with internal inventory.")

# Job
@job(
    resource_defs={
        "ftp": ftp_resource,
        "internal_inventory": internal_inventory_resource
    },
    hooks=[notify_success, notify_failure],
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def vendor_inventory_pipeline():
    file_name = wait_for_ftp_file()
    file_path = download_vendor_file(file_name)
    cleansed_file_path = cleanse_vendor_data(file_path)
    merge_with_internal_inventory(cleansed_file_path)

if __name__ == '__main__':
    result = vendor_inventory_pipeline.execute_in_process()