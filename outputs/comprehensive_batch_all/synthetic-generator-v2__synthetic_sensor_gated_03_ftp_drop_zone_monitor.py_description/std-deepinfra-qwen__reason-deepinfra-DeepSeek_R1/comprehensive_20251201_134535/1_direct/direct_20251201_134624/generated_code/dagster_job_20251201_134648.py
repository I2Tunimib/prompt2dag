from dagster import job, op, sensor, RunRequest, RunStatusSensorContext, build_run_status_sensor, resource
import time
import os
import pandas as pd
from ftplib import FTP

# Resources
@resource
def ftp_resource():
    return FTP('ftp.example.com', 'user', 'password')

@resource
def internal_inventory_resource():
    return pd.read_csv('internal_inventory.csv')

# Ops
@op(required_resource_keys={"ftp"})
def wait_for_ftp_file(context):
    ftp = context.resources.ftp
    file_name = 'vendor_inventory.csv'
    start_time = time.time()
    timeout = 300  # 5 minutes
    poke_interval = 30  # 30 seconds

    while (time.time() - start_time) < timeout:
        if file_name in ftp.nlst():
            context.log.info(f"File {file_name} found on FTP server.")
            return file_name
        context.log.info(f"File {file_name} not found. Retrying in {poke_interval} seconds.")
        time.sleep(poke_interval)
    
    raise Exception(f"File {file_name} not found within the timeout period.")

@op
def download_vendor_file(context, file_name):
    local_path = f'/tmp/{file_name}'
    with open(local_path, 'wb') as file:
        ftp = context.resources.ftp
        ftp.retrbinary(f'RETR {file_name}', file.write)
    context.log.info(f"File {file_name} downloaded to {local_path}.")
    return local_path

@op
def cleanse_vendor_data(context, local_path):
    df = pd.read_csv(local_path)
    df.dropna(subset=['product_id', 'quantity', 'price'], inplace=True)
    df.to_csv(local_path, index=False)
    context.log.info(f"Vendor data cleansed and saved to {local_path}.")
    return local_path

@op(required_resource_keys={"internal_inventory"})
def merge_with_internal_inventory(context, local_path):
    vendor_df = pd.read_csv(local_path)
    internal_df = context.resources.internal_inventory
    merged_df = pd.merge(internal_df, vendor_df, on='product_id', how='left')
    merged_df.to_csv('merged_inventory.csv', index=False)
    context.log.info("Vendor data merged with internal inventory.")

# Job
@job(resource_defs={"ftp": ftp_resource, "internal_inventory": internal_inventory_resource})
def vendor_inventory_pipeline():
    file_name = wait_for_ftp_file()
    local_path = download_vendor_file(file_name)
    cleansed_path = cleanse_vendor_data(local_path)
    merge_with_internal_inventory(cleansed_path)

# Sensor
@build_run_status_sensor(name="ftp_file_sensor", monitored_jobs=[vendor_inventory_pipeline])
def ftp_file_sensor(context: RunStatusSensorContext):
    if context.dagster_event and context.dagster_event.is_step_success:
        if context.dagster_event.step_key == "wait_for_ftp_file":
            context.log.info("FTP file detected. Triggering pipeline run.")
            return RunRequest(run_key=None, run_config={})

# Launch pattern
if __name__ == '__main__':
    result = vendor_inventory_pipeline.execute_in_process()