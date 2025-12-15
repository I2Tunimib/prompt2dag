from dagster import job, op, sensor, RunRequest, SkipReason, DefaultSensorStatus, resource, Field, StringSource, IntSource, RetryPolicy
import time
import os
import ftplib
import pandas as pd

# Resources
class FtpResource:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    def connect(self):
        ftp = ftplib.FTP(self.host)
        ftp.login(self.user, self.password)
        return ftp

@resource(config_schema={"host": StringSource, "user": StringSource, "password": StringSource})
def ftp_resource(context):
    return FtpResource(
        host=context.resource_config["host"],
        user=context.resource_config["user"],
        password=context.resource_config["password"]
    )

class InternalInventoryResource:
    def __init__(self, db_connection_string):
        self.db_connection_string = db_connection_string

    def load_inventory(self):
        # Simulate loading internal inventory from a database
        return pd.DataFrame({
            "product_id": [1, 2, 3],
            "quantity": [100, 200, 300],
            "price": [10.0, 20.0, 30.0]
        })

    def update_inventory(self, df):
        # Simulate updating the internal inventory
        print("Updating internal inventory with the following data:")
        print(df)

@resource(config_schema={"db_connection_string": StringSource})
def internal_inventory_resource(context):
    return InternalInventoryResource(
        db_connection_string=context.resource_config["db_connection_string"]
    )

# Ops
@op(required_resource_keys={"ftp"})
def wait_for_ftp_file(context):
    ftp = context.resources.ftp.connect()
    file_name = "vendor_inventory.csv"
    start_time = time.time()
    timeout = 300  # 5 minutes
    poke_interval = 30  # 30 seconds

    while (time.time() - start_time) < timeout:
        if file_name in ftp.nlst():
            context.log.info(f"File {file_name} found on FTP server.")
            return file_name
        time.sleep(poke_interval)

    raise Exception(f"File {file_name} not found within the timeout period.")

@op(required_resource_keys={"ftp"})
def download_vendor_file(context, file_name):
    ftp = context.resources.ftp.connect()
    local_file_path = f"/tmp/{file_name}"
    with open(local_file_path, "wb") as file:
        ftp.retrbinary(f"RETR {file_name}", file.write)
    context.log.info(f"Downloaded {file_name} to {local_file_path}")
    return local_file_path

@op
def cleanse_vendor_data(context, local_file_path):
    df = pd.read_csv(local_file_path)
    df.dropna(subset=["product_id", "quantity", "price"], inplace=True)
    context.log.info(f"Cleaned data: {df}")
    return df

@op(required_resource_keys={"internal_inventory"})
def merge_with_internal_inventory(context, cleansed_data):
    internal_inventory = context.resources.internal_inventory.load_inventory()
    merged_inventory = pd.merge(internal_inventory, cleansed_data, on="product_id", how="left")
    merged_inventory["quantity"] = merged_inventory["quantity_y"].fillna(merged_inventory["quantity_x"])
    merged_inventory["price"] = merged_inventory["price_y"].fillna(merged_inventory["price_x"])
    merged_inventory = merged_inventory[["product_id", "quantity", "price"]]
    context.resources.internal_inventory.update_inventory(merged_inventory)

# Job
@job(
    resource_defs={
        "ftp": ftp_resource,
        "internal_inventory": internal_inventory_resource
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def vendor_inventory_pipeline():
    file_name = wait_for_ftp_file()
    local_file_path = download_vendor_file(file_name)
    cleansed_data = cleanse_vendor_data(local_file_path)
    merge_with_internal_inventory(cleansed_data)

# Sensor
@sensor(job=vendor_inventory_pipeline, minimum_interval_seconds=86400, default_status=DefaultSensorStatus.RUNNING)
def vendor_inventory_sensor(context):
    # Simulate a daily check for the file
    if context.last_completion_time is None or (context.last_completion_time + 86400) < time.time():
        return RunRequest(run_key=None)
    else:
        return SkipReason("Not yet time to run the pipeline.")

if __name__ == '__main__':
    result = vendor_inventory_pipeline.execute_in_process()