from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import ftplib
import os
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='vendor_inventory_pipeline',
    default_args=default_args,
    description='A pipeline to process vendor inventory data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Sensor to wait for the FTP file
    def wait_for_ftp_file():
        ftp = ftplib.FTP('ftp.example.com')
        ftp.login('user', 'password')
        file_exists = 'vendor_inventory.csv' in ftp.nlst()
        ftp.quit()
        return file_exists

    wait_for_ftp_file_sensor = PythonSensor(
        task_id='wait_for_ftp_file',
        python_callable=wait_for_ftp_file,
        poke_interval=30,
        timeout=300,
        mode='poke',
    )

    # Task to download the vendor file
    def download_vendor_file():
        ftp = ftplib.FTP('ftp.example.com')
        ftp.login('user', 'password')
        with open('/tmp/vendor_inventory.csv', 'wb') as file:
            ftp.retrbinary('RETR vendor_inventory.csv', file.write)
        ftp.quit()

    download_vendor_file_task = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    # Task to cleanse the vendor data
    def cleanse_vendor_data():
        df = pd.read_csv('/tmp/vendor_inventory.csv')
        df.dropna(subset=['product_id', 'quantity', 'price'], inplace=True)
        df.to_csv('/tmp/cleansed_vendor_inventory.csv', index=False)

    cleanse_vendor_data_task = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )

    # Task to merge with internal inventory
    def merge_with_internal_inventory():
        vendor_df = pd.read_csv('/tmp/cleansed_vendor_inventory.csv')
        internal_df = pd.read_csv('/path/to/internal_inventory.csv')
        merged_df = pd.merge(internal_df, vendor_df, on='product_id', how='left')
        merged_df.to_csv('/path/to/updated_inventory.csv', index=False)

    merge_with_internal_inventory_task = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )

    # Define the task dependencies
    wait_for_ftp_file_sensor >> download_vendor_file_task >> cleanse_vendor_data_task >> merge_with_internal_inventory_task