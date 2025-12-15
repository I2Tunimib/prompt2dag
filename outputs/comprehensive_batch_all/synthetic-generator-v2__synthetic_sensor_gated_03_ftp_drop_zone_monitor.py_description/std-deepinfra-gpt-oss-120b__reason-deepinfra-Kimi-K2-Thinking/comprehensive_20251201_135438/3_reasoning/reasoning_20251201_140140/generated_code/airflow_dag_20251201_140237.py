from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import ftplib
import os
import pandas as pd
import tempfile


class FTPFileSensor(BaseSensorOperator):
    """Sensor to check for file existence on FTP server."""

    def __init__(self, ftp_conn_id, filepath, **kwargs):
        super().__init__(**kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.filepath = filepath

    def poke(self, context):
        conn = BaseHook.get_connection(self.ftp_conn_id)
        try:
            ftp = ftplib.FTP(conn.host, conn.login, conn.password)
            ftp.cwd(os.path.dirname(self.filepath) or '/')
            filename = os.path.basename(self.filepath)
            files = ftp.nlst()
            ftp.quit()
            return filename in files
        except Exception as e:
            self.log.warning(f"FTP error: {e}")
            return False


def download_vendor_file(**context):
    """Download vendor_inventory.csv from FTP server to local temp directory."""
    conn = BaseHook.get_connection('ftp_default')
    ftp = ftplib.FTP(conn.host, conn.login, conn.password)

    remote_path = 'vendor_inventory.csv'
    local_path = os.path.join(tempfile.gettempdir(), 'vendor_inventory.csv')

    with open(local_path, 'wb') as f:
        ftp.retrbinary(f'RETR {remote_path}', f.write)

    ftp.quit()
    context['task_instance'].xcom_push(key='local_file_path', value=local_path)


def cleanse_vendor_data(**context):
    """Cleanse vendor data by removing null values from specific columns."""
    ti = context['task_instance']
    local_path = ti.xcom_pull(task_ids='download_vendor_file', key='local_file_path')

    df = pd.read_csv(local_path)

    # Remove rows with null values in specific columns
    columns_to_check = ['product_id', 'quantity', 'price']
    df_cleaned = df.dropna(subset=columns_to_check)

    # Save cleansed data
    cleansed_path = os.path.join(tempfile.gettempdir(), 'vendor_inventory_cleansed.csv')
    df_cleaned.to_csv(cleansed_path, index=False)

    ti.xcom_push(key='cleansed_file_path', value=cleansed_path)


def merge_with_internal_inventory(**context):
    """Merge cleansed vendor data with internal inventory system."""
    ti = context['task_instance']
    cleansed_path = ti.xcom_pull(task_ids='cleanse_vendor_data', key='cleansed_file_path')

    # Read cleansed vendor data
    vendor_df = pd.read_csv(cleansed_path)

    # Simulate internal inventory data (in real scenario, this would come from a database)
    internal_data = {
        'product_id': ['P001', 'P002', 'P003', 'P004'],
        'current_stock': [100, 150, 200, 120],
        'current_price': [10.99, 25.50, 15.75, 30.00]
    }
    internal_df = pd.DataFrame(internal_data)

    # Merge vendor data with internal inventory on product_id
    merged_df = internal_df.merge(
        vendor_df[['product_id', 'quantity', 'price']],
        on='product_id',
        how='left',
        suffixes=('_internal', '_vendor')
    )

    # Update stock levels and pricing from vendor data where available
    merged_df['current_stock'] = merged_df['quantity'].fillna(merged_df['current_stock'])
    merged_df['current_price'] = merged_df['price'].fillna(merged_df['current_price'])

    # Drop temporary columns
    merged_df = merged_df.drop(columns=['quantity', 'price'])

    # Save final merged data
    final_path = os.path.join(tempfile.gettempdir(), 'final_inventory.csv')
    merged_df.to_csv(final_path, index=False)

    # In a real scenario, this would update the internal inventory database
    print(f"Inventory updated. Final data saved to {final_path}")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ftp_vendor_inventory_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['ftp', 'inventory', 'vendor'],
) as dag:

    wait_for_ftp_file = FTPFileSensor(
        task_id='wait_for_ftp_file',
        ftp_conn_id='ftp_default',
        filepath='vendor_inventory.csv',
        poke_interval=30,
        timeout=300,
    )

    download_vendor_file_task = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    cleanse_vendor_data_task = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )

    merge_with_internal_inventory_task = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )

    wait_for_ftp_file >> download_vendor_file_task >> cleanse_vendor_data_task >> merge_with_internal_inventory_task