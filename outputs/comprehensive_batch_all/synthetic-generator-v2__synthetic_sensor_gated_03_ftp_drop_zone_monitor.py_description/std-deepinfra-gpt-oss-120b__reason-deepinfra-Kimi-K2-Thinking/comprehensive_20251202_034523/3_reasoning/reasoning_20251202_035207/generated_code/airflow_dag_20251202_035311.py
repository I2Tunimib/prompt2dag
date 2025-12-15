from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
import ftplib
import pandas as pd
import tempfile
import os


class FTPFileSensor(BaseSensorOperator):
    """Custom sensor to check for file existence on FTP server."""
    
    def __init__(self, ftp_conn_id, filepath, **kwargs):
        super().__init__(**kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.filepath = filepath

    def poke(self, context):
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(self.ftp_conn_id)
        try:
            with ftplib.FTP(conn.host, conn.login, conn.password) as ftp:
                ftp.cwd('/')
                if self.filepath in ftp.nlst():
                    return True
        except Exception as e:
            self.log.info(f"FTP check failed: {e}")
        return False


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='vendor_inventory_pipeline',
    default_args=default_args,
    description='Sensor-gated pipeline for vendor inventory processing',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['vendor', 'inventory', 'ftp'],
) as dag:

    wait_for_ftp_file = FTPFileSensor(
        task_id='wait_for_ftp_file',
        ftp_conn_id='ftp_default',
        filepath='vendor_inventory.csv',
        poke_interval=30,
        timeout=300,
    )

    def download_vendor_file(**context):
        """Download vendor inventory file from FTP server."""
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('ftp_default')
        local_path = os.path.join(tempfile.gettempdir(), 'vendor_inventory.csv')
        
        with ftplib.FTP(conn.host, conn.login, conn.password) as ftp:
            with open(local_path, 'wb') as f:
                ftp.retrbinary('RETR vendor_inventory.csv', f.write)
        
        context['ti'].xcom_push(key='local_file_path', value=local_path)
        return local_path

    download_vendor_file_task = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    def cleanse_vendor_data(**context):
        """Cleanse vendor data by removing null values from key columns."""
        ti = context['ti']
        local_path = ti.xcom_pull(task_ids='download_vendor_file', key='local_file_path')
        
        df = pd.read_csv(local_path)
        initial_count = len(df)
        
        df_cleaned = df.dropna(subset=['product_id', 'quantity', 'price'])
        cleaned_count = len(df_cleaned)
        
        cleansed_path = os.path.join(tempfile.gettempdir(), 'vendor_inventory_cleansed.csv')
        df_cleaned.to_csv(cleansed_path, index=False)
        
        ti.xcom_push(key='cleansed_file_path', value=cleansed_path)
        ti.xcom_push(key='records_dropped', value=initial_count - cleaned_count)
        
        return cleansed_path

    cleanse_vendor_data_task = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )

    def merge_with_internal_inventory(**context):
        """Merge cleansed vendor data with internal inventory system."""
        ti = context['ti']
        cleansed_path = ti.xcom_pull(task_ids='cleanse_vendor_data', key='cleansed_file_path')
        
        vendor_df = pd.read_csv(cleansed_path)
        
        # Simulate internal inventory data - replace with actual DB query in production
        internal_data = {
            'product_id': ['P001', 'P002', 'P003'],
            'internal_stock': [100, 150, 200],
            'internal_price': [10.0, 20.0, 30.0]
        }
        internal_df = pd.DataFrame(internal_data)
        
        merged_df = internal_df.merge(
            vendor_df[['product_id', 'quantity', 'price']],
            on='product_id',
            how='left',
            suffixes=('_internal', '_vendor')
        )
        
        merged_df['internal_stock'] = merged_df['quantity'].fillna(merged_df['internal_stock'])
        merged_df['internal_price'] = merged_df['price'].fillna(merged_df['internal_price'])
        
        final_df = merged_df.drop(columns=['quantity', 'price'])
        
        final_path = os.path.join(tempfile.gettempdir(), 'merged_inventory.csv')
        final_df.to_csv(final_path, index=False)
        
        ti.xcom_push(key='merged_file_path', value=final_path)
        ti.xcom_push(key='records_merged', value=len(final_df))
        
        return final_path

    merge_with_internal_inventory_task = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )

    wait_for_ftp_file >> download_vendor_file_task >> cleanse_vendor_data_task >> merge_with_internal_inventory_task