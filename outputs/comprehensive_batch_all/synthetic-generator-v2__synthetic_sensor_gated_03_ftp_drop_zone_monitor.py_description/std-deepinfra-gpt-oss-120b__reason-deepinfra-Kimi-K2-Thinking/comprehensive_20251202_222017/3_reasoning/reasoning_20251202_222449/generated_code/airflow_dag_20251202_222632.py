from datetime import datetime, timedelta
import logging
import os
import tempfile
from typing import Optional

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.exceptions import AirflowException


class FTPFileSensor(BaseSensorOperator):
    """
    Custom sensor to monitor for file existence on an FTP server.
    
    :param filepath: Path to the file on FTP server to monitor
    :param ftp_conn_id: Airflow connection ID for FTP server
    """
    
    def __init__(
        self,
        filepath: str,
        ftp_conn_id: str = 'ftp_default',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.filepath = filepath
        self.ftp_conn_id = ftp_conn_id

    def poke(self, context: dict) -> bool:
        """Check for file existence on FTP server."""
        logger = logging.getLogger(__name__)
        hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        
        try:
            conn = hook.get_conn()
            # List files in current directory and check for target file
            files = conn.nlst()
            
            if self.filepath in files:
                logger.info(f"File '{self.filepath}' found on FTP server")
                return True
            else:
                logger.info(f"File '{self.filepath}' not yet available on FTP server")
                return False
                
        except Exception as e:
            logger.error(f"Error checking FTP server: {e}")
            raise AirflowException(f"FTP connection error: {e}")
        finally:
            hook.close_conn()


def download_vendor_file(**context) -> str:
    """
    Downloads vendor_inventory.csv from FTP server to a local temporary file.
    
    Pushes the local file path to XCom for downstream tasks.
    """
    logger = logging.getLogger(__name__)
    hook = FTPHook(ftp_conn_id='ftp_default')
    
    # Create temporary file for download
    temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
    temp_file.close()
    local_path = temp_file.name
    
    try:
        logger.info(f"Downloading vendor_inventory.csv to {local_path}")
        hook.retrieve_file('vendor_inventory.csv', local_path)
        
        # Verify file was downloaded
        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            raise AirflowException("Downloaded file is empty or missing")
            
        # Push file path to XCom
        context['task_instance'].xcom_push(key='vendor_file_path', value=local_path)
        logger.info(f"Successfully downloaded file to {local_path}")
        
        return local_path
        
    except Exception as e:
        # Clean up on failure
        if os.path.exists(local_path):
            os.unlink(local_path)
        raise AirflowException(f"Failed to download vendor file: {e}")
    finally:
        hook.close_conn()


def cleanse_vendor_data(**context) -> str:
    """
    Cleanses vendor data by removing rows with null values in key columns.
    
    Processes product_id, quantity, and price columns. Pushes cleansed file path to XCom.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    # Pull file path from upstream task
    vendor_file_path = ti.xcom_pull(
        task_ids='download_vendor_file',
        key='vendor_file_path'
    )
    
    if not vendor_file_path or not os.path.exists(vendor_file_path):
        raise AirflowException("Vendor file path not found or file doesn't exist")
    
    try:
        # Read vendor data
        df = pd.read_csv(vendor_file_path)
        logger.info(f"Loaded {len(df)} rows from vendor file")
        
        # Define required columns for cleansing
        required_columns = ['product_id', 'quantity', 'price']
        
        # Validate columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise AirflowException(f"Required columns missing: {missing_columns}")
        
        # Drop rows with null values in required columns
        cleansed_df = df.dropna(subset=required_columns)
        logger.info(f"Cleaned data: {len(df)} -> {len(cleansed_df)} rows")
        
        # Save cleansed data to new temporary file
        cleansed_file = tempfile.NamedTemporaryFile(
            mode='w',
            delete=False,
            suffix='_cleansed.csv'
        )
        cleansed_file.close()
        cleansed_df.to_csv(cleansed_file.name, index=False)
        
        # Push cleansed file path to XCom
        ti.xcom_push(key='cleansed_file_path', value=cleansed_file.name)
        
        # Clean up original file
        os.unlink(vendor_file_path)
        
        logger.info(f"Saved cleansed data to {cleansed_file.name}")
        return cleansed_file.name
        
    except Exception as e:
        raise AirflowException(f"Data cleansing failed: {e}")


def merge_with_internal_inventory(**context) -> str:
    """
    Merges cleansed vendor data with internal inventory system.
    
    Performs join on product_id and updates stock levels and pricing.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    # Pull cleansed file path from upstream task
    cleansed_file_path = ti.xcom_pull(
        task_ids='cleanse_vendor_data',
        key='cleansed_file_path'
    )
    
    if not cleansed_file_path or not os.path.exists(cleansed_file_path):
        raise AirflowException("Cleansed file path not found or file doesn't exist")
    
    # Internal inventory file path (configurable via environment variable)
    internal_inventory_path = os.getenv(
        'INTERNAL_INVENTORY_PATH',
        '/opt/airflow/data/internal_inventory.csv'
    )
    
    try:
        # Read cleansed vendor data
        vendor_df = pd.read_csv(cleansed_file_path)
        logger.info(f"Loaded {len(vendor_df)} rows from cleansed vendor data")
        
        # Read internal inventory
        if os.path.exists(internal_inventory_path):
            internal_df = pd.read_csv(internal_inventory_path)
            logger.info(f"Loaded {len(internal_df)} rows from internal inventory")
        else:
            logger.warning(
                f"Internal inventory not found at {internal_inventory_path}, "
                "creating new file"
            )
            internal_df = pd.DataFrame(columns=['product_id', 'stock_level', 'price'])
        
        # Merge data on product_id
        # Update stock_level and price from vendor data
        vendor_df = vendor_df.rename(columns={'quantity': 'stock_level'})
        
        # Perform merge
        merged_df = internal_df.set_index('product_id').combine_first(
            vendor_df.set_index('product_id')
        ).reset_index()
        
        # Update with latest vendor data where available
        vendor_set = vendor_df.set_index('product_id')
        for idx, row in merged_df.iterrows():
            if row['product_id'] in vendor_set.index:
                merged_df.at[idx, 'stock_level'] = vendor_set.at[row['product_id'], 'stock_level']
                merged_df.at[idx, 'price'] = vendor_set.at[row['product_id'], 'price']
        
        # Save updated inventory
        os.makedirs(os.path.dirname(internal_inventory_path), exist_ok=True)
        merged_df.to_csv(internal_inventory_path, index=False)
        
        logger.info(f"Merged inventory saved to {internal_inventory_path}")
        
        # Clean up cleansed file
        os.unlink(cleansed_file_path)
        
        return internal_inventory_path
        
    except Exception as e:
        raise AirflowException(f"Inventory merge failed: {e}")


# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG definition
with DAG(
    'vendor_inventory_pipeline',
    default_args=default_args,
    description='Sensor-gated pipeline for vendor inventory processing via FTP',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['vendor', 'inventory', 'ftp', 'sensor-gated'],
) as dag:
    
    # Sensor task: Wait for FTP file with 30s poke interval and 5min timeout
    wait_for_ftp_file = FTPFileSensor(
        task_id='wait_for_ftp_file',
        filepath='vendor_inventory.csv',
        ftp_conn_id='ftp_default',
        poke_interval=30,
        timeout=300,
        mode='poke',
    )
    
    # Download task: Retrieve file from FTP
    download_vendor_file = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )
    
    # Cleansing task: Remove null values from key columns
    cleanse_vendor_data = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )
    
    # Merge task: Update internal inventory with vendor data
    merge_with_internal_inventory = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )
    
    # Define linear task dependencies
    wait_for_ftp_file >> download_vendor_file >> cleanse_vendor_data >> merge_with_internal_inventory