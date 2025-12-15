<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the pipeline has a linear sequence of tasks starting with a custom FTPFileSensor. The sensor checks for vendor_inventory.csv every 30 seconds with a 5-minute timeout. Then, there are three PythonOperator tasks for downloading, cleansing, and merging the data.

The DAG should be scheduled daily, with retries (2 retries, 5-minute delay). Default args need to include these retries, along with other standard parameters like owner, start_date, etc. Since catchup isn't mentioned, but the problem says to define it, maybe set catchup=False.

Wait, the sensor is a custom one. But in Airflow, there's no built-in FTPFileSensor. Oh, the user mentioned "custom FTPFileSensor task". Hmm, but how to handle that in code? Since I can't create a custom sensor here, maybe I should use a placeholder or assume that it's available. Alternatively, perhaps use a dummy BaseSensorOperator for the sake of the example. But the user expects executable code. Wait, maybe the user expects me to use the correct sensor. Wait, Airflow has an FTP sensor? Let me check. Oh, right, there's an FtpSensor in airflow.providers.ftp.sensors.ftp. So maybe I should import that.

Wait, the problem says "Use Airflow 2.x style imports". So for FTPFileSensor, perhaps it's from airflow.providers.ftp.sensors.ftp import FTPSensor. Wait, checking the provider's documentation: The FTPSensor is part of the FTP provider package. So the import would be correct.

So, the first task is Wait For Ftp File using FTPSensor. The parameters for the sensor: path should be the filename, ftp_conn_id (needs a connection), poke_interval=30 seconds, timeout=5*60=300 seconds.

Then, the download task uses PythonOperator with a function download_vendor_file. Similarly for cleanse and merge tasks.

Default_args should include retries=2, retry_delay=5 minutes (timedelta(minutes=5)), and other standard parameters.

The DAG is scheduled daily, schedule_interval='@daily', catchup=False.

Now, putting it all together.

First, imports: DAG, PythonOperator, FTPSensor (from providers.ftp), and others like datetime, timedelta.

Default_args: owner, start_date (maybe a safe default like datetime(2024,1,1)), retries, retry_delay, etc.

Then, define the DAG with dag_id, default_args, schedule_interval, catchup.

Tasks:

1. wait_for_ftp_file = FTPSensor(...)
   Parameters: task_id, path='vendor_inventory.csv', ftp_conn_id='ftp_default', poke_interval=30, timeout=300.

2. download_vendor_file = PythonOperator(...)
   python_callable=download_vendor_file, which is a function I need to define. But since the user wants executable code, I need to provide dummy functions with pass or minimal code.

Similarly for cleanse_vendor_data and merge_with_internal_inventory functions.

So, the code will have placeholder functions with docstrings explaining their purpose.

Dependencies are linear: sensor >> download >> cleanse >> merge.

Now, code structure:

Imports:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.sensors.ftp import FTPSensor
from datetime import datetime, timedelta

Functions:

def download_vendor_file():
    pass

def cleanse_vendor_data():
    pass

def merge_with_internal_inventory():
    pass

Default args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

DAG definition:

with DAG(...) as dag:

    wait_for_ftp_file = FTPSensor(
        task_id='wait_for_ftp_file',
        path='vendor_inventory.csv',
        ftp_conn_id='ftp_default',
        poke_interval=30,
        timeout=300,
    )

    download_vendor_file = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    # Similarly for other tasks.

Then dependencies:

wait_for_ftp_file >> download_vendor_file >> cleanse_vendor_data >> merge_with_internal_inventory

Wait, the functions need to be defined. Even if they are placeholders, they should exist. So in the code, include those functions with docstrings.

Check PEP8 compliance: proper indentation, line lengths, etc.

Also, the FTP sensor's timeout is 5 minutes (300 seconds), which matches the description.

The schedule_interval is '@daily', and catchup=False.

Putting all together, the code should be executable, assuming the FTP connection is set up (ftp_default). The user might need to adjust the start_date, but the example uses a fixed one.

So the final code should look like this.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.sensors.ftp import FTPSensor
from datetime import datetime, timedelta

def download_vendor_file():
    """Download vendor_inventory.csv from FTP server to local storage"""
    pass

def cleanse_vendor_data():
    """Clean null values from product_id, quantity, and price columns"""
    pass

def merge_with_internal_inventory():
    """Merge vendor data with internal inventory using product_id join"""
    pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='vendor_inventory_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['inventory', 'vendor'],
) as dag:

    wait_for_ftp_file = FTPSensor(
        task_id='wait_for_ftp_file',
        path='vendor_inventory.csv',
        ftp_conn_id='ftp_default',
        poke_interval=30,
        timeout=300,
    )

    download_vendor_file = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    cleanse_vendor_data = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )

    merge_with_internal_inventory = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )

    wait_for_ftp_file >> download_vendor_file >> cleanse_vendor_data >> merge_with_internal_inventory