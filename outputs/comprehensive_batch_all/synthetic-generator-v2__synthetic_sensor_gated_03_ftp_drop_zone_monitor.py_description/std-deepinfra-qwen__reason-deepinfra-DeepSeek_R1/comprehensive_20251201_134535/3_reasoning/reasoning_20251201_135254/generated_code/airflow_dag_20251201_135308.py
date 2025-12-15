<think>
Okay, let's tackle this problem. The user wants an Apache Airflow DAG that follows the given pipeline description. Let me break down the requirements step by step.

First, the pipeline starts with a custom FTPFileSensor. Since Airflow doesn't have a built-in FTP sensor, I'll need to create a custom one. Wait, the description mentions "custom FTPFileSensor task", so I should implement that. But how? Maybe by subclassing BaseSensorOperator. I'll need to write a CustomFTPFileSensor class that connects to the FTP server and checks for the file.

Next, the sensor needs to poll every 30 seconds (poke_interval=30) and timeout after 5 minutes. So the timeout is 5*60=300 seconds. The sensor's execution_timeout should be set to 300 seconds. Also, the DAG has retries with 2 retries and 5-minute delay between them. That goes into default_args: retries=2, retry_delay=timedelta(minutes=5).

The DAG is scheduled daily, so schedule_interval is '@daily' or '0 0 * * *'. Catchup should be set appropriately; the user didn't mention catchup, but best practice is to set catchup=False unless needed.

Now, the tasks are linear: sensor -> download -> cleanse -> merge. Each is a PythonOperator except the sensor. The functions download_vendor_file, cleanse_vendor_data, and merge_with_internal_inventory need to be defined. Since they're placeholders, I'll write dummy functions with print statements or pass, but make sure they're properly structured.

Wait, the user said to avoid placeholders. But since the actual implementation of these functions isn't provided, I'll have to include minimal safe defaults. So, each function will have a docstring and a print statement indicating what they do.

For the CustomFTPFileSensor, I need to override the poke method. The sensor should connect to FTP, check if the file exists, and return True if found. But since FTP details aren't provided, I'll use default connection IDs or placeholders. Maybe get the FTP connection from Airflow's connections, so the sensor uses an ftp_conn_id parameter.

Wait, the user might not have the FTP connection set up, but the code should still be executable. So, in the sensor, I'll handle cases where the connection might fail, but for the sake of the code, just attempt to connect. The actual implementation would require an FTP connection in Airflow, but the code should have the structure.

Putting it all together: Import necessary modules. Define the DAG with default_args including retries, retry_delay, and other parameters. Then define the CustomFTPFileSensor with the correct parameters. Then the PythonOperators for each step.

Dependencies are linear, so sensor >> download >> cleanse >> merge.

Also, the DAG should have a start_date. Since it's not specified, I'll set it to a date in the past, like days_ago(1), but that's deprecated. Alternatively, use datetime(2024, 1, 1) as a safe default.

Let me check the imports: from airflow import DAG, settings; from airflow.sensors.base import BaseSensorOperator; from airflow.operators.python import PythonOperator. Also, datetime and timedelta from datetime.

Wait, the user specified Airflow 2.x style imports. So, for the sensor, BaseSensorOperator is from airflow.sensors.base.

Putting it all into code:

- Define default_args with owner, retries, retry_delay, etc.
- Create the DAG with dag_id, schedule_interval, catchup=False, etc.
- Define the CustomFTPFileSensor with task_id, poke_interval=30, timeout=300, mode='poke', ftp_conn_id='ftp_default', file_name='vendor_inventory.csv'.
- Then the PythonOperators for each step.
- Set dependencies using >>.

Wait, the sensor's timeout is set via execution_timeout? Or is the timeout parameter in the sensor? Let me check. The BaseSensorOperator has a timeout parameter which is the total time before the task times out. So in the sensor, timeout=300.

Also, the functions for the PythonOperators need to accept **kwargs or context, but since they're not using it, just use dummy functions.

Testing for PEP8 compliance: variable names, proper spacing, etc.

So the final code should look something like this.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

class CustomFTPFileSensor(BaseSensorOperator):
    def __init__(self, ftp_conn_id='ftp_default', file_name='', **kwargs):
        super().__init__(**kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.file_name = file_name

    def poke(self, context):
        from airflow.providers.ftp.hooks.ftp import FTPHook
        hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        file_list = hook.list_directory()
        self.log.info(f"Files in directory: {file_list}")
        return self.file_name in file_list

def download_vendor_file():
    print("Simulating file download from FTP server")

def cleanse_vendor_data():
    print("Cleaning data: removing nulls from product_id, quantity, price")

def merge_with_internal_inventory():
    print("Merging vendor data with internal inventory system")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='vendor_inventory_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1)
) as dag:

    wait_for_file = CustomFTPFileSensor(
        task_id='wait_for_ftp_file',
        ftp_conn_id='ftp_vendor',
        file_name='vendor_inventory.csv',
        poke_interval=30,
        timeout=300,
        mode='poke'
    )

    download_file = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file
    )

    cleanse_data = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data
    )

    merge_inventory = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory
    )

    wait_for_file >> download_file >> cleanse_data >> merge_inventory