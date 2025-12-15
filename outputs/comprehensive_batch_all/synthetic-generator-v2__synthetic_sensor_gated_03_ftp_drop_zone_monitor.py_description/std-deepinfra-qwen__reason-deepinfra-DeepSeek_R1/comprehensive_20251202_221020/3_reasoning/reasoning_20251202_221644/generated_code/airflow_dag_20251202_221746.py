<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the pipeline has a linear sequence of tasks starting with a custom FTPFileSensor. The sensor checks for vendor_inventory.csv every 30 seconds with a 5-minute timeout. Then, there are three PythonOperator tasks for downloading, cleansing, and merging the data.

The DAG should be scheduled daily, with retries (2 retries, 5-minute delay). Default args need to include these retries, along with other standard parameters like start_date and email settings if any. But the user didn't mention emails, so maybe just the retries, retry_delay, and maybe others like 'owner'.

Wait, the user specified Airflow 2.x style imports. So I should import DAG from airflow, and operators from their respective modules. The FTPFileSensor is a custom sensor, so I'll need to create a dummy class for it since the actual implementation isn't provided. But the code should still be executable, so I'll define a basic sensor that inherits from BaseSensorOperator.

Let me outline the steps:

1. Import necessary modules: DAG, PythonOperator, BaseSensorOperator, etc.
2. Define default_args with retries, retry_delay, and other parameters.
3. Create the DAG with schedule_interval='@daily', catchup=False, and default_args.
4. Define the custom FTPFileSensor. Since it's custom, I'll create a class that overrides poke() or something. But to make it executable, maybe just a placeholder that returns True after some condition. Wait, but sensors in Airflow typically use a poke method where they check a condition. However, for the sake of the code being executable, maybe simulate the sensor succeeding immediately. But the user wants the code to be executable, so perhaps the sensor is a dummy that just waits. Alternatively, maybe use the built-in FileSensor, but the problem states it's an FTP server, so a custom sensor is needed. Since we can't implement the actual FTP logic here, I'll create a minimal custom sensor that always returns True for the sake of example. But in a real scenario, this would have the actual FTP checking code.

Wait, the user said to return executable code. So the FTPFileSensor needs to be a valid sensor. Since we can't have actual FTP code, perhaps use a dummy sensor that just pokes successfully after a certain time. Alternatively, maybe use a BaseSensorOperator with a poke method that returns True. But how to make it wait until the file is present? Maybe for the sake of the example, the sensor will just pass after the first poke. But the user's description says it times out after 5 minutes. So the timeout is 5 minutes (300 seconds), and poke_interval is 30 seconds. So the sensor's execution_timeout is 5 minutes, and the poke_interval is 30. So in the sensor parameters, we set timeout=300, poke_interval=30. Wait, in Airflow sensors, the 'timeout' is the total time before the sensor fails, and 'poke_interval' is the time between checks. So the sensor will check every 30 seconds, up to 5 minutes. If the file isn't found by then, it fails.

But how to implement a custom sensor. Let's create a class FTPFileSensor that inherits from BaseSensorOperator. The poke method would check the FTP server. But since we can't implement that, perhaps the poke method just returns True immediately for the example, but in reality, it would have the actual check. So the code will have a skeleton for the sensor.

Next, the Python functions for the tasks: download_vendor_file, cleanse_vendor_data, merge_with_internal_inventory. These need to be defined as callables. Since they are placeholders, their implementations can be simple print statements or dummy operations.

Now, putting it all together:

- Imports: from airflow import DAG; from airflow.operators.python import PythonOperator; from airflow.sensors.base import BaseSensorOperator; from datetime import datetime, timedelta.

- Default args: start_date is required. Let's set it to days_ago(1) for example. But the user might not specify, so set a reasonable default.

- The DAG definition: with DAG(...) as dag.

- Define the custom sensor class FTPFileSensor inside the module.

- Then, instantiate the tasks in order: wait_for_ftp_file (sensor), download_task, cleanse_task, merge_task.

- Set dependencies using >> operator in order.

Now, code structure:

Import statements.

Define the FTPFileSensor class.

Define the Python functions.

Default_args setup.

Instantiate DAG.

Define tasks within the DAG context.

Set dependencies.

Potential issues: The custom sensor's module might need to be imported, but since it's part of the same code, defining it inline should work. Also, the sensor's parameters: mode='poke', timeout=300, poke_interval=30.

Wait, in Airflow, the timeout for a sensor is controlled by execution_timeout parameter. So the sensor should have execution_timeout=timedelta(minutes=5), and poke_interval=30.

So the sensor task would be:

wait_for_ftp_file = FTPFileSensor(
    task_id='wait_for_ftp_file',
    poke_interval=30,
    execution_timeout=timedelta(minutes=5),
    ...
)

But the user's description says the sensor times out after 5 minutes. So execution_timeout is 5 minutes.

Putting it all together.

Testing if the code is executable: The custom sensor must not have syntax errors. The functions for the PythonOperators must be defined.

So, the code would look like:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
from typing import Any

class FTPFileSensor(BaseSensorOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.poke_interval = 30  # Override if passed via parameters
        self.timeout = 300  # 5 minutes in seconds

    def poke(self, context: Any) -> bool:
        # Implement actual FTP check here
        # For example purposes, assume file is found immediately
        return True

def download_vendor_file():
    print("Downloading vendor_inventory.csv")

def cleanse_vendor_data():
    print("Cleansing vendor data")

def merge_with_internal_inventory():
    print("Merging with internal inventory")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'vendor_inventory_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    wait_for_ftp_file = FTPFileSensor(
        task_id='wait_for_ftp_file',
        poke_interval=30,
        execution_timeout=timedelta(minutes=5),
    )

    download_task = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    cleanse_task = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )

    merge_task = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )

    wait_for_ftp_file >> download_task >> cleanse_task >> merge_task

Wait, but the FTPFileSensor's __init__ method may need to accept parameters like poke_interval and execution_timeout. However, in Airflow, the BaseSensorOperator already has parameters for poke_interval and timeout. Wait, no: the BaseSensorOperator has a 'poke_interval' parameter, and the 'timeout' is inherited from BaseOperator (execution_timeout). So perhaps the correct way is to pass poke_interval and execution_timeout when instantiating the sensor.

Wait, the BaseSensorOperator's parameters include poke_interval, which is the time between pokes. The execution_timeout is a parameter of BaseOperator, which sets the maximum time allowed for the task to execute. So in the sensor task, we set execution_timeout to 5 minutes (timedelta(minutes=5)), and poke_interval=30.

So the FTPFileSensor can inherit from BaseSensorOperator and use the existing parameters. Therefore, the custom sensor may not need to override __init__, unless there's specific setup needed. But for the example, perhaps the custom sensor doesn't need any additional code beyond the poke method.

Wait, the problem states that the sensor is a custom FTPFileSensor. So the code must include that. But since we can't implement actual FTP logic, the poke method returns True immediately, simulating that the file is found. In reality, this method would check the FTP server.

So the FTPFileSensor class would be:

class FTPFileSensor(BaseSensorOperator):
    def poke(self, context):
        # Actual implementation would check FTP for vendor_inventory.csv
        # For this example, return True to simulate file found
        return True

Then, when creating the task:

wait_for_ftp_file = FTPFileSensor(
    task_id='wait_for_ftp_file',
    poke_interval=30,
    timeout=300,  # Total time before failing (5 minutes)
    mode='poke',
)

Wait, but in Airflow, the 'timeout' parameter of BaseOperator is execution_timeout. Wait, no: execution_timeout is a separate parameter. Let me check: the BaseOperator has an 'execution_timeout' parameter which is a timedelta. The 'timeout' is a deprecated parameter. So to set the maximum time the task can run, we use execution_timeout=timedelta(minutes=5).

So the sensor task should be:

wait_for_ftp_file = FTPFileSensor(
    task_id='wait_for_ftp_file',
    poke_interval=30,
    execution_timeout=timedelta(minutes=5),
    mode='poke',
)

But the mode is 'poke' by default for sensors. So maybe not needed.

Putting it all together, the code would be:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
from typing import Any

class FTPFileSensor(BaseSensorOperator):
    def poke(self, context: Any) -> bool:
        # Implement actual FTP check for vendor_inventory.csv here
        # For demonstration, return True immediately
        return True

def download_vendor_file():
    print("Downloading vendor_inventory.csv from FTP server")

def cleanse_vendor_data():
    print("Cleansing data: removing nulls in product_id, quantity, price")

def merge_with_internal_inventory():
    print("Merging with internal inventory on product_id")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'vendor_inventory_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['vendor'],
) as dag:

    wait_for_ftp_file = FTPFileSensor(
        task_id='wait_for_ftp_file',
        poke_interval=30,
        execution_timeout=timedelta(minutes=5),
    )

    download_task = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file,
    )

    cleanse_task = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data,
    )

    merge_task = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory,
    )

    wait_for_ftp_file >> download_task >> cleanse_task >> merge_task

This code should be executable, with the custom sensor, proper imports, and linear dependencies. The functions are placeholders but valid. The sensor uses the correct parameters for poke_interval and execution_timeout. The default_args include retries and retry_delay as specified. The DAG is scheduled daily with catchup disabled.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
from typing import Any

class FTPFileSensor(BaseSensorOperator):
    def poke(self, context: Any) -> bool:
        # Implement actual FTP file check here
        return True  # Simplified for example

def download_vendor_file():
    print("Downloading vendor_inventory.csv from FTP server")

def cleanse_vendor_data():
    print("Cleansing data: removing null values")

def merge_with_internal_inventory():
    print("Merging vendor data with internal inventory")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'vendor_inventory_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['inventory'],
) as dag:

    wait_for_file = FTPFileSensor(
        task_id='wait_for_ftp_file',
        poke_interval=30,
        execution_timeout=timedelta(minutes=5),
        mode='poke'
    )

    download_task = PythonOperator(
        task_id='download_vendor_file',
        python_callable=download_vendor_file
    )

    cleanse_task = PythonOperator(
        task_id='cleanse_vendor_data',
        python_callable=cleanse_vendor_data
    )

    merge_task = PythonOperator(
        task_id='merge_with_internal_inventory',
        python_callable=merge_with_internal_inventory
    )

    wait_for_file >> download_task >> cleanse_task >> merge_task