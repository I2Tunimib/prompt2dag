<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has four main steps: fetching CSV files from four warehouses in parallel, normalizing SKU formats concurrently for each, reconciling discrepancies, and generating a final report. The DAG should reflect this structure with proper task dependencies.

The user specified using Airflow 2.x style imports, so I'll make sure to import DAG and PythonOperator correctly. Since there are no sensors or branching mentioned, I don't need to include those operators. All tasks will use PythonOperator.

Default_args need to be set with the start date, retries, retry delay, and other parameters. The schedule_interval is daily, so I'll set it to '@daily'. Catchup should be False as per the constraints.

Next, I'll define the DAG with the given start date and other parameters. The tasks will be structured as follows:

1. Four fetch tasks (north, south, east, west) running in parallel.
2. Four normalization tasks, each dependent on their respective fetch task, also running in parallel.
3. The reconcile task depends on all normalization tasks completing.
4. The generate_report task depends on the reconcile task.

Each of these tasks will use PythonOperator with mock functions since the actual data processing logic isn't provided. I'll need to create placeholder functions for fetching, normalizing, reconciling, and generating the report. These functions should accept **kwargs to handle Airflow context if needed, even if they don't use it.

For the fetch tasks, each will take a warehouse name as an argument. I can use the op_args parameter in PythonOperator to pass the warehouse name to the function. Similarly, the normalization tasks will take the warehouse name and the task instance to pull the XCom value from the corresponding fetch task.

Wait, the description mentions using XCom for passing normalized file references. So each fetch task pushes the file path via XCom, and the normalization task pulls it. Then, the reconcile task needs all normalized data, so each normalization task pushes its result, and reconcile pulls all of them.

In the normalize_sku function, after processing, we'll push the normalized file path to XCom. The reconcile task will collect all four normalized files from XCom. To do this, the reconcile function can use the task instance to pull XCom values from the normalization tasks.

But how to get the XCom from the specific normalization tasks? Since each normalization task is for a warehouse, maybe we can use the task_ids of those tasks. For example, 'normalize_north', 'normalize_south', etc. So in the reconcile function, we can pull XCom values from each of these tasks.

Similarly, the generate_report function will take the discrepancy report from the reconcile task via XCom.

Now, setting up dependencies: all fetch tasks must complete before any normalization starts. Wait, no—each normalization task depends on its respective fetch task. For example, normalize_north depends on fetch_north. But according to the pipeline description, all fetch tasks complete before normalization begins. Wait, the orchestration says "all fetch tasks complete before normalization begins". Oh, that's different. So the four fetch tasks run in parallel, and all must finish before any normalization task starts. So each normalization task depends on all four fetch tasks? Or wait, the description says: "Task dependencies enforce proper sequencing: all fetch tasks complete before normalization begins". So all four fetch tasks must complete first, then the four normalization tasks can run in parallel. So the normalization tasks don't depend on just their respective fetch task, but all fetch tasks.

Wait, that's conflicting. Let me check the pipeline steps again. The first step is fetching CSV files in parallel from four locations. Then, normalization happens concurrently for all four. The orchestration section says: "all fetch tasks complete before normalization begins". So the normalization tasks can't start until all fetch tasks are done. So each normalization task depends on all four fetch tasks. But that's not efficient. Wait, maybe the user intended that each normalization task depends only on its own fetch task, but the description says "all fetch tasks complete before normalization begins". Hmm, perhaps the user made a mistake, but according to the given description, I need to follow that.

Wait, the pipeline steps say: "Normalize SKU formats concurrently for all four warehouses" after fetching. So the normalization can happen as soon as each respective CSV is fetched. But the orchestration section says "all fetch tasks complete before normalization begins". That's a conflict. Which one takes precedence?

The user provided the orchestration details, so I should follow that. So all four fetch tasks must complete before any normalization can start. Therefore, each normalization task depends on all four fetch tasks. But that would require each normalization task to have all four fetch tasks as upstream. But that's not efficient. Alternatively, maybe the user intended that normalization can start as soon as the respective fetch is done, but the reconcile step requires all normalization tasks to complete. Wait, the orchestration section says: "Task dependencies enforce proper sequencing: all fetch tasks complete before normalization begins, all normalization tasks complete before reconciliation starts". So normalization can't start until all fetch tasks are done. So even if north's fetch completes early, the normalization for north has to wait until south, east, west fetches are done.

That's a bit odd, but according to the description, that's the requirement. So the dependencies are:

fetch_north, fetch_south, fetch_east, fetch_west >> normalize_north, normalize_south, normalize_east, normalize_west

But that would mean each normalization task is downstream of all fetch tasks. So all four fetch tasks must complete before any normalization starts.

But in reality, if normalization for north only needs the north's CSV, why wait for others? But according to the user's description, that's the constraint. So perhaps the pipeline is designed such that normalization requires all CSV files to be present before proceeding, maybe for consistency. Or maybe it's a misunderstanding in the description. But as per the user's instructions, I need to follow the given task dependencies.

So, in the DAG, the four fetch tasks are all upstream of each normalization task. Wait, no—each normalization task depends on all four fetch tasks. So for example, normalize_north would be set to depend on [fetch_north, fetch_south, fetch_east, fetch_west]. But that's not efficient. Alternatively, perhaps the user intended that all fetch tasks must complete before any normalization task starts. So the normalization tasks can run in parallel, but only after all fetch tasks are done.

In Airflow, to model this, we can have the four fetch tasks run in parallel, then all normalization tasks run in parallel after all fetch tasks complete. So the dependencies would be:

[fetch_north, fetch_south, fetch_east, fetch_west] >> [normalize_north, normalize_south, normalize_east, normalize_west]

Yes, that's how you set it in Airflow. So all four fetch tasks are upstream of all four normalization tasks.

Then, the reconcile task depends on all four normalization tasks. So:

[normalize_north, normalize_south, normalize_east, normalize_west] >> reconcile_inventory

Then, reconcile_inventory >> generate_report.

Now, for the functions:

The fetch_warehouse_file function would take a warehouse name, simulate fetching a CSV, and push the file path via XCom. The normalize_sku_format function would take the warehouse name, pull the file path from the corresponding fetch task, process it, and push the normalized file path. But wait, if all normalization tasks depend on all fetch tasks, how does each normalization task know which fetch task's XCom to pull? Because if all fetch tasks have to complete before any normalization starts, each normalization task would have access to all four XComs. But each normalization task is specific to a warehouse. So perhaps each fetch task pushes its XCom with a specific key, like 'north_file', 'south_file', etc. Then, the normalization task for north would pull the XCom from the fetch_north task.

Wait, but if the normalization tasks are set to depend on all fetch tasks, then in the normalization function, how to get the correct XCom? For example, normalize_north needs the XCom from fetch_north. So in the normalize_sku_format function, for the 'north' warehouse, we need to get the XCom value from the 'fetch_north' task.

So in the PythonOperator for normalize_north, we can pass the task_id of the fetch task as 'fetch_task_id', and then in the function, use ti.xcom_pull(task_ids=fetch_task_id, key='file_path').

Alternatively, when setting up each normalization task, we can pass the specific fetch task_id as an argument. For example:

normalize_north = PythonOperator(
    task_id='normalize_north',
    python_callable=normalize_sku_format,
    op_args=['north', 'fetch_north'],
    ...
)

Then, in the normalize_sku_format function:

def normalize_sku_format(warehouse, fetch_task_id, **kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids=fetch_task_id, key='file_path')
    # process and push normalized file path
    normalized_path = f'normalized_{warehouse}.csv'
    ti.xcom_push(key='normalized_file', value=normalized_path)
    return normalized_path

This way, each normalization task knows which fetch task to pull XCom from.

But how to structure the op_args for each normalization task? For example, for normalize_north, op_args would be ['north', 'fetch_north'], and similarly for others.

Yes, that makes sense. So each normalization task is passed its warehouse name and the corresponding fetch task ID. Then, in the function, it pulls the XCom from that specific fetch task.

Then, the reconcile task needs to collect all four normalized files. So in the reconcile_inventory_discrepancies function, we can pull XCom from all four normalization tasks.

def reconcile_inventory_discrepancies(**kwargs):
    ti = kwargs['ti']
    normalized_files = []
    for warehouse in ['north', 'south', 'east', 'west']:
        file_path = ti.xcom_pull(task_ids=f'normalize_{warehouse}', key='normalized_file')
        normalized_files.append(file_path)
    # process files and generate discrepancy report
    discrepancy_report = 'discrepancy_report.csv'
    ti.xcom_push(key='discrepancy_report', value=discrepancy_report)
    return discrepancy_report

Similarly, the generate_report function pulls the discrepancy report from the reconcile task.

Putting it all together:

The DAG will have:

- Four fetch tasks, each with their own warehouse name.
- Four normalization tasks, each depending on all four fetch tasks, but pulling XCom from their respective fetch task.
- The reconcile task depends on all four normalization tasks.
- The generate report task depends on the reconcile task.

Now, setting up the dependencies correctly. The fetch tasks are all in parallel. Then, all normalization tasks are downstream of all fetch tasks. Then, reconcile is downstream of all normalization tasks. Then generate report is downstream of reconcile.

In Airflow, this can be done using the bitshift operators. For example:

fetch_tasks = [fetch_north, fetch_south, fetch_east, fetch_west]
normalize_tasks = [normalize_north, normalize_south, normalize_east, normalize_west]

# All fetch tasks must complete before any normalization task starts
fetch_tasks >> normalize_tasks

# All normalization tasks must complete before reconcile starts
normalize_tasks >> reconcile_inventory

# Reconcile must complete before generate report
reconcile_inventory >> generate_report

This structure ensures the parallel execution as per the description.

Now, the default_args should include the start_date, retries, retry_delay, and other parameters. The DAG is scheduled daily with no catchup.

Putting all this into code:

Import statements:

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

Default args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

Wait, the user mentioned start date is January 1, 2024. So start_date is set accordingly. The schedule_interval is '@daily'. Catchup is False.

The DAG definition:

with DAG(
    dag_id='retail_inventory_reconciliation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'inventory'],
) as dag:

Then, define the functions:

def fetch_warehouse_file(warehouse, **kwargs):
    # Simulate fetching CSV from warehouse
    file_path = f'/data/{warehouse}_inventory.csv'
    print(f"Mock fetching {warehouse} warehouse file: {file_path}")
    # Push file path to XCom
    kwargs['ti'].xcom_push(key='file_path', value=file_path)
    return file_path

def normalize_sku_format(warehouse, fetch_task_id, **kwargs):
    ti = kwargs['ti']
    # Pull file path from the specific fetch task
    file_path = ti.xcom_pull(task_ids=fetch_task_id, key='file_path')
    print(f"Mock normalizing {warehouse} SKUs from {file_path}")
    # Simulate transformations
    normalized_path = f'/data/normalized_{warehouse}_inventory.csv'
    print(f"Generated normalized file: {normalized_path}")
    ti.xcom_push(key='normalized_file', value=normalized_path)
    return normalized_path

def reconcile_inventory_discrepancies(**kwargs):
    ti = kwargs['ti']
    normalized_files = []
    for warehouse in ['north', 'south', 'east', 'west']:
        file = ti.xcom_pull(task_ids=f'normalize_{warehouse}', key='normalized_file')
        normalized_files.append(file)
    print("Reconciling inventory discrepancies with files:", normalized_files)
    discrepancy_report = '/data/discrepancy_report.csv'
    print(f"Generated discrepancy report: {discrepancy_report}")
    ti.xcom_push(key='discrepancy_report', value=discrepancy_report)
    return discrepancy_report

def generate_final_report(**kwargs):
    ti = kwargs['ti']
    report_path = ti.xcom_pull(task_ids='reconcile_inventory', key='discrepancy_report')
    print(f"Generating final reconciliation report from {report_path}")
    final_report = '/data/final_reconciliation_report.pdf'
    print(f"Generated final report: {final_report}")
    return final_report

Then, create the tasks:

fetch_north = PythonOperator(
    task_id='fetch_north',
    python_callable=fetch_warehouse_file,
    op_args=['north'],
)

fetch_south = PythonOperator(
    task_id='fetch_south',
    python_callable=fetch_warehouse_file,
    op_args=['south'],
)

fetch_east = PythonOperator(
    task_id='fetch_east',
    python_callable=fetch_warehouse_file,
    op_args=['east'],
)

fetch_west = PythonOperator(
    task_id='fetch_west',
    python_callable=fetch_warehouse_file,
    op_args=['west'],
)

normalize_north = PythonOperator(
    task_id='normalize_north',
    python_callable=normalize_sku_format,
    op_args=['north', 'fetch_north'],
)

normalize_south = PythonOperator(
    task_id='normalize_south',
    python_callable=normalize_sku_format,
    op_args=['south', 'fetch_south'],
)

normalize_east = PythonOperator(
    task_id='normalize_east',
    python_callable=normalize_sku_format,
    op_args=['east', 'fetch_east'],
)

normalize_west = PythonOperator(
    task_id='normalize_west',
    python_callable=normalize_sku_format,
    op_args=['west', 'fetch_west'],
)

reconcile_inventory = PythonOperator(
    task_id='reconcile_inventory',
    python_callable=reconcile_inventory_discrepancies,
)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_final_report,
)

Then, set dependencies:

fetch_tasks = [fetch_north, fetch_south, fetch_east, fetch_west]
normalize_tasks = [normalize_north, normalize_south, normalize_east, normalize_west]

fetch_tasks >> normalize_tasks
normalize_tasks >> reconcile_inventory
reconcile_inventory >> generate_report

Wait, but in Airflow, when you have a list of tasks, using >> between lists sets all upstream dependencies. So [a, b] >> [c, d] means a and b are upstream of both c and d.

In this case, the code fetch_tasks >> normalize_tasks means each fetch task is upstream of each normalization task. Which aligns with the requirement that all fetch tasks must complete before any normalization starts.

Yes, that's correct.

Now, check for PEP 8 compliance. Function names are lowercase with underscores. Task IDs are descriptive. Imports are correct. The code should be executable as long as the mock functions are in place.

I think that covers all the requirements. The code should now be structured properly with parallel tasks, dependencies, XCom usage, and the required parameters.
</think>

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def fetch_warehouse_file(warehouse, **kwargs):
    """Mock fetching CSV file from specified warehouse system"""
    print(f"Fetching {warehouse} warehouse inventory CSV")
    return f"/tmp/{warehouse}_inventory.csv"

def normalize_sku_format(warehouse, **kwargs):
    """Normalize warehouse-specific SKU format to standard"""
    ti = kwargs['ti']
    input_file = ti.xcom_pull(task_ids=f'fetch_{warehouse}')
    print(f"Normalizing {warehouse} SKUs from {input_file}")
    return f"/tmp/normalized_{warehouse}_skus.csv"

def reconcile_discrepancies(**kwargs):
    """Compare normalized files and generate discrepancy report"""
    ti = kwargs['ti']
    normalized_files = [ti.xcom_pull(task_ids=f'normalize_{wh}') 
                       for wh in ['north', 'south', 'east', 'west']]
    print(f"Reconciling {len(normalized_files)} normalized files")
    return "/tmp/discrepancy_report.csv"

def generate_final_report(**kwargs):
    """Create PDF report from discrepancy data"""
    ti = kwargs['ti']
    input_file = ti.xcom_pull(task_ids='reconcile_inventory')
    print(f"Generating final report from {input_file}")
    return "/tmp/final_reconciliation_report.pdf"

with DAG(
    'retail_inventory_reconciliation',
    default_args=default_args,
    description='Daily retail inventory reconciliation pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'inventory'],
) as dag:

    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{wh}',
            python_callable=fetch_warehouse_file,
            op_args=[wh],
        ) for wh in ['north', 'south', 'east', 'west']
    ]

    normalize_tasks = [
        PythonOperator(
            task_id=f'normalize_{wh}',
            python_callable=normalize_sku_format,
            op_args=[wh],
        ) for wh in ['north', 'south', 'east', 'west']
    ]

    reconcile = PythonOperator(
        task_id='reconcile_inventory',
        python_callable=reconcile_discrepancies,
    )

    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_final_report,
    )

    fetch_tasks >> normalize_tasks >> reconcile >> report