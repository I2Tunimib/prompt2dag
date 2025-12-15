<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the pipeline has three stages: Extract, Transform, Load. The Extract stage has three parallel tasks for vendors A, B, and C. Then Transform depends on all three extract tasks. Load and send email are sequential in the third stage.

The user specified Airflow 2.x style imports, so I'll use the correct modules. Since there are no sensors or branching mentioned, I don't need to include those operators. But I should make sure to use PythonOperator for the tasks.

Default args should include the start date, retries (2), retry delay (5 minutes), and set email_on_failure to False. The DAG should be scheduled daily, with catchup disabled.

Each stage is grouped into TaskGroups. Wait, the user mentioned using TaskGroups for logical stage grouping. So I need to structure the code with TaskGroups for each stage.

For the tasks: ingest_vendor_a, b, c are parallel. Then cleanse_data depends on all three. Then load_to_db and send_summary_email are sequential, with load_to_db first.

The functions for each task will be placeholders since the actual data processing isn't detailed. I'll define them with pass or simple print statements.

XCom is used to pass data between stages, so the extract functions should push some data, and cleanse_data would pull them. But since the example is about structure, maybe the functions just simulate that.

I need to set dependencies correctly. The extract tasks are in parallel, then cleanse_data depends on all of them. Then load_to_db depends on cleanse_data, and send_summary_email depends on load_to_db.

Putting it all together:

- Import necessary modules: DAG, PythonOperator, TaskGroup, etc.
- Define default_args with the given parameters.
- Instantiate the DAG with the specified schedule and catchup settings.
- Use TaskGroups for each stage.
- Define the functions for each task.
- Set up dependencies using >> operators.

Wait, the user mentioned "minimal but meaningful default_args". So include owner, depends_on_past (probably False), email_on_failure False, retries, etc.

Also, the start date is January 1, 2024. Schedule interval is daily, so schedule_interval="@daily".

Let me structure the code step by step.

First, the imports:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

Then default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

Then the DAG definition:

with DAG(
    dag_id='supply_chain_shipment_etl',
    default_args=default_args,
    description='ETL pipeline for supply chain shipment data',
    schedule_interval='@daily',
    catchup=False,
    tags=['supply_chain', 'etl'],
) as dag:

Now, create TaskGroups for each stage.

Stage 1: Extract with three tasks in parallel.

with TaskGroup('extract_stage') as extract_group:
    ingest_vendor_a = PythonOperator(
        task_id='ingest_vendor_a',
        python_callable=ingest_vendor_a_func,
    )
    # Similarly for B and C

But need to define the functions. Let's create dummy functions.

def ingest_vendor_a_func(**kwargs):
    print("Ingesting data from Vendor A")

Similarly for B and C.

Then, in the extract_group, all three tasks are in parallel. So their dependencies are not set here; they'll be part of the group and then the group's tasks are all siblings.

Then Stage 2: Transform with cleanse_data.

with TaskGroup('transform_stage') as transform_group:
    cleanse_data = PythonOperator(
        task_id='cleanse_data',
        python_callable=cleanse_data_func,
    )

def cleanse_data_func(**kwargs):
    print("Cleansing and normalizing data")

Stage 3: Load and notification.

with TaskGroup('load_stage') as load_group:
    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db_func,
    )
    send_summary_email = PythonOperator(
        task_id='send_summary_email',
        python_callable=send_summary_email_func,
    )
    load_to_db >> send_summary_email

def load_to_db_func(**kwargs):
    print("Loading data to PostgreSQL")

def send_summary_email_func(**kwargs):
    print("Sending summary email")

Now, setting dependencies between stages:

extract_group >> transform_group >> load_group

Wait, but within the groups, the tasks have their own dependencies. For extract_group, the three tasks are parallel, so in the group, they don't depend on each other. Then transform_group's cleanse_data depends on all three extract tasks. But how to set that?

Wait, when using TaskGroups, the group itself is a node, but if the tasks inside the group need to be dependencies for tasks in another group, you have to reference them directly.

Wait, no. The TaskGroup is just a UI grouping. The actual dependencies are between the individual tasks. So in this case, the three ingest tasks are in the extract_group, and the cleanse_data is in transform_group. So the dependencies should be:

ingest_vendor_a >> cleanse_data
ingest_vendor_b >> cleanse_data
ingest_vendor_c >> cleanse_data

But since they are in different TaskGroups, how to reference them?

Alternatively, perhaps the TaskGroups are just for organization, and the dependencies are set between the tasks across groups.

Wait, maybe I should structure the code with the TaskGroups, but inside each group, the tasks are defined, and then dependencies are set between groups.

But the problem is that the three ingest tasks are in parallel, and the cleanse_data depends on all three. So the correct way is to have all three ingest tasks precede cleanse_data.

So perhaps the TaskGroups are for visual grouping, but the dependencies are set as:

[ingest_vendor_a, ingest_vendor_b, ingest_vendor_c] >> cleanse_data

Then cleanse_data >> load_to_db >> send_summary_email.

But how to structure this with TaskGroups.

Wait, maybe the TaskGroups are nested under each stage. So:

Within the DAG context:

with TaskGroup('extract_stage') as extract_group:
    ingest_vendor_a = PythonOperator(...)
    ingest_vendor_b = PythonOperator(...)
    ingest_vendor_c = PythonOperator(...)

Then, outside of the extract_group, but inside the DAG, define the transform_group:

with TaskGroup('transform_stage') as transform_group:
    cleanse_data = PythonOperator(...)

Similarly for load_group.

Then, dependencies:

ingest_vendor_a >> cleanse_data
ingest_vendor_b >> cleanse_data
ingest_vendor_c >> cleanse_data

cleanse_data >> load_to_db
load_to_db >> send_summary_email

But since the load_to_db and send_summary_email are inside the load_group, which is a TaskGroup, perhaps the dependencies are set within the load_group.

Wait, no. The load_group has two tasks: load_to_db and send_summary_email, with load_to_db >> send_summary_email. Then, the cleanse_data (from transform_group) should precede load_to_db.

So:

transform_group >> load_group

But transform_group's task is cleanse_data, and load_group's first task is load_to_db. So:

cleanse_data >> load_to_db

But if the load_group is a TaskGroup containing load_to_db and send_summary_email, then:

cleanse_data >> load_group.load_to_db

But in Airflow, when you have a task inside a TaskGroup, its task_id is group_id.task_id. So to reference it, you need to use the full task ID. Alternatively, when using the TaskGroup context manager, you can assign the operators to variables and then set dependencies.

Alternatively, perhaps the TaskGroups are just for UI organization and don't affect the task IDs. So when you create a PythonOperator inside a TaskGroup, its task_id becomes something like 'extract_stage.ingest_vendor_a'.

But when setting dependencies, you can refer to the task objects directly.

So, inside the DAG:

with TaskGroup('extract_stage') as extract_group:
    ingest_vendor_a = PythonOperator(task_id='ingest_vendor_a', ...)
    ingest_vendor_b = PythonOperator(task_id='ingest_vendor_b', ...)
    ingest_vendor_c = PythonOperator(task_id='ingest_vendor_c', ...)

Then, in the transform_group:

with TaskGroup('transform_stage') as transform_group:
    cleanse_data = PythonOperator(task_id='cleanse_data', ...)

Then, dependencies:

[ingest_vendor_a, ingest_vendor_b, ingest_vendor_c] >> cleanse_data

Similarly, in the load_group:

with TaskGroup('load_stage') as load_group:
    load_to_db = PythonOperator(task_id='load_to_db', ...)
    send_summary_email = PythonOperator(task_id='send_summary_email', ...)
    load_to_db >> send_summary_email

Then, cleanse_data >> load_to_db

So putting it all together:

The code structure would be:

- Imports
- Default args
- DAG definition
- Within DAG:
   - extract_group TaskGroup with three ingest tasks
   - transform_group TaskGroup with cleanse_data
   - load_group TaskGroup with load and email tasks
- Set dependencies between the tasks across groups

So the code would look like this:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def ingest_vendor_a(**context):
    print("Ingesting Vendor A data")

def ingest_vendor_b(**context):
    print("Ingesting Vendor B data")

def ingest_vendor_c(**context):
    print("Ingesting Vendor C data")

def cleanse_data(**context):
    print("Cleansing and normalizing data")

def load_to_db(**context):
    print("Loading data into PostgreSQL")

def send_summary_email(**context):
    print("Sending summary email")

with DAG(
    dag_id='supply_chain_shipment_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['supply_chain', 'etl'],
) as dag:

    with TaskGroup('extract_stage') as extract_group:
        ingest_vendor_a_task = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a,
        )
        ingest_vendor_b_task = PythonOperator(
            task_id='ingest_vendor_b',
            python_callable=ingest_vendor_b,
        )
        ingest_vendor_c_task = PythonOperator(
            task_id='ingest_vendor_c',
            python_callable=ingest_vendor_c,
        )

    with TaskGroup('transform_stage') as transform_group:
        cleanse_data_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
        )

    with TaskGroup('load_stage') as load_group:
        load_to_db_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
        )
        send_summary_email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
        )
        load_to_db_task >> send_summary_email_task

    # Set dependencies between stages
    [ingest_vendor_a_task, ingest_vendor_b_task, ingest_vendor_c_task] >> cleanse_data_task
    cleanse_data_task >> load_to_db_task

Wait, but in the load_group, the load_to_db_task is inside the group. So when referencing it outside the group, it's just the variable load_to_db_task. So the dependencies are correctly set.

Yes, this should work. The extract tasks are in parallel, then all three go to cleanse_data, which then goes to load_to_db, which then goes to send_summary_email.

I think that's the correct structure. Now, check for PEP 8 compliance, proper imports, and correct use of context managers.

Also, ensure that the functions have **kwargs or **context to handle Airflow context variables if needed, even if they don't use them.

The email task's function just prints, but in reality, it would use EmailOperator, but the user specified to use PythonOperator, so that's okay.

No sensors or branching, so no need for those operators.

XCom is mentioned, but the example doesn't require explicit handling since the user said to include minimal code. So the functions can be placeholders.

Thus, the code should be as written above.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def ingest_vendor_a(**context):
    """Mock function for Vendor A data ingestion"""
    print("Ingesting 1250 records from Vendor A CSV")

def ingest_vendor_b(**context):
    """Mock function for Vendor B data ingestion"""
    print("Ingesting 980 records from Vendor B CSV")

def ingest_vendor_c(**context):
    """Mock function for Vendor C data ingestion"""
    print("Ingesting 1750 records from Vendor C CSV")

def cleanse_data(**context):
    """Mock data transformation function"""
    print("Cleansing 3850 records: normalizing SKUs, validating dates, enriching locations")

def load_to_db(**context):
    """Mock database loading function"""
    print("Upserting 3850 records to PostgreSQL inventory_shipments table")

def send_summary_email(**context):
    """Mock email notification function"""
    print("Sending shipment summary to supply-chain-team@company.com")

with DAG(
    dag_id='supply_chain_shipment_etl',
    default_args=default_args,
    description='Daily ETL pipeline for supply chain shipments',
    schedule_interval='@daily',
    catchup=False,
    tags=['supply_chain', 'etl'],
) as dag:

    with TaskGroup('extract_stage') as extract_group:
        vendor_a_task = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a,
        )
        vendor_b_task = PythonOperator(
            task_id='ingest_vendor_b',
            python_callable=ingest_vendor_b,
        )
        vendor_c_task = PythonOperator(
            task_id='ingest_vendor_c',
            python_callable=ingest_vendor_c,
        )

    with TaskGroup('transform_stage') as transform_group:
        cleanse_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
        )

    with TaskGroup('load_stage') as load_group:
        load_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
        )
        email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
        )
        load_task >> email_task

    # Set pipeline dependencies
    [vendor_a_task, vendor_b_task, vendor_c_task] >> cleanse_task
    cleanse_task >> load_task