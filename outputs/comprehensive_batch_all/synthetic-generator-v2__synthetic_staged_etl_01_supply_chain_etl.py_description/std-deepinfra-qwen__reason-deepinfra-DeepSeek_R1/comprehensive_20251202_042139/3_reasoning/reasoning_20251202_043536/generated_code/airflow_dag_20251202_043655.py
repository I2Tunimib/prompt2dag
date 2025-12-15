<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the pipeline has three stages: Extract, Transform, Load. The Extract stage has three parallel tasks for vendors A, B, and C. Then Transform depends on all three extract tasks. Load and send email are sequential in the Load stage.

The user specified Airflow 2.x style imports, so I'll use the correct modules. Since there are no sensors or branching mentioned, I don't need to include those operators.

Default args should include the start date, retries, retry delay, and email settings. The schedule is daily, catchup is False. So the DAG will have schedule_interval='@daily', catchup=False.

TaskGroups are required for each stage. I'll create three TaskGroups: extract, transform, load. Wait, the Load stage has two tasks: load_to_db and send_summary_email. The email task depends on the load task, so they should be in order within the load group.

Each of the extract tasks (vendor A, B, C) will be PythonOperators. The functions for these can be placeholders with a docstring explaining their purpose. Similarly, the transform and load tasks will be PythonOperators. The send_email can use the EmailOperator, but wait, the user mentioned to use PythonOperator if needed. Wait, the pipeline says to send an email notification. The EmailOperator is part of Airflow, so I should use that. But I need to make sure to import it. Let me check: from airflow.operators.email import EmailOperator.

Wait, the user's constraints say if sensors appear, demonstrate proper use, but there are none here. So no sensors needed.

The dependencies: extract tasks (all three) >> transform task >> load task >> send email. So within the TaskGroups, the extract group's tasks are parallel, then transform, then load and email in sequence.

XCom is used to pass data between stages. So the extract tasks need to push some data (maybe file paths or record counts), the transform task pulls them, processes, and pushes the cleansed data, then the load task uses that.

But since the code is illustrative, the functions can have mock implementations with print statements and xcom pushes.

Now, putting it all together:

Import necessary modules: DAG, PythonOperator, EmailOperator, TaskGroup, default_args.

Set default_args with owner, start_date, retries, retry_delay, email on failure, etc. The problem states failure email notifications are disabled, so 'email_on_failure': False.

Define the DAG with dag_id, default_args, schedule_interval, catchup, tags.

Then, using the DAG context manager, create three TaskGroups: extract, transform, load.

In the extract group, three PythonOperators for each vendor. Their python_callable functions would be defined to do the extraction, perhaps pushing a dummy XCom value.

The transform task is a PythonOperator in the transform group, which depends on all three extract tasks. Its function would pull the XComs from the extract tasks, process, and push the result.

The load task is a PythonOperator in the load group, depending on transform. Then the send_email is an EmailOperator, depending on load.

Wait, the EmailOperator requires the email to be set up in Airflow's configuration. But the user wants executable code, so I'll include it with a dummy email address. The task will have the to_email as specified.

Now, code structure:

Imports first.

Then default_args.

Then the DAG definition.

Within the DAG, TaskGroups.

The functions for each operator: ingest_vendor_a, etc., cleanse_data, load_to_db, and send_summary_email (though the email is an EmailOperator, so no function needed).

Wait, the send_summary_email is an EmailOperator, so its parameters are to, subject, html_content, etc. The subject and content can include Jinja templates to pull execution dates or XCom data.

But since the example is minimal, maybe hardcode some values, but use execution_date.

Putting it all together:

The EmailOperator's html_content could be a simple string with placeholders, using {{ ds }} for the execution date. But since it's a PythonOperator, wait no—EmailOperator is a separate operator. So:

send_summary_email = EmailOperator(
    task_id='send_summary_email',
    to='supply-chain-team@company.com',
    subject='Shipment Data Processing Summary {{ ds }}',
    html_content='...',
)

But the content might include data from XCom, like record counts. So the transform task would push the count to XCom, and the email task can pull it via Jinja templating.

But for simplicity, maybe the email just includes the execution date. The problem states the email includes processing date, record counts, etc. So perhaps the transform task pushes a XCom with the count, and the email task uses that.

But in the code, since it's a mock example, the cleanse_data function can push xcom with a count, say 3850.

Then, in the EmailOperator's html_content, we can reference that via a template.

But how? The load_to_db task would need to pull that XCom, or maybe the send_email task can pull it from the transform task's XCom.

Alternatively, the cleanse_data pushes to XCom, and the send_email task pulls it via {{ ti.xcom_pull(task_ids='transform.cleanse_data') }}.

But in the code, the functions are placeholders. So in the EmailOperator, the html_content could be:

html_content='''<p>Processed {{ ti.xcom_pull(task_ids='transform.cleanse_data') }} records.</p>''',

But in the mock functions, the cleanse_data would do something like:

def cleanse_data(**kwargs):
    # processing logic
    processed_records = 3850
    kwargs['ti'].xcom_push(key='processed_records', value=processed_records)

Then, the email task's html_content can reference this via:

html_content='''<p>Processed {{ ti.xcom_pull(task_ids='transform.cleanse_data', key='processed_records') }} records.</p>''',

But maybe for simplicity, the code can include that.

Now, putting all together:

The code will have:

- Imports for DAG, PythonOperator, EmailOperator, TaskGroup, etc.
- default_args with the specified parameters.
- DAG instance with the given schedule and catchup.
- TaskGroups for each stage.
- Tasks within each group with dependencies.
- Functions for PythonOperators that push/pull XComs.
- EmailOperator with appropriate parameters.

Testing for PEP8 compliance, proper indentation, and correct task dependencies.

Also, the user mentioned to use TaskGroups for logical stage grouping, so each stage (extract, transform, load) is a TaskGroup.

So, the code outline is:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='supply_chain_shipment_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['supply_chain'],
) as dag:

    with TaskGroup('extract') as extract_group:
        # Three tasks here
        ingest_vendor_a = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a_func,
        )
        # Similarly for B and C

    with TaskGroup('transform') as transform_group:
        cleanse_data = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data_func,
        )

    with TaskGroup('load') as load_group:
        load_to_db = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db_func,
        )
        send_summary_email = EmailOperator(
            task_id='send_summary_email',
            to='supply-chain-team@company.com',
            subject='Shipment Data Processed - {{ ds }}',
            html_content="""...""",
        )

    # Dependencies
    extract_group >> transform_group >> load_group
    load_to_db >> send_summary_email

Wait, but within the load_group, load_to_db must come before send_summary_email. So inside the load_group, the order is load_to_db then send_summary_email, so within the group:

load_to_db = PythonOperator(...)
send_summary_email = EmailOperator(...)
load_to_db >> send_summary_email

But when using TaskGroup, the dependencies inside the group are set as usual.

Then, the overall dependencies are extract_group >> transform_group >> load_group. But the load_group's internal tasks have their own dependencies.

Wait, the load_group contains both load_to_db and send_summary_email. So the dependencies would be:

transform_group >> load_to_db
load_to_db >> send_summary_email

But since they are inside the load_group, perhaps the code should set dependencies within the group.

Alternatively, the code can have:

extract_group >> transform_group >> load_to_db
load_to_db >> send_summary_email

But since they are in the same TaskGroup, maybe arrange them within the group.

But the TaskGroup is just for grouping in the UI; the dependencies are set as normal.

So, putting it all together:

The code would have:

In the 'load' TaskGroup:

load_to_db = PythonOperator(...)
send_summary_email = EmailOperator(...)
load_to_db >> send_summary_email

Then, outside the TaskGroups, the dependencies are:

extract_group >> transform_group >> load_group

Wait, no. Because transform_group is the cleanse_data task. So transform_group (which is the cleanse_data task) must come after extract_group (the three ingest tasks). Then, the cleanse_data must be followed by load_to_db, which is inside the load_group.

So the dependencies should be:

extract_group >> transform_group
transform_group >> load_group.load_to_db
load_group.load_to_db >> load_group.send_summary_email

But when using TaskGroups, the tasks inside are accessed via the group. So:

transform_group >> load_to_db

But within the DAG context, the load_to_db is inside the load_group, so perhaps:

transform_group >> load_group.load_to_db

But in code, when using TaskGroup, the task IDs are prefixed. For example, if the TaskGroup is 'load', then the task_id 'load_to_db' becomes 'load.load_to_db'. So when setting dependencies, you can reference them as load_to_db (if within the same group) or via the group's object.

Alternatively, when using the TaskGroup as a context manager, the tasks are added to the group, and their dependencies are set within the group.

But perhaps the correct way is:

Inside the 'load' TaskGroup:

with TaskGroup('load') as load_group:
    load_to_db = PythonOperator(...)
    send_summary_email = EmailOperator(...)
    load_to_db >> send_summary_email

Then, outside, the dependencies are:

extract_group >> transform_group >> load_to_db

But since transform_group is the cleanse_data task, which is in the 'transform' TaskGroup, the code would be:

extract_group >> transform_group >> load_group

But load_group is a group containing both load_to_db and send_summary_email. So transform_group (cleanse_data) must come before load_to_db.

So the correct dependency is:

transform_group >> load_to_db

But since load_to_db is inside the load_group, how to reference it? Maybe:

transform_group >> load_group['load_to_db']

Alternatively, when using the TaskGroup context manager, the tasks are variables inside. So:

transform_group >> load_group.load_to_db

But in Python, the TaskGroup's children are stored in a mapping, so perhaps using the >> operator between the transform_group (which is a task group) and the load_to_db task.

Wait, no. The transform_group is a TaskGroup, which itself doesn't represent a task. The actual task is the cleanse_data inside the transform_group. So perhaps the code should have:

extract_group >> transform_group.cleanse_data >> load_group.load_to_db

But when using TaskGroup, the tasks inside are in the group, so their IDs are like 'transform.cleanse_data'. So in dependencies, you can refer to them by their task_id, but using the group's object may not directly allow that.

Alternatively, perhaps the code should set dependencies between the groups as a whole. For example:

extract_group >> transform_group >> load_group

But that would mean all tasks in extract_group are upstream of all tasks in transform_group, which is correct because the transform task depends on all extract tasks. Then, the transform task is upstream of all tasks in load_group. But in the load_group, the load_to_db is the first task, which depends on transform, and send_email depends on load_to_db.

So perhaps:

extract_group >> transform_group >> load_group

But how does that translate to the actual tasks? The transform_group's task is cleanse_data, which is a single task. So:

extract_group >> transform_group.cleanse_data

Then, transform_group.cleanse_data >> load_group.load_to_db

But within the DAG context, how to reference the tasks inside the groups.

Alternatively, perhaps the code should be structured with:

In the extract TaskGroup, the three vendor tasks are in parallel. Then, the transform_group's cleanse_data depends on all three extract tasks. Then, the load_group's load_to_db depends on cleanse_data, and send_email depends on load_to_db.

So the dependencies are:

ingest_vendor_a >> cleanse_data
ingest_vendor_b >> cleanse_data
ingest_vendor_c >> cleanse_data

cleanse_data >> load_to_db

load_to_db >> send_summary_email

But since they are inside TaskGroups, perhaps the code should use the group's children.

But when using the TaskGroup context manager, the tasks are added to the group, but their IDs are accessible via the group's name. For example, if the extract group has tasks ingest_vendor_a, b, c, their task IDs are 'extract.ingest_vendor_a', etc.

So, in the code, after defining all groups, the dependencies can be set as:

[ingest_vendor_a, ingest_vendor_b, ingest_vendor_c] >> cleanse_data

cleanse_data >> load_to_db

load_to_db >> send_summary_email

But since these tasks are inside their respective TaskGroups, perhaps the code should reference them via their variables.

Wait, inside the 'extract' TaskGroup, the code defines ingest_vendor_a, etc. So in the code:

with TaskGroup('extract') as extract_group:
    ingest_vendor_a = PythonOperator(...)
    ingest_vendor_b = PythonOperator(...)
    ingest_vendor_c = PythonOperator(...)

Then, outside of that group, but within the DAG context, the code can set:

ingest_vendor_a >> cleanse_data

But cleanse_data is inside the transform_group:

with TaskGroup('transform') as transform_group:
    cleanse_data = PythonOperator(...)

So, the code can directly reference ingest_vendor_a and cleanse_data as variables, because they are assigned in the outer scope.

So, the dependencies would be:

ingest_vendor_a >> cleanse_data
ingest_vendor_b >> cleanse_data
ingest_vendor_c >> cleanse_data

Then:

cleanse_data >> load_to_db

load_to_db >> send_summary_email

But load_to_db is inside the load_group:

with TaskGroup('load') as load_group:
    load_to_db = PythonOperator(...)
    send_summary_email = EmailOperator(...)
    load_to_db >> send_summary_email

So, the code can reference cleanse_data >> load_to_db directly.

Putting it all together, the code would have the three TaskGroups, and then the dependencies are set between the individual tasks across groups.

But the user's example says to use explicit dependencies with >>, so the code should explicitly link the tasks.

So, the final code would look like:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def ingest_vendor_a(**kwargs):
    """Ingest Vendor A data."""
    print("Ingesting Vendor A data (1250 records)")
    kwargs['ti'].xcom_push(key='vendor_a', value=1250)

def ingest_vendor_b(**kwargs):
    """Ingest Vendor B data."""
    print("Ingesting Vendor B data (980 records)")
    kwargs['ti'].xcom_push(key='vendor_b', value=980)

def ingest_vendor_c(**kwargs):
    """Ingest Vendor C data."""
    print("Ingesting Vendor C data (1750 records)")
    kwargs['ti'].xcom_push(key='vendor_c', value=1750)

def cleanse_data(**kwargs):
    """Clean and normalize data."""
    ti = kwargs['ti']
    a = ti.xcom_pull(task_ids='extract.ingest_vendor_a', key='vendor_a')
    b = ti.xcom_pull(task_ids='extract.ingest_vendor_b', key='vendor_b')
    c = ti.xcom_pull(task_ids='extract.ingest_vendor_c', key='vendor_c')
    total = a + b + c
    print(f"Cleansing {total} records")
    ti.xcom_push(key='processed_records', value=3850)

def load_to_db(**kwargs):
    """Load processed data to database."""
    records = kwargs['ti'].xcom_pull(task_ids='transform.cleanse_data', key='processed_records')
    print(f"Loading {records} records to PostgreSQL database")

with DAG(
    dag_id='supply_chain_shipment_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['supply_chain'],
) as dag:

    with TaskGroup('extract') as extract_group:
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

    with TaskGroup('transform') as transform_group:
        cleanse_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
        )

    with TaskGroup('load') as load_group:
        load_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
        )
        email_task = EmailOperator(
            task_id='send_summary_email',
            to='supply-chain-team@company.com',
            subject='Shipment Processing Summary {{ ds }}',
            html_content="""<p>Processed {{ ti.xcom_pull(task_ids='transform.cleanse_data', key='processed_records') }} records.</p>
            <p>Data quality metrics: ...</p>""",
        )
        load_task >> email_task

    # Set dependencies
    vendor_a_task >> cleanse_task
    vendor_b_task >> cleanse_task
    vendor_c_task >> cleanse_task
    cleanse_task >> load_task

Wait, but vendor_a_task is inside the extract_group, which is a TaskGroup. So their task IDs are 'extract.ingest_vendor_a', etc. But in the code above, the variables vendor_a_task, vendor_b_task, vendor_c_task are assigned within the extract_group context. Then, the code sets dependencies between those tasks and cleanse_task (which is in transform_group).

Yes, this should work because the variables are accessible within the DAG context.

So, the code sets:

vendor_a_task >> cleanse_task
vendor_b_task >> cleanse_task
vendor_c_task >> cleanse_task

Which makes all three extract tasks upstream of the cleanse_task.

Then, cleanse_task >> load_task (which is inside the load_group), and load_task >> email_task (inside the load_group).

This correctly models the dependencies.

The EmailOperator's html_content uses a Jinja template to pull the XCom value from the cleanse_data task. The key 'processed_records' is pushed by the cleanse_data function.

The functions for the extract tasks push their record counts, and the cleanse_data function sums them but in the problem statement, after cleansing, it's 3850 records, so the mock function pushes that value regardless.

The load_to_db function pulls the processed_records from XCom.

This code should meet all the requirements: TaskGroups for each stage, parallel extraction, sequential stages, proper dependencies, XCom usage, EmailOperator with templated content, and correct imports.

I need to check for PEP8 compliance, like proper indentation, line lengths, etc. Also, ensure that all tasks are properly connected.

Yes, this should be correct.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def ingest_vendor_a(**kwargs):
    """Ingest raw shipment data from Vendor A."""
    print("Ingesting 1250 records from Vendor A")
    kwargs['ti'].xcom_push(key='vendor_a', value=1250)

def ingest_vendor_b(**kwargs):
    """Ingest raw shipment data from Vendor B."""
    print("Ingesting 980 records from Vendor B")
    kwargs['ti'].xcom_push(key='vendor_b', value=980)

def ingest_vendor_c(**kwargs):
    """Ingest raw shipment data from Vendor C."""
    print("Ingesting 1750 records from Vendor C")
    kwargs['ti'].xcom_push(key='vendor_c', value=1750)

def cleanse_data(**kwargs):
    """Process and normalize combined vendor data."""
    ti = kwargs['ti']
    total = sum([
        ti.xcom_pull(task_ids='extract.ingest_vendor_a', key='vendor_a'),
        ti.xcom_pull(task_ids='extract.ingest_vendor_b', key='vendor_b'),
        ti.xcom_pull(task_ids='extract.ingest_vendor_c', key='vendor_c')
    ])
    print(f"Cleansing {total} raw records")
    ti.xcom_push(key='processed_records', value=3850)

def load_to_db(**kwargs):
    """Load processed records to PostgreSQL database."""
    records = kwargs['ti'].xcom_pull(task_ids='transform.cleanse_data', key='processed_records')
    print(f"Upserting {records} records to inventory_shipments table")

with DAG(
    dag_id='supply_chain_shipment_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['supply_chain'],
) as dag:

    with TaskGroup('extract') as extract_group:
        vendor_a = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a
        )
        vendor_b = PythonOperator(
            task_id='ingest_vendor_b',
            python_callable=ingest_vendor_b
        )
        vendor_c = PythonOperator(
            task_id='ingest_vendor_c',
            python_callable=ingest_vendor_c
        )

    with TaskGroup('transform') as transform_group:
        cleanse = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data
        )

    with TaskGroup('load') as load_group:
        load = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db
        )
        notify = EmailOperator(
            task_id='send_summary_email',
            to='supply-chain-team@company.com',
            subject='Shipment Data Summary {{ ds }}',
            html_content="""<p>Processed {{ ti.xcom_pull(task_ids='transform.cleanse_data', key='processed_records') }} records</p>
            <p>Successfully loaded to inventory database</p>"""
        )
        load >> notify

    # Set pipeline dependencies
    [vendor_a, vendor_b, vendor_c] >> cleanse >> load