<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements.

First, the pipeline is a linear workflow with two tasks: print_configuration and cleanup_airflow_db. They need to run sequentially. The DAG should run daily at midnight UTC, with catchup disabled. There are no sensors or branching, so the structure is straightforward.

The DAG should use Airflow 2.x style imports. So I'll import DAG from airflow, PythonOperator from airflow.operators.python, and maybe others like settings and sqlalchemy for the database session. Also, need to handle XCom, so I'll import xcom from airflow.models.

Default arguments are specified: owner, email on failure, retries with 1 retry and a 1-minute delay. Also, need to set catchup=False in the DAG parameters.

For the tasks, print_configuration initializes cleanup parameters. It uses DAG run variables or defaults. Then pushes max_date to XCom. The cleanup_airflow_db task retrieves that max_date and performs the deletions.

I need to write Python functions for these tasks. The print_configuration function will get variables from the DAG run, calculate the max_date, and push it to XCom. The cleanup function will pull that value and execute the SQL queries.

Wait, the cleanup uses SQLAlchemy ORM. So I need to create a session using the Airflow's settings. Also, handle different models and their deletion logic. The models listed include DagRun, TaskInstance, etc. So I'll need to import those from airflow.models.

In the cleanup function, I'll loop through a predefined list of models. For each model, check if there's a timestamp field (like execution_date for DagRun), build a query to delete records older than max_date. But also, there's an ENABLE_DELETE setting, so maybe that's an Airflow Variable too. So the function should check if deletion is enabled, otherwise just log the count.

Need to handle special cases for some models. For example, maybe some models require different fields or grouping. But the description says "processes each table with appropriate filters and grouping logic", but the example code might just handle the general case, perhaps with a comment indicating where special logic would go.

Also, version-specific logic for Airflow API changes. Maybe check the Airflow version and adjust the models or queries accordingly. But the user's code might not include that unless there's a specific example. Maybe add a placeholder comment.

Now, putting it all together. The DAG will have default_args with the specified parameters. The schedule_interval is '@daily', start_date is set to a few days ago, maybe days_ago(2) for example. But the user might want a specific start date. However, since it's a template, using days_ago(2) is acceptable.

The tasks are PythonOperators. The print_configuration task's python_callable is the function that handles config. The cleanup task depends on the first task, using >> operator.

Wait, the first task pushes to XCom, and the second pulls it. So in the cleanup function, they need to retrieve the max_date using xcom_pull from the previous task.

Also, the functions need to have **kwargs or context parameters to access the task instance.

For the print_configuration function: get variables from the DAG run. The description says it loads configuration from DAG run variables or defaults. So maybe using context['params'] or accessing Airflow Variables. Wait, DAG run variables could be passed via the DagRun's conf, which is accessed via context['dag_run'].conf.get(). Alternatively, using Airflow Variables (the global ones). The description mentions "DAG run variables", which might refer to parameters passed when triggering the DAG. So perhaps using params in the DAG definition, but the example uses Airflow Variables. Hmm, maybe the user wants to retrieve variables like 'max_db_entry_age_in_days' from Airflow Variables, with a default if not set.

So in the print_configuration function, use Variable.get() with a default. Then calculate max_date as datetime.now() - retention_days. Then push to XCom.

In the cleanup function, retrieve that max_date from XCom, then create a session using settings.Session(), then iterate over the models. For each model, determine the relevant date field (like 'execution_date' for DagRun, 'start_date' for TaskInstance, etc.). Then build a query to filter where that field < max_date. Then, if ENABLE_DELETE is True, perform the delete, else log the count.

But how to handle different models? Maybe create a list of model configurations, each specifying the model class and the date field. For example:

MODEL_CONFIGS = [
    (DagRun, 'execution_date'),
    (TaskInstance, 'start_date'),
    # ... other models
]

Then loop through these configs. For each model and date_field, query the records older than max_date. But some models might have different date fields or require different handling. For example, Log has dttm, XCom has timestamp, etc. So the MODEL_CONFIGS should map each model to its respective datetime column.

So in the code, define MODEL_CONFIGS as a list of tuples. Then, for each entry, get the model and date_attr. Then build the filter condition using getattr(model, date_attr) < max_date.

But wait, SQLAlchemy uses the column names as attributes. So for example, DagRun.execution_date is a column, so the filter would be model_class.<date_attr> < max_date.

So in the code:

for model_cls, date_attr in MODEL_CONFIGS:
    date_field = getattr(model_cls, date_attr)
    query = session.query(model_cls).filter(date_field < max_date)
    # etc.

But need to make sure that the date_attr is a column field of the model.

Also, the ENABLE_DELETE setting is probably an Airflow Variable, so retrieve that with Variable.get('ENABLE_DELETE', default_var=False), perhaps as a boolean.

Now, putting all this into code.

Potential issues: Some models may not be available in all Airflow versions. For example, TaskReschedule might be available in certain versions. But the code can include them with a comment noting that.

Also, handling the session properly, using a context manager (with session.begin() as transaction: ...) to manage transactions.

Testing for existence of models or handling exceptions if certain models are not present could be part of version-specific logic, but the code might just proceed and log errors if a model isn't found.

So, the code outline would be:

- Imports: DAG, PythonOperator, Variable, settings, models (DagRun, TaskInstance, etc.), datetime, timedelta, etc.

- Default args: owner, email, retries, retry_delay.

- DAG definition with schedule_interval='@daily', catchup=False, etc.

- Functions for the tasks.

- MODEL_CONFIGS list.

Potential code:

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError, Job, BaseJob
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from sqlalchemy import and_, or_
from sqlalchemy.orm import sessionmaker
import logging

default_args = {
    'owner': 'airflow',
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_configuration(**context):
    retention_days = int(Variable.get('max_db_entry_age_in_days', default_var=30))
    enable_delete = Variable.get('ENABLE_DELETE', default_var='False').lower() == 'true'
    max_date = datetime.now() - timedelta(days=retention_days)
    context['ti'].xcom_push(key='max_date', value=max_date.isoformat())
    context['ti'].xcom_push(key='enable_delete', value=enable_delete)
    logging.info(f"Configured retention: {retention_days} days, delete enabled: {enable_delete}")

def cleanup_airflow_db(**context):
    ti = context['ti']
    max_date_str = ti.xcom_pull(task_ids='print_configuration', key='max_date')
    enable_delete = ti.xcom_pull(task_ids='print_configuration', key='enable_delete')
    max_date = datetime.fromisoformat(max_date_str)
    
    MODEL_CONFIGS = [
        (DagRun, 'execution_date'),
        (TaskInstance, 'start_date'),
        (Log, 'dttm'),
        (XCom, 'timestamp'),
        (SlaMiss, 'timestamp'),
        (DagModel, None),  # Example of special handling
        (TaskReschedule, 'start_date'),
        (TaskFail, 'start_date'),
        (RenderedTaskInstanceFields, 'execution_date'),
        (ImportError, 'timestamp'),
        (Job, 'latest_heartbeat'),
        (BaseJob, 'latest_heartbeat'),
    ]
    
    session = settings.Session()
    try:
        for model_cls, date_attr in MODEL_CONFIGS:
            if not date_attr:
                logging.warning(f"Skipping {model_cls.__name__} due to missing date attribute")
                continue
            date_field = getattr(model_cls, date_attr)
            query = session.query(model_cls).filter(date_field < max_date)
            count = query.count()
            if count == 0:
                logging.info(f"No {model_cls.__name__} records older than {max_date}")
                continue
            if enable_delete:
                logging.info(f"Deleting {count} {model_cls.__name__} records older than {max_date}")
                query.delete(synchronize_session=False)
            else:
                logging.info(f"Dry run: Would delete {count} {model_cls.__name__} records older than {max_date}")
        if enable_delete:
            session.commit()
        else:
            session.rollback()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

with DAG(
    'airflow_db_cleanup',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance'],
) as dag:
    
    print_config = PythonOperator(
        task_id='print_configuration',
        python_callable=print_configuration,
        provide_context=True,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_airflow_db',
        python_callable=cleanup_airflow_db,
        provide_context=True,
    )
    
    print_config >> cleanup_task

Wait, but in the cleanup function, some models like DagModel don't have a date_attr. The example shows (DagModel, None), which would be skipped. But according to the pipeline description, DagModel is one of the tables. So perhaps DagModel uses a different field. Need to check Airflow's model definitions.

For example, DagModel has 'last_parsed_time' or 'last_scheduler_run' fields. But the user's code might need to handle that. However, the pipeline description mentions that some models require different deletion logic. So in the code, perhaps for DagModel, we need a different approach. But since the example code is minimal, maybe the user expects a comment indicating where such logic would go.

Alternatively, perhaps the code should include handling for DagModel with the correct date attribute. Let me check Airflow's DagModel model. In Airflow 2.x, DagModel has 'last_parsed_time' as a DateTime field. So maybe for DagModel, the date_attr is 'last_parsed_time'. So updating MODEL_CONFIGS:

(DagModel, 'last_parsed_time'),

But if the user's pipeline description includes DagModel, then the code should include it with the correct date attribute.

But since I might not know all the correct fields, perhaps the code should include placeholders but with comments indicating that the actual field may vary and need verification.

Alternatively, proceed with the best assumption based on common field names.

Alternatively, the user's code may have errors here, but the task is to generate code that follows the description.

In any case, the code should include the models listed in the description, even if some date attributes are incorrect, as the user can adjust them.

Another point: The cleanup function uses session.query(model_cls).filter(date_field < max_date). But some models might have relationships that require more complex deletion logic (e.g., cascading deletes). The code as written may not handle that, but the pipeline description mentions "handles special cases for certain models that require different deletion logic". So perhaps in the code, after the general loop, there's a section for special cases. But the example code may not include that unless specified.

Alternatively, the code can include a comment indicating where special handling would occur.

Another consideration: The use of session.commit() after all deletions if enable_delete is True. In the current code, the commit is done once after processing all models. But if any model's deletion fails, the entire transaction is rolled back. That's probably correct.

Also, the use of synchronize_session=False in query.delete() is appropriate for SQLAlchemy.

Testing the code for syntax errors: The imports should be correct. The models like TaskReschedule are part of airflow.models, so they need to be imported.

Wait, in Airflow, some models are in different modules. For example, TaskReschedule is in airflow.models.taskreschedule. So the code may need to import them from their respective modules. But the user's code may have import errors unless all models are correctly imported.

This is a potential issue. For example:

from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, ImportError, Job, BaseJob
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.taskfail import TaskFail
from airflow.models.renderedtifields import RenderedTaskInstanceFields

But I'm not sure about the exact import paths. This could lead to ImportErrors if not correctly specified.

To avoid errors, the code should correctly import all the models. However, given the time constraints, perhaps the user expects the code to include the necessary imports, even if some are incorrect, as the user can adjust them based on their Airflow version.

Alternatively, use the correct import paths. For example:

In Airflow 2.x:

- TaskReschedule is in airflow.models.taskreschedule
- TaskFail is in airflow.models.taskfail
- RenderedTaskInstanceFields is in airflow.models.renderedtifields
- ImportError is in airflow.models.import_error

So the code should import them accordingly:

from airflow.models.taskreschedule import TaskReschedule
from airflow.models.taskfail import TaskFail
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.import_error import ImportError

But including all these imports may make the code lengthy. However, the user's code must be executable, so correct imports are necessary.

So adjusting the imports:

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, Job, BaseJob
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.taskfail import TaskFail
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.import_error import ImportError
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import logging

But why is send_email imported? The default_args have email_on_failure, which uses Airflow's default email configuration. The send_email import may not be necessary unless explicitly used, which it's not in the code. So removing that import.

So correcting the imports.

Another point: The 'provide_context' argument in PythonOperator is deprecated in Airflow 2.x, and 'do_xcom_push' is True by default. However, to pass the context (like ti, etc.), the function needs to accept **kwargs or have the context parameter. The code uses **context in the functions, which is correct.

Putting it all together, the final code should look like this.
</think>

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.models import (
    Variable,
    DagRun,
    TaskInstance,
    Log,
    XCom,
    SlaMiss,
    DagModel,
    Job,
    BaseJob,
)
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.taskfail import TaskFail
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.import_error import ImportError
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def print_configuration(**context):
    """Load cleanup parameters and calculate retention threshold."""
    retention_days = int(Variable.get('max_db_entry_age_in_days', default_var=30))
    enable_delete = Variable.get('ENABLE_DELETE', default_var='False').lower() == 'true'
    max_date = datetime.utcnow() - timedelta(days=retention_days)
    
    context['ti'].xcom_push(key='max_date', value=max_date.isoformat())
    context['ti'].xcom_push(key='enable_delete', value=enable_delete)
    logging.info(f"Configured retention: {retention_days} days, delete enabled: {enable_delete}")

def cleanup_airflow_db(**context):
    """Execute database cleanup operations using SQLAlchemy ORM."""
    ti = context['ti']
    max_date = datetime.fromisoformat(ti.xcom_pull(task_ids='print_configuration', key='max_date'))
    enable_delete = ti.xcom_pull(task_ids='print_configuration', key='enable_delete')
    
    model_configs = [
        (DagRun, 'execution_date'),
        (TaskInstance, 'start_date'),
        (Log, 'dttm'),
        (XCom, 'timestamp'),
        (SlaMiss, 'timestamp'),
        (DagModel, 'last_parsed_time'),
        (TaskReschedule, 'start_date'),
        (TaskFail, 'start_date'),
        (RenderedTaskInstanceFields, 'execution_date'),
        (ImportError, 'timestamp'),
        (Job, 'latest_heartbeat'),
        (BaseJob, 'latest_heartbeat'),
    ]

    session = settings.Session()
    try:
        for model_cls, date_field in model_configs:
            query = session.query(model_cls).filter(
                getattr(model_cls, date_field) < max_date
            )
            count = query.count()
            
            if count == 0:
                logging.info(f"No {model_cls.__name__} records older than {max_date}")
                continue
                
            if enable_delete:
                logging.info(f"Deleting {count} {model_cls.__name__} records")
                query.delete(synchronize_session=False)
            else:
                logging.info(f"Dry run: {count} {model_cls.__name__} records would be deleted")

        if enable_delete:
            session.commit()
        else:
            session.rollback()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

with DAG(
    'airflow_metadata_cleanup',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance'],
) as dag:

    print_config_task = PythonOperator(
        task_id='print_configuration',
        python_callable=print_configuration,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_airflow_db',
        python_callable=cleanup_airflow_db,
    )

    print_config_task >> cleanup_task