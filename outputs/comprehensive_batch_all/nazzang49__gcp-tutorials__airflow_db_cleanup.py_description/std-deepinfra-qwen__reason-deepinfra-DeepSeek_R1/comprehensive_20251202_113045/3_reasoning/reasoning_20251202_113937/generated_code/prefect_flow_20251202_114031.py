<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

The original pipeline has two sequential tasks: print_configuration and cleanup_airflow_db. They run in order, no parallelism. The schedule is daily at midnight UTC, but Prefect handles scheduling through deployments, so I'll add a comment about that.

First, I'll create a Prefect flow with two tasks. Since they are sequential, I'll call them normally without .submit(). Each task will be a @task-decorated function.

For print_configuration: It loads config, calculates max_date, and pushes to XCom. But in Prefect, tasks can return values which are automatically passed to downstream tasks. So instead of XCom, the first task can return max_date, and the second task takes it as an argument.

The cleanup_airflow_db task uses SQLAlchemy and Airflow models. I need to make sure those dependencies are imported. But the code should assume required packages are installed. So I'll include necessary imports like sqlalchemy and airflow models, but note that the user must have them installed.

Wait, the user might not have Airflow installed. The problem mentions Airflow's internal database, so maybe this flow is intended to run within an Airflow environment? Or perhaps it's a separate script that connects to Airflow's database. Hmm, maybe the code should use SQLAlchemy to connect directly to the database without relying on Airflow's ORM? But the original steps mention using Airflow Core Models, so perhaps the tasks are using Airflow's ORM. That complicates things because Airflow isn't typically a dependency for Prefect flows. But the problem says to assume required packages are installed, so proceed.

So, in the cleanup task, I'll need to import the necessary Airflow models like DagRun, TaskInstance, etc. Also, handle session management. Maybe use a session from Airflow's settings, but that might require Airflow's configuration. Alternatively, create a session using SQLAlchemy. But the original task uses Airflow's ORM, so perhaps the code should get a session from Airflow's scoped session. But how?

Alternatively, maybe the code uses SQLAlchemy directly to connect to the Airflow metadata database. But the problem states that the pipeline is for Airflow's internal database, so perhaps the code is part of an Airflow setup. However, Prefect and Airflow are separate, so this might be a script that connects to the same database Airflow uses.

This is getting a bit complicated. The user might expect that the Prefect flow interacts with Airflow's database using SQLAlchemy, perhaps by having the correct connection string. But the original steps mention using Airflow Variables, which are part of Airflow's own variables. So maybe the code is intended to be run within an Airflow environment, but that's conflicting with Prefect. Hmm, perhaps the problem is a translation from Airflow DAGs to Prefect flows, so the Airflow-specific parts need to be adapted.

Wait, the problem says "Convert the following pipeline description into a Prefect 2 Flow Python module." The original pipeline is an Airflow DAG, but now it's being converted to Prefect. So the Airflow-specific parts like PythonOperator and XCom should be replaced with Prefect tasks and parameters.

So, in Prefect, the print_configuration task would load configuration variables, perhaps from Prefect's own configuration or environment variables, and return the max_date. Then the cleanup task would take that max_date as an argument.

For the configuration, the original step uses Airflow Variables, but in Prefect, maybe we can use Prefect's configuration or pass parameters to the flow. So the flow could accept parameters like max_db_entry_age_in_days and enable_delete, with defaults.

The cleanup task would then connect to the database using SQLAlchemy, iterate over the models, and perform deletions. So I'll need to set up SQLAlchemy sessions.

So, putting it all together:

- The flow function will take parameters for max_db_entry_age_in_days and enable_delete.
- The print_configuration task processes these parameters, calculates max_date, and returns it.
- The cleanup_airflow_db task takes max_date and enable_delete as arguments, connects to the database, and performs deletions.

Now, handling the database models. The code needs to define a list of models and their configurations. Each model might have different query conditions. For example, some models have 'execution_date' while others have 'start_date' or 'timestamp'.

So, in the cleanup task, there's a predefined configuration of models with their date fields and optional conditions. The code loops through each model, constructs the query, and executes it.

Special cases are mentioned, like different deletion logic for certain models. For example, maybe some models require a join or a different filter. The code should handle that with conditional checks.

But to keep it minimal, perhaps the code defines a list of model_configs, each with the model class, date_field, and any additional filters.

The code uses SQLAlchemy's ORM to create a session, then for each model_config, it creates a query filtering where the date_field is less than max_date. If enable_delete is True, it deletes those records; otherwise, it just logs the count.

Handling special cases: For example, DagModel might need to check for dags that are not examples. So in the model_config, there's an extra filter condition.

Now, the imports: Need to import the necessary Airflow models. But if the user is expected to have Airflow installed, then the code can import from airflow.models. However, this might not be the case. Wait, the problem says "Integrations & Dependencies" include Airflow's Metadata Database and Core Models. So the code assumes that Airflow is installed, and the models are accessible.

But Prefect and Airflow are separate, so this code would need to be in an environment where Airflow is installed. The user is responsible for that. So proceed with importing the models.

Putting it all together:

The code starts with imports: prefect.flow, prefect.task, datetime, timedelta, sqlalchemy, and the Airflow models.

Then, the print_configuration task takes max_db_entry_age_in_days and enable_delete as parameters (with defaults), calculates max_date, and returns a dictionary with max_date and enable_delete.

The cleanup_airflow_db task takes that config, creates a session, iterates over model_configs, builds queries, and performs deletions.

But wait, in Prefect, tasks can't return multiple values directly, but they can return a tuple or a dictionary. So the print_configuration task returns a config dict containing max_date and enable_delete, which is then passed to cleanup_airflow_db.

The model_configs would be a list defined within the cleanup task, specifying each model, date_field, and any additional filters.

Handling the session: Using SQLAlchemy's sessionmaker, perhaps connecting to the Airflow database. But the connection details (like SQLALCHEMY_CONN) would need to be configured. Since the problem says to assume required packages are installed, perhaps the code uses a default connection, but in reality, the user would need to set that up. For the sake of the example, the code can get the session from the Airflow settings, but that's part of Airflow's internal setup. Alternatively, create a session using a connection string.

But the original pipeline is part of Airflow, so perhaps the code here is intended to be run within Airflow's context. However, Prefect is separate. This is a bit conflicting. Maybe the code should use a SQLAlchemy engine created from the Airflow database connection. But without specific connection details, perhaps the code will have a placeholder, like using an environment variable for the database URL.

But the problem says to avoid placeholders. So perhaps the code includes a comment indicating that the SQLALCHEMY_CONN should be configured, but in the code, it's left as a placeholder. However, the user instruction says to avoid placeholders. Hmm.

Alternatively, the code can get the connection from Airflow's settings, assuming that Airflow is configured. For example:

from airflow.settings import Session
But again, this requires Airflow to be properly initialized, which might not be the case in a Prefect flow. This is a potential issue. However, the problem states that the code must be executable assuming required packages are installed. So perhaps the code includes the necessary imports and uses a session from Airflow, but in reality, this would require Airflow to be set up correctly.

Alternatively, perhaps the code uses a SQLAlchemy engine directly. For example:

engine = create_engine('postgresql://user:pass@host/dbname')

But the connection details are not provided. Since the problem says to add minimal safe defaults, maybe the code uses a default SQLite connection, but that's unlikely. Alternatively, the code can raise an error if the connection isn't set up, but the problem says to avoid placeholders.

This is a tricky part. The original pipeline is for Airflow's metadata database, so the connection details would typically be handled by Airflow's configuration. But in a Prefect flow, we need to connect to that database independently. So the code should include the necessary setup for SQLAlchemy to connect, perhaps using a connection string from an environment variable or Prefect configuration.

But given the problem constraints, perhaps the code can include a comment indicating that the SQLALCHEMY_CONN should be configured, and in the code, use a default that may not work but is a starting point. However, the user instruction says to avoid placeholders. So maybe the code uses a default SQLite connection for illustration, but that's not realistic. Alternatively, the code can have a function to get the engine, which the user must implement. But the problem says the code must be executable, so perhaps the code includes a minimal way to create the engine, even if it's not functional without setup.

Alternatively, perhaps the code uses a session from Airflow, assuming that Airflow is installed and configured. For example:

from airflow.settings import Session

But again, this requires Airflow to be initialized, which might not be the case. This is a problem because Prefect and Airflow are separate systems.

This seems like a potential contradiction in the problem statement. The pipeline is meant to clean Airflow's database, which implies that the code needs access to it. The original Airflow DAG would have access, but a Prefect flow would need to connect externally. Therefore, the code should include SQLAlchemy setup to connect to the Airflow database, perhaps via a connection string stored in an environment variable.

So, in the code, the cleanup task creates an engine using sqlalchemy.create_engine, getting the connection string from an environment variable, say AIRFLOW_DATABASE_URI. But the problem says to avoid placeholders, so maybe the code uses a default that's common, like 'sqlite:///airflow.db', but that's just an example.

But the user might need to adjust this. However, the problem says to include minimal safe defaults. So perhaps the code uses a default SQLite connection but with a comment indicating that it should be configured.

In any case, proceeding with the code:

The cleanup_airflow_db task would create a session using SQLAlchemy, connect to the database, loop through the model configurations, build the queries, and delete or count the records based on enable_delete.

Now, handling the model configurations. The code defines a list of model_configs, each with the model class, date_field, and optional conditions. For example:

model_configs = [
    {'model': DagRun, 'date_field': DagRun.execution_date},
    {'model': TaskInstance, 'date_field': TaskInstance.execution_date},
    # ... other models
    {'model': DagModel, 'date_field': DagModel.last_scheduler_run, 'extra_conditions': [DagModel.is_subdag == False, DagModel.fileloc.notlike('%example_dags%')]},
    # ... etc.
]

Then, for each config, the code constructs a query with filters.

But in SQLAlchemy, the date_field is a column attribute. So when building the query, it's something like:

query = session.query(config['model']).filter(config['date_field'] < max_date)

If there are extra_conditions, add them to the filter.

Once the query is built, if enable_delete is True, delete the records in batches (to avoid locking the table), else log the count.

The code should handle batch deletions, perhaps using a while loop that deletes chunks of records until none are left.

But to keep it simple, maybe the code just deletes all matching records in one go, but that's not efficient for large tables. However, the problem says to add minimal code, so perhaps the code uses a single delete operation.

But the original step mentions "processes each database table with appropriate filters and grouping logic", which might imply batching. However, without specific details, perhaps the code uses a simple delete.

Putting it all together:

The cleanup_airflow_db task would loop through each model_config, build the query, and execute delete or select count(*).

Now, handling the different date fields and conditions. For example, some models might have a different date field name. The model_config entries should specify the correct field.

Special cases, like DagModel requiring additional filters, are handled by adding extra_conditions to the query.

Now, the code:

Import necessary modules:

from prefect import flow, task
from datetime import datetime, timedelta
from typing import Dict, Any
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError, Job, BaseJob

Then, define the tasks.

The print_configuration task:

@task
def print_configuration(max_db_entry_age_in_days: int = 30, enable_delete: bool = False) -> Dict[str, Any]:
    # Calculate max_date based on retention period
    max_date = datetime.now() - timedelta(days=max_db_entry_age_in_days)
    config = {
        'max_date': max_date,
        'enable_delete': enable_delete
    }
    print(f"Configuration loaded: Max age {max_db_entry_age_in_days} days, Enable delete: {enable_delete}")
    return config

The cleanup_airflow_db task:

@task
def cleanup_airflow_db(config: Dict[str, Any]):
    max_date = config['max_date']
    enable_delete = config['enable_delete']
    
    # Database connection setup
    # Assuming AIRFLOW_DATABASE_URI is set in environment
    engine = create_engine('postgresql://user:pass@localhost/airflow')  # Example, should be configured
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Model configurations with date fields and filters
    model_configs = [
        {'model': DagRun, 'date_field': DagRun.execution_date},
        {'model': TaskInstance, 'date_field': TaskInstance.execution_date},
        {'model': Log, 'date_field': Log.dttm},
        {'model': XCom, 'date_field': XCom.timestamp},
        {'model': SlaMiss, 'date_field': SlaMiss.timestamp},
        {'model': TaskReschedule, 'date_field': TaskReschedule.start_date},
        {'model': TaskFail, 'date_field': TaskFail.start_date},
        {'model': RenderedTaskInstanceFields, 'date_field': RenderedTaskInstanceFields.execution_date},
        {'model': ImportError, 'date_field': ImportError.timestamp},
        {'model': Job, 'date_field': Job.latest_heartbeat},
        {'model': BaseJob, 'date_field': BaseJob.latest_heartbeat},
        {
            'model': DagModel,
            'date_field': DagModel.last_scheduler_run,
            'extra_conditions': [
                DagModel.is_subdag == False,
                DagModel.fileloc.notlike('%example_dags%')
            ]
        }
    ]
    
    for config in model_configs:
        model = config['model']
        date_field = config['date_field']
        extra_conditions = config.get('extra_conditions', [])
        
        # Build base query
        query = session.query(model).filter(date_field < max_date)
        
        # Add extra conditions if present
        if extra_conditions:
            query = query.filter(and_(*extra_conditions))
        
        if enable_delete:
            deleted_count = query.delete(synchronize_session=False)
            print(f"Deleted {deleted_count} records from {model.__name__}")
        else:
            record_count = query.count()
            print(f"Found {record_count} records to delete in {model.__name__}")
        
        session.commit()
    
    session.close()

Then, the flow:

@flow
def airflow_db_cleanup_flow(max_db_entry_age_in_days: int = 30, enable_delete: bool = False):
    config = print_configuration(max_db_entry_age_in_days, enable_delete)
    cleanup_airflow_db(config)

if __name__ == '__main__':
    # Example: airflow_db_cleanup_flow(max_db_entry_age_in_days=30, enable_delete=False)
    # To configure scheduling, use Prefect deployments with cron schedule
    airflow_db_cleanup_flow()

Wait, but the database connection in the cleanup task uses a hardcoded example URL. That's not ideal. The problem says to avoid placeholders, so perhaps the code should get the connection string from an environment variable. For example:

import os
DATABASE_URI = os.getenv('AIRFLOW_DATABASE_URI', 'sqlite:///airflow.db')
engine = create_engine(DATABASE_URI)

But the problem says to assume required packages are installed, so including this would make the code more flexible. So modifying the cleanup task:

@task
def cleanup_airflow_db(config: Dict[str, Any]):
    max_date = config['max_date']
    enable_delete = config['enable_delete']
    
    # Get database connection from environment variable
    import os
    database_uri = os.getenv('AIRFLOW_DATABASE_URI', 'sqlite:///airflow.db')
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # ... rest of the code ...

This way, the user can set AIRFLOW_DATABASE_URI in their environment, and the code uses that. The default is sqlite, which might not be correct but allows the code to run.

Also, note that some models like Job and BaseJob might be version-specific. The code includes them, but if there are API changes, the user would need to adjust the imports. But the problem mentions version-specific logic, which is handled via comments or code structure. However, the code as written includes all the models listed.

Now, testing if the code is PEP 8 compliant. Lines not too long, proper spacing, etc.

The email alerts and retries are part of the original orchestration, but in Prefect, those are configured via flow decorator parameters or Prefect settings. However, the problem says to include a comment about deployment/schedule configuration. So in the main guard, add a comment about using Prefect deployments for scheduling.

Putting it all together, the final code would look like this.
</think>

from prefect import flow, task
from datetime import datetime, timedelta
from typing import Dict, Any
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
import os

try:
    from airflow.models import (
        DagRun,
        TaskInstance,
        Log,
        XCom,
        SlaMiss,
        DagModel,
        TaskReschedule,
        TaskFail,
        RenderedTaskInstanceFields,
        ImportError,
        Job,
        BaseJob,
    )
except ImportError:
    pass  # Handle Airflow version differences in deployment


@task
def print_configuration(
    max_db_entry_age_in_days: int = 30, enable_delete: bool = False
) -> Dict[str, Any]:
    """Load cleanup parameters and calculate date threshold."""
    max_date = datetime.now() - timedelta(days=max_db_entry_age_in_days)
    config = {"max_date": max_date, "enable_delete": enable_delete}
    print(f"Cleanup threshold date: {max_date.isoformat()}")
    return config


@task
def cleanup_airflow_db(config: Dict[str, Any]):
    """Execute database cleanup operations for Airflow metadata tables."""
    max_date = config["max_date"]
    enable_delete = config["enable_delete"]

    database_uri = os.getenv("AIRFLOW_DATABASE_URI", "sqlite:///airflow.db")
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    model_configs = [
        {"model": DagRun, "date_field": DagRun.execution_date},
        {"model": TaskInstance, "date_field": TaskInstance.execution_date},
        {"model": Log, "date_field": Log.dttm},
        {"model": XCom, "date_field": XCom.timestamp},
        {"model": SlaMiss, "date_field": SlaMiss.timestamp},
        {"model": TaskReschedule, "date_field": TaskReschedule.start_date},
        {"model": TaskFail, "date_field": TaskFail.start_date},
        {
            "model": RenderedTaskInstanceFields,
            "date_field": RenderedTaskInstanceFields.execution_date,
        },
        {"model": ImportError, "date_field": ImportError.timestamp},
        {"model": Job, "date_field": Job.latest_heartbeat},
        {"model": BaseJob, "date_field": BaseJob.latest_heartbeat},
        {
            "model": DagModel,
            "date_field": DagModel.last_scheduler_run,
            "extra_conditions": [
                DagModel.is_subdag == False,
                DagModel.fileloc.notlike("%example_dags%"),
            ],
        },
    ]

    for table_config in model_configs:
        model = table_config["model"]
        date_field = table_config["date_field"]
        extra_conditions = table_config.get("extra_conditions", [])

        try:
            query = session.query(model).filter(date_field < max_date)
            if extra_conditions:
                query = query.filter(and_(*extra_conditions))

            if enable_delete:
                deleted_count = query.delete(synchronize_session=False)
                action = f"Deleted {deleted_count}"
            else:
                record_count = query.count()
                action = f"Would delete {record_count}"

            print(f"{action} records from {model.__tablename__}")
            session.commit()
        except Exception as e:
            print(f"Error processing {model.__name__}: {str(e)}")
            session.rollback()

    session.close()


@flow
def airflow_db_cleanup_flow(
    max_db_entry_age_in_days: int = 30, enable_delete: bool = False
):
    """Orchestrate Airflow metadata database maintenance workflow."""
    config = print_configuration(max_db_entry_age_in_days, enable_delete)
    cleanup_airflow_db(config)


if __name__ == "__main__":
    # To configure daily execution: prefect deployment create with cron schedule
    airflow_db_cleanup_flow()