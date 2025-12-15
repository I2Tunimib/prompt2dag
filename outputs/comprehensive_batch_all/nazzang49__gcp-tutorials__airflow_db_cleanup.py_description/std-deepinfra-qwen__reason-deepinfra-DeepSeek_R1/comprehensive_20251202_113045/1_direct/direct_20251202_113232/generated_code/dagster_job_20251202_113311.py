from dagster import op, job, resource, RetryPolicy, Failure, Field, String, Int, In, Out

# Simplified resource for database session management
@resource(config_schema={"connection_string": Field(String, is_required=True)})
def db_session_resource(context):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(context.resource_config["connection_string"])
    Session = sessionmaker(bind=engine)
    return Session()

# Simplified configuration for Airflow variables
@resource(config_schema={"max_db_entry_age_in_days": Field(Int, default_value=30)})
def airflow_variables_resource(context):
    return context.resource_config

# Op to print and calculate configuration
@op(required_resource_keys={"airflow_variables"}, out={"max_date": Out(dagster_type=str)})
def print_configuration(context):
    from datetime import datetime, timedelta

    max_db_entry_age_in_days = context.resources.airflow_variables["max_db_entry_age_in_days"]
    max_date = (datetime.utcnow() - timedelta(days=max_db_entry_age_in_days)).isoformat()
    context.log.info(f"Max date for data deletion: {max_date}")
    return max_date

# Op to perform the actual database cleanup
@op(required_resource_keys={"db_session"}, ins={"max_date": In(dagster_type=str)})
def cleanup_airflow_db(context, max_date):
    from datetime import datetime
    from sqlalchemy.orm import Session
    from airflow.models import DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError, Job

    session: Session = context.resources.db_session

    models = [
        DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, TaskReschedule, TaskFail, RenderedTaskInstanceFields, ImportError, Job
    ]

    enable_delete = True  # Simplified for example

    for model in models:
        query = session.query(model).filter(model.execution_date <= datetime.fromisoformat(max_date))
        if enable_delete:
            query.delete(synchronize_session=False)
            session.commit()
        else:
            context.log.info(f"Would delete {query.count()} records from {model.__tablename__}")

# Define the job with linear execution pattern
@job(
    resource_defs={
        "db_session": db_session_resource,
        "airflow_variables": airflow_variables_resource,
    },
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def airflow_db_cleanup_job():
    max_date = print_configuration()
    cleanup_airflow_db(max_date)

# Launch pattern
if __name__ == "__main__":
    result = airflow_db_cleanup_job.execute_in_process()