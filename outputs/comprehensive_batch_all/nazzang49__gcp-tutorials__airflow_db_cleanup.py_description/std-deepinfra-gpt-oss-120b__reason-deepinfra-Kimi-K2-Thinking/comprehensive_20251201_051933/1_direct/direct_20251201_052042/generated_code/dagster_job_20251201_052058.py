from datetime import datetime, timedelta

from dagster import In, Out, OpExecutionContext, job, op, resource
from sqlalchemy import create_engine, text


@resource
def db_engine_resource(_):
    """
    Minimal SQLAlchemy engine resource.
    Uses an in‑memory SQLite database for demonstration purposes.
    Replace with a real Airflow metadata DB engine in production.
    """
    return create_engine("sqlite:///:memory:")


@op(
    out=Out(datetime),
    config_schema={"retention_days": int},
    description="Calculate the maximum date threshold for cleanup based on retention period.",
)
def print_configuration(context: OpExecutionContext) -> datetime:
    retention_days = context.op_config.get("retention_days", 30)
    max_date = datetime.utcnow() - timedelta(days=retention_days)
    context.log.info(f"Calculated max_date (UTC): {max_date.isoformat()}")
    return max_date


@op(
    ins={"max_date": In(datetime)},
    config_schema={"enable_delete": bool},
    required_resource_keys={"db_engine"},
    description="Delete records older than the supplied max_date from Airflow metadata tables.",
)
def cleanup_airflow_db(context: OpExecutionContext, max_date: datetime) -> str:
    engine = context.resources.db_engine
    tables = [
        "dag_run",
        "task_instance",
        "log",
        "xcom",
        "sla_miss",
        "dag_model",
    ]
    enable_delete = context.op_config.get("enable_delete", False)

    with engine.connect() as conn:
        for table in tables:
            delete_sql = f"DELETE FROM {table} WHERE execution_date < :max_date"
            context.log.info(
                f"{'Executing' if enable_delete else 'Skipping'} deletion for table "
                f"`{table}` with max_date={max_date.isoformat()}"
            )
            if enable_delete:
                conn.execute(text(delete_sql), {"max_date": max_date})

    return "cleanup_complete"


@job(resource_defs={"db_engine": db_engine_resource})
def airflow_cleanup_job():
    """
    Linear Dagster job that mirrors the Airflow maintenance workflow.
    """
    max_date = print_configuration()
    cleanup_airflow_db(max_date)


if __name__ == "__main__":
    result = airflow_cleanup_job.execute_in_process(
        run_config={
            "ops": {
                "print_configuration": {"config": {"retention_days": 30}},
                "cleanup_airflow_db": {"config": {"enable_delete": False}},
            }
        }
    )
    if not result.success:
        raise RuntimeError("Dagster job execution failed.")