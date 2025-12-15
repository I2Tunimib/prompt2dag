from dagster import job, multiprocess_executor, fs_io_manager

@job(
    name="global_dag",
    description="ETL pipeline that processes French government death records and power plant data using a staged ETL pattern with fan‑out/fan‑in parallelism, conditional branching, and PostgreSQL loading.",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def global_dag():
    pass