from dagster import job, fs_io_manager, docker_executor

@job(
    name="unnamed_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def unnamed_pipeline():
    pass