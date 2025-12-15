from dagster import job, op, in_process_executor, fs_io_manager

# Pre-generated op definitions
@op
def start_op(context):
    context.log.info("Starting the pipeline")

@op
def end_op(context):
    context.log.info("Ending the pipeline")

# Job definition
@job(
    name="unnamed_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def unnamed_pipeline():
    start_op()
    end_op()