from dagster import job, op, daily_schedule, multiprocess_executor, fs_io_manager

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
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def unnamed_pipeline():
    start_op()
    end_op()

# Schedule definition
@daily_schedule(
    pipeline_name="unnamed_pipeline",
    start_date='2023-01-01',
    execution_time_of_day="00:00:00",
    execution_timezone="UTC",
)
def daily_unnamed_pipeline_schedule(date):
    return {}

# Enable the schedule
daily_unnamed_pipeline_schedule.create_schedule_definition()