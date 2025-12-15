from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='run_ctas',
    description='Run CTAS',
)
def run_ctas(context):
    """Op: Run CTAS"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_slack_notification',
    description='Send Slack Notification',
)
def send_slack_notification(context):
    """Op: Send Slack Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="run_ctas_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager, "snowflake_conn": ResourceDefinition.hardcoded_resource(None)},
)
def run_ctas_pipeline():
    send_slack_notification(start=run_ctas())
```
```python
from dagster import ScheduleDefinition, DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")

run_ctas_pipeline_schedule = ScheduleDefinition(
    job=run_ctas_pipeline,
    cron_schedule=daily_partitions.get_cron_schedule(),
)