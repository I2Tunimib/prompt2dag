from dagster import (
    op,
    job,
    in_process_executor,
    fs_io_manager,
    ResourceDefinition,
    schedule,
)


# Pre-Generated Task Definitions
@op(
    name="create_analytics_table",
    description="Create Analytics Table via CTAS",
)
def create_analytics_table(context):
    """Op: Create Analytics Table via CTAS"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="slack_failure_notifier",
    description="Slack Failure Notifier",
)
def slack_failure_notifier(context):
    """Op: Slack Failure Notifier"""
    # Docker execution
    # Image: python:3.9
    pass


# Resources (placeholders)
slack_conn = ResourceDefinition.hardcoded_resource(None)
snowflake_conn = ResourceDefinition.hardcoded_resource(None)


# Job Definition
@job(
    name="runelt_alert",
    description="Comprehensive Pipeline Description",
    executor_def=in_process_executor,
    resource_defs={
        "slack_conn": slack_conn,
        "snowflake_conn": snowflake_conn,
        "io_manager": fs_io_manager,
    },
)
def runelt_alert():
    # Sequential wiring: create_analytics_table -> slack_failure_notifier
    create_analytics_table() >> slack_failure_notifier()


# Schedule (daily)
@schedule(
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    job=runelt_alert,
    description="Daily schedule for RunELT_Alert",
)
def runelt_alert_schedule():
    return {}