from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition


@op(
    name='run_ctas',
    description='Create Analytics Table via CTAS',
)
def run_ctas(context):
    """Op: Create Analytics Table via CTAS"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='slack_failure_alert',
    description='Slack Failure Notification',
)
def slack_failure_alert(context):
    """Op: Slack Failure Notification"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="run_ctas_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "snowflake_conn": ResourceDefinition.hardcoded_resource(None),
        "slack_conn": ResourceDefinition.hardcoded_resource(None),
    },
)
def run_ctas_pipeline():
    ctas = run_ctas()
    slack_failure_alert().after(ctas)