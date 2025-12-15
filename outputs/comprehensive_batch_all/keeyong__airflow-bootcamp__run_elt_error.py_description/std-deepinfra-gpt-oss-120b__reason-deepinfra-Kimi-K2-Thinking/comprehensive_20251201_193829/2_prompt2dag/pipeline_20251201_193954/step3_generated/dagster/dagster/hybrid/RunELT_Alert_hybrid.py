from dagster import op, job, ResourceDefinition, fs_io_manager, InProcessExecutor

# Pre-Generated Task Definitions
@op(
    name='notify_failure_slack',
    description='Slack Failure Notification',
)
def notify_failure_slack(context):
    """Op: Slack Failure Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='run_ctas',
    description='Run CTAS to Build Analytics Tables',
)
def run_ctas(context):
    """Op: Run CTAS to Build Analytics Tables"""
    # Docker execution
    # Image: python:3.9
    pass

# Resource placeholders
slack_conn = ResourceDefinition.hardcoded_resource(None)
snowflake_conn = ResourceDefinition.hardcoded_resource(None)

# Job definition
@job(
    name='runelt_alert',
    description='Sequential ELT pipeline that builds analytics tables in Snowflake using CTAS operations, with data validation, atomic swaps, and Slack failure notifications.',
    resource_defs={
        "io_manager": fs_io_manager,
        "slack_conn": slack_conn,
        "snowflake_conn": snowflake_conn,
    },
    executor_def=InProcessExecutor(),
)
def runelt_alert():
    # Entry point ops (no explicit dependencies)
    run_ctas()
    notify_failure_slack()