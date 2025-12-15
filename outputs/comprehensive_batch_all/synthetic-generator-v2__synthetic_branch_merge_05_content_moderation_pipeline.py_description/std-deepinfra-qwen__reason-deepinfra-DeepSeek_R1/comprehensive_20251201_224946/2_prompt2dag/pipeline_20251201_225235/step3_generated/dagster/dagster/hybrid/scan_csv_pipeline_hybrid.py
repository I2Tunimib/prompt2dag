from dagster import job, op, In, Out, graph, execute_pipeline, DagsterType, DagsterEvent, DagsterEventType, DagsterInstance, ModeDefinition, ResourceDefinition, fs_io_manager, in_process_executor

# Task Definitions
@op(
    name='scan_csv',
    description='Scan CSV',
)
def scan_csv(context):
    """Op: Scan CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='toxicity_check',
    description='Toxicity Check',
)
def toxicity_check(context):
    """Op: Toxicity Check"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='publish_content',
    description='Publish Content',
)
def publish_content(context):
    """Op: Publish Content"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='remove_and_flag_content',
    description='Remove and Flag Content',
)
def remove_and_flag_content(context):
    """Op: Remove and Flag Content"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='audit_log',
    description='Audit Log',
)
def audit_log(context):
    """Op: Audit Log"""
    # Docker execution
    # Image: python:3.9
    pass

# Job Definition
@job(
    name="scan_csv_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "audit_logging_system": ResourceDefinition.mock_resource(),
        "platform_content_management": ResourceDefinition.mock_resource(),
        "platform_publishing_system": ResourceDefinition.mock_resource(),
        "local_filesystem": ResourceDefinition.mock_resource(),
        "io_manager": fs_io_manager
    },
)
def scan_csv_pipeline():
    scan_csv_output = scan_csv()
    toxicity_check_output = toxicity_check(scan_csv_output)
    remove_and_flag_content_output = remove_and_flag_content(toxicity_check_output)
    publish_content_output = publish_content(toxicity_check_output)
    audit_log(remove_and_flag_content_output, publish_content_output)

# Schedule Definition
from dagster import ScheduleDefinition, schedule

@schedule(
    cron_schedule="@daily",
    job=scan_csv_pipeline,
    execution_timezone="UTC",
)
def daily_scan_csv_pipeline_schedule(context):
    return {}

# Example Execution
if __name__ == "__main__":
    result = execute_pipeline(scan_csv_pipeline, instance=DagsterInstance.get())
    print(result.success)