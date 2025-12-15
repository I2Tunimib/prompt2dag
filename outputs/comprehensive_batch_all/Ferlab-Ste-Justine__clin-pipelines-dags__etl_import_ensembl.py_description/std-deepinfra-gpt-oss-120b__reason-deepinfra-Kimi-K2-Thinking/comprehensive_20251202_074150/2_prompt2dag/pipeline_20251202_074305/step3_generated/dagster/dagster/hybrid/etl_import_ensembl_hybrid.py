from dagster import op, job, In, Out, Nothing, ResourceDefinition

# ----------------------------------------------------------------------
# Task Definitions (provided exactly as given)
# ----------------------------------------------------------------------


@op(
    name='notify_slack_start',
    description='Slack Notification on DAG Start',
)
def notify_slack_start(context):
    """Op: Slack Notification on DAG Start"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='check_and_download_ensembl_files',
    description='Check and Download Ensembl Mapping Files',
)
def check_and_download_ensembl_files(context):
    """Op: Check and Download Ensembl Mapping Files"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='process_ensembl_mapping_spark',
    description='Process Ensembl Mapping with Spark',
)
def process_ensembl_mapping_spark(context):
    """Op: Process Ensembl Mapping with Spark"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='notify_slack_success',
    description='Slack Notification on DAG Completion',
)
def notify_slack_success(context):
    """Op: Slack Notification on DAG Completion"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='notify_slack_failure',
    description='Slack Notification on Task Failure',
)
def notify_slack_failure(context):
    """Op: Slack Notification on Task Failure"""
    # Docker execution
    # Image: python:3.9
    pass


# ----------------------------------------------------------------------
# Additional Op for Branching (required for fan‑out)
# ----------------------------------------------------------------------


@op(
    name='outcome_branch',
    description='Branch based on previous outcome',
    ins={'input': In(Nothing)},
    out=Out(Nothing),
)
def outcome_branch(context, input):
    """Placeholder branching op – simply passes through."""
    # In a real pipeline this would inspect the upstream result and
    # possibly raise or return a value that downstream ops can react to.
    pass


# ----------------------------------------------------------------------
# Job Definition
# ----------------------------------------------------------------------


@job(
    name='etl_import_ensembl',
    description='Comprehensive Pipeline Description',
    resource_defs={
        "ftp_ensembl_conn": ResourceDefinition.hardcoded_resource(None),
        "slack_conn": ResourceDefinition.hardcoded_resource(None),
        "k8s_etl_context": ResourceDefinition.hardcoded_resource(None),
        "s3_conn": ResourceDefinition.hardcoded_resource(None),
        "io_manager": ResourceDefinition.hardcoded_resource(None),
    },
)
def etl_import_ensembl():
    # Entry point
    start = notify_slack_start()

    # Fan‑out chain
    download = check_and_download_ensembl_files(start)
    processed = process_ensembl_mapping_spark(download)

    # Branching point – fan‑out to success & failure notifications
    branch = outcome_branch(processed)

    # Both notifications depend on the same branch (fan‑out)
    notify_slack_success(branch)
    notify_slack_failure(branch)