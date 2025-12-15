from dagster import (
    op,
    job,
    ResourceDefinition,
    fs_io_manager,
    InProcessExecutor,
)


@op(
    name="validate_color_parameter",
    description="Validate Color Parameter",
)
def validate_color_parameter(context):
    """Op: Validate Color Parameter"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="download_mondo_terms",
    description="Download Mondo OBO File",
)
def download_mondo_terms(context):
    """Op: Download Mondo OBO File"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="normalize_mondo_terms",
    description="Normalize Mondo Terms",
)
def normalize_mondo_terms(context):
    """Op: Normalize Mondo Terms"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="index_mondo_terms",
    description="Index Mondo Terms",
)
def index_mondo_terms(context):
    """Op: Index Mondo Terms"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="publish_mondo_data",
    description="Publish Mondo Dataset",
)
def publish_mondo_data(context):
    """Op: Publish Mondo Dataset"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="send_slack_notification",
    description="Send Slack Notification",
)
def send_slack_notification(context):
    """Op: Send Slack Notification"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="etl_import_mondo",
    description="Comprehensive Pipeline Description",
    executor_def=InProcessExecutor(),
    resource_defs={
        "io_manager": fs_io_manager,
        "s3_conn_id": ResourceDefinition.hardcoded_resource("s3_conn_id_placeholder"),
        "github_api": ResourceDefinition.hardcoded_resource("github_api_placeholder"),
        "slack_webhook": ResourceDefinition.hardcoded_resource("slack_webhook_placeholder"),
        "es_url": ResourceDefinition.hardcoded_resource("es_url_placeholder"),
    },
)
def etl_import_mondo():
    (
        validate_color_parameter()
        >> download_mondo_terms()
        >> normalize_mondo_terms()
        >> index_mondo_terms()
        >> publish_mondo_data()
        >> send_slack_notification()
    )