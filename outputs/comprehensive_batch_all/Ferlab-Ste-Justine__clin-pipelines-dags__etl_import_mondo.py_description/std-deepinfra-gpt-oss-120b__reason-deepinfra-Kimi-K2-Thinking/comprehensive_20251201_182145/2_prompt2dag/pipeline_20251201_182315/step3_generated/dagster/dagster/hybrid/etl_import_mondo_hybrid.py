from dagster import op, job, InProcessExecutor, fs_io_manager, ResourceDefinition

@op(
    name='validate_params',
    description='Validate Parameters',
)
def validate_params(context):
    """Op: Validate Parameters"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_mondo_terms',
    description='Download Mondo OBO File',
)
def download_mondo_terms(context):
    """Op: Download Mondo OBO File"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_mondo_terms',
    description='Normalize Mondo Terms',
)
def normalize_mondo_terms(context):
    """Op: Normalize Mondo Terms"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='index_mondo_terms',
    description='Index Mondo Terms',
)
def index_mondo_terms(context):
    """Op: Index Mondo Terms"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='publish_mondo',
    description='Publish Mondo Dataset',
)
def publish_mondo(context):
    """Op: Publish Mondo Dataset"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='slack_notification',
    description='Send Slack Notification',
)
def slack_notification(context):
    """Op: Send Slack Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="etl_import_mondo",
    description="Comprehensive Pipeline Description",
    executor_def=InProcessExecutor(),
    resource_defs={
        "es_conn_id": ResourceDefinition.hardcoded_resource(None),
        "s3_conn_id": ResourceDefinition.hardcoded_resource(None),
        "slack_webhook_conn": ResourceDefinition.hardcoded_resource(None),
    },
    default_io_manager=fs_io_manager,
)
def etl_import_mondo():
    validate_params() \
        >> download_mondo_terms() \
        >> normalize_mondo_terms() \
        >> index_mondo_terms() \
        >> publish_mondo() \
        >> slack_notification()