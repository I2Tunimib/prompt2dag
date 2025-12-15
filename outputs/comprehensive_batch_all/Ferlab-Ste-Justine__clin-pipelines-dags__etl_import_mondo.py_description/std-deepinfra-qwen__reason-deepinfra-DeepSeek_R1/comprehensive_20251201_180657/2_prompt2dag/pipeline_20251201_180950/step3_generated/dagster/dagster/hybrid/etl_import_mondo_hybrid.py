from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='params_validate',
    description='Validate Parameters',
)
def params_validate(context):
    """Op: Validate Parameters"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_mondo_terms',
    description='Download Mondo Terms',
)
def download_mondo_terms(context):
    """Op: Download Mondo Terms"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalized_mondo_terms',
    description='Normalize Mondo Terms',
)
def normalized_mondo_terms(context):
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
    description='Publish Mondo Data',
)
def publish_mondo(context):
    """Op: Publish Mondo Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='slack',
    description='Send Slack Notification',
)
def slack(context):
    """Op: Send Slack Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="etl_import_mondo",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "es_url": ResourceDefinition.string_resource(),
        "s3_conn_id": ResourceDefinition.string_resource(),
        "internal_validation_system": ResourceDefinition.string_resource(),
        "slack_webhook": ResourceDefinition.string_resource(),
        "github_releases": ResourceDefinition.string_resource(),
    },
)
def etl_import_mondo():
    params_validate_output = params_validate()
    download_mondo_terms_output = download_mondo_terms(params_validate_output)
    normalized_mondo_terms_output = normalized_mondo_terms(download_mondo_terms_output)
    index_mondo_terms_output = index_mondo_terms(normalized_mondo_terms_output)
    publish_mondo_output = publish_mondo(index_mondo_terms_output)
    slack(publish_mondo_output)