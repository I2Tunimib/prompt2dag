from dagster import job, op, ResourceDefinition, fs_io_manager, k8s_job_executor

# Task Definitions
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

# Job Definition
@job(
    name="etl_import_mondo",
    description="No description provided.",
    executor_def=k8s_job_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "internal_parameter_validation_system": ResourceDefinition.none_resource(),
        "k8s_context": ResourceDefinition.none_resource(),
        "github_releases": ResourceDefinition.none_resource(),
        "s3_conn_id": ResourceDefinition.none_resource(),
        "internal_publishing_system": ResourceDefinition.none_resource(),
        "es_url": ResourceDefinition.none_resource(),
        "slack_webhook": ResourceDefinition.none_resource(),
    }
)
def etl_import_mondo():
    params_validate_op = params_validate()
    download_mondo_terms_op = download_mondo_terms(params_validate_op)
    normalized_mondo_terms_op = normalized_mondo_terms(download_mondo_terms_op)
    index_mondo_terms_op = index_mondo_terms(normalized_mondo_terms_op)
    publish_mondo_op = publish_mondo(index_mondo_terms_op)
    slack_op = slack(publish_mondo_op)
```
This code defines the `etl_import_mondo` job with the specified ops and their dependencies, ensuring the sequential pattern is maintained. The required resources are also defined.