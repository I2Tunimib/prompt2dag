from dagster import op, job, ResourceDefinition, fs_io_manager
from dagster_k8s import k8s_job_executor


@op(
    name='validate_params',
    description='Validate Pipeline Parameters',
)
def validate_params(context):
    """Op: Validate Pipeline Parameters"""
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
    description='Index Mondo Terms into Elasticsearch',
)
def index_mondo_terms(context):
    """Op: Index Mondo Terms into Elasticsearch"""
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
    name='notify_slack',
    description='Slack Notification',
)
def notify_slack(context):
    """Op: Slack Notification"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='etl_import_mondo',
    description='Comprehensive Pipeline Description',
    executor_def=k8s_job_executor,
    resource_defs={
        "s3_conn_id": ResourceDefinition.hardcoded_resource("s3_conn_id"),
        "es_conn_id": ResourceDefinition.hardcoded_resource("es_conn_id"),
        "slack_conn_id": ResourceDefinition.hardcoded_resource("slack_conn_id"),
        "io_manager": fs_io_manager,
    },
)
def etl_import_mondo():
    # Sequential execution respecting dependencies
    validated = validate_params()
    downloaded = download_mondo_terms(validated)
    normalized = normalize_mondo_terms(downloaded)
    indexed = index_mondo_terms(normalized)
    published = publish_mondo(indexed)
    notify_slack(published)