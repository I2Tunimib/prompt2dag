from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="validate_params", retries=0)
def validate_params():
    """Task: Validate Pipeline Parameters"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="download_mondo_terms", retries=0)
def download_mondo_terms():
    """Task: Download Mondo OBO File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="normalize_mondo_terms", retries=0)
def normalize_mondo_terms():
    """Task: Normalize Mondo Terms"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="index_mondo_terms", retries=0)
def index_mondo_terms():
    """Task: Index Mondo Terms into Elasticsearch"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="publish_mondo", retries=0)
def publish_mondo():
    """Task: Publish Mondo Dataset"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="notify_slack", retries=0)
def notify_slack():
    """Task: Slack Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="etl_import_mondo", task_runner=SequentialTaskRunner())
def etl_import_mondo():
    # Entry point
    validate_params()
    # Subsequent steps respecting dependencies
    download_mondo_terms()
    normalize_mondo_terms()
    index_mondo_terms()
    publish_mondo()
    notify_slack()


if __name__ == "__main__":
    etl_import_mondo()