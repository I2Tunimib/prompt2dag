from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='validate_color_parameter', retries=0)
def validate_color_parameter():
    """Task: Validate Color Parameter"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='download_mondo_terms', retries=0)
def download_mondo_terms():
    """Task: Download Mondo OBO File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='normalize_mondo_terms', retries=0)
def normalize_mondo_terms():
    """Task: Normalize Mondo Terms"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='index_mondo_terms', retries=0)
def index_mondo_terms():
    """Task: Index Mondo Terms"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='publish_mondo_data', retries=0)
def publish_mondo_data():
    """Task: Publish Mondo Dataset"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='send_slack_notification', retries=0)
def send_slack_notification():
    """Task: Send Slack Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name='etl_import_mondo', task_runner=SequentialTaskRunner())
def etl_import_mondo():
    """Sequential ETL pipeline for Mondo data."""
    validate_color_parameter()
    download_mondo_terms()
    normalize_mondo_terms()
    index_mondo_terms()
    publish_mondo_data()
    send_slack_notification()


if __name__ == "__main__":
    etl_import_mondo()