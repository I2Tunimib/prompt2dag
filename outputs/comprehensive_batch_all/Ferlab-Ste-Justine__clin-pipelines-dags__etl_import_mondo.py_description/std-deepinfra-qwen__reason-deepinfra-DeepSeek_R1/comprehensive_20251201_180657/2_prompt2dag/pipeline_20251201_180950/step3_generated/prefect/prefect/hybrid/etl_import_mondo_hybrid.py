from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

@task(name='params_validate', retries=0)
def params_validate():
    """Task: Validate Parameters"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_mondo_terms', retries=0)
def download_mondo_terms():
    """Task: Download Mondo Terms"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalized_mondo_terms', retries=0)
def normalized_mondo_terms():
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

@task(name='publish_mondo', retries=0)
def publish_mondo():
    """Task: Publish Mondo Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='slack', retries=0)
def slack():
    """Task: Send Slack Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="etl_import_mondo", task_runner=SequentialTaskRunner())
def etl_import_mondo_flow():
    logger = get_run_logger()
    logger.info("Starting ETL Import Mondo Flow")

    params_validate_result = params_validate.submit()
    download_mondo_terms_result = download_mondo_terms.submit(wait_for=[params_validate_result])
    normalized_mondo_terms_result = normalized_mondo_terms.submit(wait_for=[download_mondo_terms_result])
    index_mondo_terms_result = index_mondo_terms.submit(wait_for=[normalized_mondo_terms_result])
    publish_mondo_result = publish_mondo.submit(wait_for=[index_mondo_terms_result])
    slack.submit(wait_for=[publish_mondo_result])

if __name__ == "__main__":
    etl_import_mondo_flow()