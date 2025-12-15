from prefect import flow, task, triggers
from prefect.task_runners import SequentialTaskRunner


@task(name='notify_slack_start', retries=1)
def notify_slack_start():
    """Task: Slack Notification on DAG Start"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='check_and_download_ensembl_files', retries=2)
def check_and_download_ensembl_files():
    """Task: Check and Download Ensembl Mapping Files"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='process_ensembl_mapping_spark', retries=3)
def process_ensembl_mapping_spark():
    """Task: Process Ensembl Mapping with Spark"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='notify_slack_success', retries=1)
def notify_slack_success():
    """Task: Slack Notification on DAG Completion"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='notify_slack_failure', retries=1)
def notify_slack_failure():
    """Task: Slack Notification on Task Failure"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='outcome_branch')
def outcome_branch():
    """Branching placeholder to enable fan‑out to success/failure notifications."""
    # No operation – serves only as a dependency anchor.
    pass


@flow(name="etl_import_ensembl", task_runner=SequentialTaskRunner())
def etl_import_ensembl():
    """Main ETL flow for importing Ensembl data."""
    # Entry point
    start = notify_slack_start()

    # Sequential steps
    download = check_and_download_ensembl_files(wait_for=[start])
    process = process_ensembl_mapping_spark(wait_for=[download])

    # Branch point for fan‑out
    branch = outcome_branch(wait_for=[process])

    # Fan‑out to success/failure notifications
    notify_slack_success(wait_for=[branch], trigger=triggers.all_successful)
    notify_slack_failure(wait_for=[branch], trigger=triggers.any_failed)


if __name__ == "__main__":
    etl_import_ensembl()