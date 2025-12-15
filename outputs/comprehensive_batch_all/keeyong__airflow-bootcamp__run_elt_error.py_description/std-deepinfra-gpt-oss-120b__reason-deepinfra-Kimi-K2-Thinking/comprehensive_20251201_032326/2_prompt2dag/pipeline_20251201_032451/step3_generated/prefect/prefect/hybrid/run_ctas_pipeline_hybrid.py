from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="run_ctas", retries=0)
def run_ctas():
    """Task: Create Analytics Table via CTAS"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="slack_failure_alert", retries=0)
def slack_failure_alert():
    """Task: Slack Failure Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="run_ctas_pipeline",
    task_runner=SequentialTaskRunner(),
)
def run_ctas_pipeline():
    """Sequential pipeline that runs CTAS and then sends a Slack alert on completion."""
    # Entry task
    run_ctas()
    # Dependent task
    slack_failure_alert()


if __name__ == "__main__":
    run_ctas_pipeline()