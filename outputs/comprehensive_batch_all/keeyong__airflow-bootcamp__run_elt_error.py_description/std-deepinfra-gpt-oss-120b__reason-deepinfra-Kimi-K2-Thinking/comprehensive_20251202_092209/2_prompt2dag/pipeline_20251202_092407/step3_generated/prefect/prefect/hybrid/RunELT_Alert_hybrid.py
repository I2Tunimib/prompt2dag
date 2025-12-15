from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='create_analytics_table', retries=0)
def create_analytics_table():
    """Task: Create Analytics Table via CTAS"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='slack_failure_notifier', retries=3)
def slack_failure_notifier():
    """Task: Slack Failure Notifier"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="runelt_alert", task_runner=SequentialTaskRunner())
def runelt_alert():
    """Main flow for RunELT_Alert pipeline."""
    # Entry task
    create_analytics_table()
    # Dependent task
    slack_failure_notifier()


if __name__ == "__main__":
    runelt_alert()