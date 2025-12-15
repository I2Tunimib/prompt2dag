from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='notify_failure_slack', retries=3)
def notify_failure_slack():
    """Task: Slack Failure Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='run_ctas', retries=0)
def run_ctas():
    """Task: Run CTAS to Build Analytics Tables"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="runelt_alert", task_runner=SequentialTaskRunner)
def runelt_alert():
    """Sequential ELT pipeline that builds analytics tables in Snowflake."""
    run_ctas()
    notify_failure_slack()


if __name__ == "__main__":
    runelt_alert()