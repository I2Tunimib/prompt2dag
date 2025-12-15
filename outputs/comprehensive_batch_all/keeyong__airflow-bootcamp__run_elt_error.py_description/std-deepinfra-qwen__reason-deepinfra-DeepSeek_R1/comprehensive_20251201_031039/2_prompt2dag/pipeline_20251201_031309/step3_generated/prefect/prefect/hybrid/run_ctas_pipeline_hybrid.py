from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import SequentialTaskRunner

@task(name='run_ctas', retries=0)
def run_ctas():
    """Task: Run CTAS"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_slack_notification', retries=0)
def send_slack_notification():
    """Task: Send Slack Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="run_ctas_pipeline", task_runner=SequentialTaskRunner)
def run_ctas_pipeline():
    logger = get_run_logger()
    logger.info("Starting run_ctas_pipeline")

    # Run tasks in sequential order
    run_ctas_result = run_ctas()
    send_slack_notification_result = send_slack_notification(wait_for=[run_ctas_result])

    logger.info("run_ctas_pipeline completed successfully")

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=run_ctas_pipeline,
    name="run_ctas_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()
```
```python
# This block is for local testing and can be removed or commented out
if __name__ == "__main__":
    run_ctas_pipeline()