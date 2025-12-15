from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='analyze_transactions', retries=2)
def analyze_transactions():
    """Task: Analyze Transactions"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='route_transaction', retries=2)
def route_transaction():
    """Task: Route Transaction"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='route_to_auto_approve', retries=2)
def route_to_auto_approve():
    """Task: Route to Auto Approve"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='route_to_manual_review', retries=2)
def route_to_manual_review():
    """Task: Route to Manual Review"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_notification', retries=2)
def send_notification():
    """Task: Send Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="analyze_transactions_pipeline", task_runner=ConcurrentTaskRunner)
def analyze_transactions_pipeline():
    logger = get_run_logger()
    logger.info("Starting analyze_transactions_pipeline")

    # Entry point
    analyze_transactions_result = analyze_transactions.submit()

    # Fan out
    route_transaction_result = route_transaction.submit(analyze_transactions_result)
    route_to_manual_review_result = route_to_manual_review.submit(route_transaction_result)
    route_to_auto_approve_result = route_to_auto_approve.submit(route_transaction_result)

    # Fan in
    send_notification.submit(route_to_manual_review_result, route_to_auto_approve_result)

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=analyze_transactions_pipeline,
    name="analyze_transactions_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()