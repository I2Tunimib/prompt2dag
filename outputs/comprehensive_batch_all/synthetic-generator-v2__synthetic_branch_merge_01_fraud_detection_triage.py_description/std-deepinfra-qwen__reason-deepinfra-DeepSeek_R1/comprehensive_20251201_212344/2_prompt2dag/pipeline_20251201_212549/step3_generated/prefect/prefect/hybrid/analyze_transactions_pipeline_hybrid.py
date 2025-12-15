from prefect import flow, task, get_run_logger
from prefect.orion.schemas.states import Completed
from prefect.task_runners import ConcurrentTaskRunner

# Task Definitions
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

# Flow Definition
@flow(name="analyze_transactions_pipeline", task_runner=ConcurrentTaskRunner, version="2.14.0")
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

if __name__ == "__main__":
    analyze_transactions_pipeline()