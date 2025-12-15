from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name="analyze_transactions", retries=2)
def analyze_transactions():
    """Task: Analyze Transactions"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="route_transaction", retries=2)
def route_transaction():
    """Task: Route Transaction"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="route_to_auto_approve", retries=2)
def route_to_auto_approve():
    """Task: Route to Auto Approve"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="route_to_manual_review", retries=2)
def route_to_manual_review():
    """Task: Route to Manual Review"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="send_notification", retries=2)
def send_notification():
    """Task: Send Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="analyze_transactions_pipeline",
    task_runner=ConcurrentTaskRunner(),
)
def analyze_transactions_pipeline():
    # Entry point
    analysis = analyze_transactions()

    # Fan‑out: route transaction after analysis
    routed = route_transaction(wait_for=[analysis])

    # Parallel branches
    manual = route_to_manual_review(wait_for=[routed])
    auto = route_to_auto_approve(wait_for=[routed])

    # Fan‑in: send notification after both branches complete
    send_notification(wait_for=[manual, auto])


if __name__ == "__main__":
    analyze_transactions_pipeline()