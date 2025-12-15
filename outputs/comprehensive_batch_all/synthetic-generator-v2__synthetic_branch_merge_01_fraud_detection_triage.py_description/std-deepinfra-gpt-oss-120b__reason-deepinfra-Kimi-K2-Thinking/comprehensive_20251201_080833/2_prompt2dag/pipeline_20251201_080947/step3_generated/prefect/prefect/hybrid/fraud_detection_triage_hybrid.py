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


@task(name="process_auto_approve", retries=2)
def process_auto_approve():
    """Task: Process Auto Approval"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="process_manual_review", retries=2)
def process_manual_review():
    """Task: Process Manual Review"""
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
    name="fraud_detection_triage",
    task_runner=ConcurrentTaskRunner(),
)
def fraud_detection_triage():
    """Main flow for fraud detection triage using a fanout-fanin pattern."""
    # Entry point
    analyze = analyze_transactions()

    # Route transaction after analysis
    routed = route_transaction(wait_for=[analyze])

    # Fan‑out: parallel processing paths
    manual = process_manual_review(wait_for=[routed])
    auto = process_auto_approve(wait_for=[routed])

    # Fan‑in: send notification after both paths complete
    send_notification(wait_for=[manual, auto])


if __name__ == "__main__":
    fraud_detection_triage()