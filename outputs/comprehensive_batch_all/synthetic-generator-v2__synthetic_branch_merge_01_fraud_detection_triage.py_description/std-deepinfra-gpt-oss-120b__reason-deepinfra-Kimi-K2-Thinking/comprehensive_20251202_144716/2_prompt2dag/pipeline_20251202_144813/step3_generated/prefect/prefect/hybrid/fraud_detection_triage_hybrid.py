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
    """Task: Route to Auto‑Approve"""
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
    name="fraud_detection_triage",
    task_runner=ConcurrentTaskRunner(),
)
def fraud_detection_triage():
    """
    Fraud detection triage flow implementing a fan‑out / fan‑in pattern.
    """
    # Entry point
    analyze_fut = analyze_transactions.submit()

    # Next step depends on analysis
    route_fut = route_transaction.submit(wait_for=[analyze_fut])

    # Fan‑out: parallel routing decisions
    manual_fut = route_to_manual_review.submit(wait_for=[route_fut])
    auto_fut = route_to_auto_approve.submit(wait_for=[route_fut])

    # Fan‑in: notification after both routing paths complete
    send_notification.submit(wait_for=[manual_fut, auto_fut])


# If this script is executed directly, run the flow
if __name__ == "__main__":
    fraud_detection_triage()