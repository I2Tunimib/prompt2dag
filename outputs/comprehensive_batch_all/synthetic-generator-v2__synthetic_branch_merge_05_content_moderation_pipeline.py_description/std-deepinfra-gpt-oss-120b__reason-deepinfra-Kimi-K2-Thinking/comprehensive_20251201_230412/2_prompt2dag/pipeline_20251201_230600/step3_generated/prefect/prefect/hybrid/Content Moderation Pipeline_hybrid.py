from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.orion.schemas.schedules import CronSchedule


@task(name="extract_user_content", retries=2)
def extract_user_content():
    """Task: Extract User Content"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="evaluate_toxicity", retries=2)
def evaluate_toxicity():
    """Task: Evaluate Toxicity"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="publish_content", retries=2)
def publish_content():
    """Task: Publish Safe Content"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="remove_and_flag_content", retries=2)
def remove_and_flag_content():
    """Task: Remove and Flag Toxic Content"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="audit_log", retries=2)
def audit_log():
    """Task: Audit Log Consolidation"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="content_moderation_pipeline",
    task_runner=SequentialTaskRunner(),
    schedule=CronSchedule(cron="0 0 * * *")  # daily schedule
)
def content_moderation_pipeline():
    """
    Content Moderation Pipeline
    - Extract user content
    - Evaluate toxicity
    - Fan‑out: publish safe content OR remove & flag toxic content
    - Fan‑in: consolidate audit log
    """
    # Entry point
    extract_user_content()

    # Evaluate toxicity based on extracted content
    evaluate_toxicity()

    # Fan‑out: both branches depend on toxicity evaluation
    remove_and_flag_content()
    publish_content()

    # Fan‑in: audit log runs after both branches complete
    audit_log()


if __name__ == "__main__":
    # Execute the flow when the script is run directly
    content_moderation_pipeline()