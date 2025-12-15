from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="extract_user_content", retries=2)
def extract_user_content():
    """Task: Extract User Content CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="evaluate_toxicity", retries=2)
def evaluate_toxicity():
    """Task: Evaluate Toxicity and Branch"""
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
    """Task: Remove Toxic Content and Flag Users"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="audit_log", retries=2)
def audit_log():
    """Task: Create Consolidated Audit Log"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="content_moderation_pipeline",
    task_runner=SequentialTaskRunner(),
)
def content_moderation_pipeline():
    """
    Content Moderation Pipeline
    Fan‑out/Fan‑in pattern:
        extract_user_content → evaluate_toxicity
        evaluate_toxicity → (remove_and_flag_content, publish_content)  # fan‑out
        (remove_and_flag_content, publish_content) → audit_log          # fan‑in
    """
    # Entry point
    extract_user_content()

    # Branching point
    evaluate_toxicity()

    # Fan‑out: two parallel (sequential runner will execute one after the other)
    remove_and_flag_content()
    publish_content()

    # Fan‑in: consolidate results
    audit_log()


if __name__ == "__main__":
    content_moderation_pipeline()