from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import DeploymentSpec
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
    """Task: Create Audit Log"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="extract_user_content_pipeline",
    task_runner=SequentialTaskRunner(),
)
def extract_user_content_pipeline():
    # Entry point
    extract_user_content()
    # Fan‑out
    evaluate_toxicity()
    remove_and_flag_content()
    publish_content()
    # Fan‑in
    audit_log()


# Deployment configuration
DeploymentSpec(
    name="extract_user_content_pipeline_deployment",
    flow=extract_user_content_pipeline,
    schedule=CronSchedule(cron="0 0 * * *"),  # daily
    work_pool_name="default-agent-pool",
    task_runner=SequentialTaskRunner(),
)