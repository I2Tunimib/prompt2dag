from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import DeploymentSpec
from prefect.server.schemas.schedules import CronSchedule


@task(name="run_system_check", retries=0)
def run_system_check():
    """Task: Run System Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="execute_hive_script", retries=0)
def execute_hive_script():
    """Task: Execute Hive Script"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="run_system_check_pipeline",
    task_runner=SequentialTaskRunner(),
)
def run_system_check_pipeline():
    """Sequential pipeline that runs a system check and then executes a Hive script."""
    run_system_check()
    execute_hive_script()


# Deployment configuration
DeploymentSpec(
    name="run_system_check_pipeline_deployment",
    flow=run_system_check_pipeline,
    schedule=CronSchedule(cron="00 1 * * *"),
    work_pool_name="default-agent-pool",
)