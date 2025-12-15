from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="run_system_check", retries=0)
def run_system_check():
    """Task: Run System Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="run_hive_script", retries=0)
def run_hive_script():
    """Task: Run Hive Script"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="run_system_check_pipeline",
    task_runner=SequentialTaskRunner(),
)
def run_system_check_pipeline():
    """Sequential pipeline that runs a system check followed by a Hive script."""
    system_check_result = run_system_check()
    run_hive_script(wait_for=[system_check_result])


if __name__ == "__main__":
    run_system_check_pipeline()