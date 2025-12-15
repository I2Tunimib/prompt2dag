from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='run_system_check', retries=0)
def run_system_check():
    """Task: Run System Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='run_hive_script', retries=0)
def run_hive_script():
    """Task: Run Hive Script"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="[pipeline_name]", task_runner=SequentialTaskRunner())
def pipeline_name():
    """Simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data."""
    run_system_check()
    run_hive_script()


if __name__ == "__main__":
    pipeline_name()