import prefect
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

@task(name='run_ctas', retries=3)
def run_ctas():
    """Task: Run CTAS"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="run_ctas_pipeline", task_runner=SequentialTaskRunner)
def run_ctas_pipeline():
    logger = get_run_logger()
    logger.info("Starting run_ctas_pipeline")
    
    run_ctas()

if __name__ == "__main__":
    run_ctas_pipeline()