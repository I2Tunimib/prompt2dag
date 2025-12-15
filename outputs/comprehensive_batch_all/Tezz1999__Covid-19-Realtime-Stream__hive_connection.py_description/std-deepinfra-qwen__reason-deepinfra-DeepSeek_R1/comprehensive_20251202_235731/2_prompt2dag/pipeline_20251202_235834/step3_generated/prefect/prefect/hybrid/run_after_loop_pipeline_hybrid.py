from prefect import flow, task, SequentialTaskRunner

@task(name='run_after_loop', retries=0)
def run_after_loop():
    """Task: Run After Loop"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='hive_script_task', retries=0)
def hive_script_task():
    """Task: Hive Script Task"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="run_after_loop_pipeline", task_runner=SequentialTaskRunner)
def run_after_loop_pipeline():
    run_after_loop_result = run_after_loop()
    hive_script_task_result = hive_script_task(wait_for=[run_after_loop_result])

if __name__ == "__main__":
    run_after_loop_pipeline()