from prefect import flow, task, SequentialTaskRunner

@task(name='run_system_check', retries=0)
def run_system_check():
    """Task: Run System Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='execute_hive_script', retries=0)
def execute_hive_script():
    """Task: Execute Hive Script"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="run_system_check_pipeline", task_runner=SequentialTaskRunner)
def run_system_check_pipeline():
    run_system_check_result = run_system_check()
    execute_hive_script_result = execute_hive_script(wait_for=[run_system_check_result])

if __name__ == "__main__":
    run_system_check_pipeline()