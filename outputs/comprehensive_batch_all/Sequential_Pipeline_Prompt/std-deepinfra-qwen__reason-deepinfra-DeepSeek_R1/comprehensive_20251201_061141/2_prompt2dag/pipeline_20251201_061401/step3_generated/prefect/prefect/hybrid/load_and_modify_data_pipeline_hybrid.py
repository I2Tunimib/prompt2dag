from prefect import flow, task, SequentialTaskRunner

@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@task(name='reconcile_data', retries=1)
def reconcile_data():
    """Task: Data Reconciliation"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass

@task(name='extend_with_weather_data', retries=1)
def extend_with_weather_data():
    """Task: OpenMeteo Data Extension"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass

@task(name='extend_columns', retries=1)
def extend_columns():
    """Task: Column Extension"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass

@task(name='save_final_data', retries=1)
def save_final_data():
    """Task: Save Final Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass

@flow(name="load_and_modify_data_pipeline", task_runner=SequentialTaskRunner)
def load_and_modify_data_pipeline():
    load_data_result = load_and_modify_data()
    reconcile_data_result = reconcile_data(wait_for=[load_data_result])
    extend_weather_data_result = extend_with_weather_data(wait_for=[reconcile_data_result])
    extend_columns_result = extend_columns(wait_for=[extend_weather_data_result])
    save_final_data(wait_for=[extend_columns_result])

if __name__ == "__main__":
    load_and_modify_data_pipeline()