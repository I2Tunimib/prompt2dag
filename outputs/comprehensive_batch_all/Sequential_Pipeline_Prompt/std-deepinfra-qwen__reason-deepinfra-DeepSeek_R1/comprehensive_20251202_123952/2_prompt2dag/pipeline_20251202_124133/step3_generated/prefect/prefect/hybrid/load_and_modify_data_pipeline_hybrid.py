from prefect import flow, task, SequentialTaskRunner

@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@task(name='data_reconciliation', retries=1)
def data_reconciliation():
    """Task: Data Reconciliation"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass

@task(name='openmeteo_data_extension', retries=1)
def openmeteo_data_extension():
    """Task: OpenMeteo Data Extension"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass

@task(name='column_extension', retries=1)
def column_extension():
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
    load_and_modify_data_result = load_and_modify_data()
    data_reconciliation_result = data_reconciliation(wait_for=[load_and_modify_data_result])
    openmeteo_data_extension_result = openmeteo_data_extension(wait_for=[data_reconciliation_result])
    column_extension_result = column_extension(wait_for=[openmeteo_data_extension_result])
    save_final_data_result = save_final_data(wait_for=[column_extension_result])

if __name__ == "__main__":
    load_and_modify_data_pipeline()