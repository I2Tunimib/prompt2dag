from prefect import flow, task, SequentialTaskRunner

@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@task(name='entity_reconciliation', retries=1)
def entity_reconciliation():
    """Task: Entity Reconciliation"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
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
    reconciliation_result = entity_reconciliation(wait_for=[load_data_result])
    save_final_data(wait_for=[reconciliation_result])

if __name__ == "__main__":
    load_and_modify_data_pipeline()