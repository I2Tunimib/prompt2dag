from prefect import flow, task, SequentialTaskRunner

@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@task(name='reconcile_geocoding', retries=1)
def reconcile_geocoding():
    """Task: Reconcile Geocoding"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass

@task(name='calculate_distance_pt', retries=1)
def calculate_distance_pt():
    """Task: Calculate Distance to Public Transport"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass

@task(name='calculate_distance_residential', retries=1)
def calculate_distance_residential():
    """Task: Calculate Distance to Residential Areas"""
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
    load_data = load_and_modify_data()
    reconcile = reconcile_geocoding(wait_for=[load_data])
    calc_distance_pt = calculate_distance_pt(wait_for=[reconcile])
    calc_distance_residential = calculate_distance_residential(wait_for=[calc_distance_pt])
    save_data = save_final_data(wait_for=[calc_distance_residential])

if __name__ == "__main__":
    load_and_modify_data_pipeline()