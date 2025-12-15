from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

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
    logger = get_run_logger()
    logger.info("Starting the load_and_modify_data_pipeline")

    # Task execution in sequential order
    load_and_modify_data_result = load_and_modify_data()
    reconcile_geocoding_result = reconcile_geocoding(wait_for=[load_and_modify_data_result])
    calculate_distance_pt_result = calculate_distance_pt(wait_for=[reconcile_geocoding_result])
    calculate_distance_residential_result = calculate_distance_residential(wait_for=[calculate_distance_pt_result])
    save_final_data_result = save_final_data(wait_for=[calculate_distance_residential_result])

    logger.info("load_and_modify_data_pipeline completed successfully")

if __name__ == "__main__":
    load_and_modify_data_pipeline()