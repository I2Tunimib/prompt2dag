from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='load_modify_facilities', retries=1)
def load_modify_facilities():
    """Task: Load and Modify Facilities Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name='geocode_facilities', retries=1)
def geocode_facilities():
    """Task: Geocode Facilities Using HERE API"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@task(name='calculate_distance_to_public_transport', retries=1)
def calculate_distance_to_public_transport():
    """Task: Calculate Distance to Nearest Public Transport"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@task(name='calculate_distance_to_residential_areas', retries=1)
def calculate_distance_to_residential_areas():
    """Task: Calculate Distance to Nearest Residential Area"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@task(name='save_facilities_accessibility', retries=1)
def save_facilities_accessibility():
    """Task: Save Enriched Facility Accessibility Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@flow(name="load_modify_facilities_pipeline", task_runner=SequentialTaskRunner())
def load_modify_facilities_pipeline():
    """Sequential pipeline orchestrating facility data processing."""
    load_modify_facilities()
    geocode_facilities()
    calculate_distance_to_public_transport()
    calculate_distance_to_residential_areas()
    save_facilities_accessibility()


if __name__ == "__main__":
    load_modify_facilities_pipeline()