from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load & Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name='geocode_reconciliation', retries=1)
def geocode_reconciliation():
    """Task: Geocoding Reconciliation"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@task(name='openmeteo_extension', retries=1)
def openmeteo_extension():
    """Task: OpenMeteo Data Extension"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass


@task(name='land_use_extension', retries=1)
def land_use_extension():
    """Task: Land Use Extension"""
    # Docker execution via infrastructure
    # Image: geoapify-land-use:latest
    pass


@task(name='population_density_extension', retries=1)
def population_density_extension():
    """Task: Population Density Extension"""
    # Docker execution via infrastructure
    # Image: worldpop-density:latest
    pass


@task(name='calculate_environmental_risk', retries=1)
def calculate_environmental_risk():
    """Task: Environmental Calculation (Column Extension)"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@task(name='save_final_data', retries=1)
def save_final_data():
    """Task: Save Final Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@flow(name="load_and_modify_data_pipeline", task_runner=SequentialTaskRunner())
def load_and_modify_data_pipeline():
    """Sequential pipeline orchestrating data loading, enrichment, and saving."""
    load_task = load_and_modify_data()
    geocode_task = geocode_reconciliation(wait_for=[load_task])
    openmeteo_task = openmeteo_extension(wait_for=[geocode_task])
    land_use_task = land_use_extension(wait_for=[openmeteo_task])
    pop_density_task = population_density_extension(wait_for=[land_use_task])
    calc_risk_task = calculate_environmental_risk(wait_for=[pop_density_task])
    save_task = save_final_data(wait_for=[calc_risk_task])
    return save_task


if __name__ == "__main__":
    load_and_modify_data_pipeline()