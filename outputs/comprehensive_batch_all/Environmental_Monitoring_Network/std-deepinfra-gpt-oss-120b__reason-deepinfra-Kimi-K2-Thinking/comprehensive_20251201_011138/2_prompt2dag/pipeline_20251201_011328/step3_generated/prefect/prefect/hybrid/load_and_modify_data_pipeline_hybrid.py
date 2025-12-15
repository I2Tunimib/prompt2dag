from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name='geocode_reconciliation', retries=1)
def geocode_reconciliation():
    """Task: Geocode Reconciliation"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@task(name='open_meteo_extension', retries=1)
def open_meteo_extension():
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


@task(name='environmental_risk_calculation', retries=1)
def environmental_risk_calculation():
    """Task: Environmental Risk Calculation"""
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
    load_and_modify_data()
    geocode_reconciliation()
    open_meteo_extension()
    land_use_extension()
    population_density_extension()
    environmental_risk_calculation()
    save_final_data()


if __name__ == "__main__":
    load_and_modify_data_pipeline()