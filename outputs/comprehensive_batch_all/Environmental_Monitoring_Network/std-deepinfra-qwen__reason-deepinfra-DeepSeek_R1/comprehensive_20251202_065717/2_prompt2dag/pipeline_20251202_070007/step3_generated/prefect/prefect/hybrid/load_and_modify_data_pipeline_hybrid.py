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

@task(name='extend_openmeteo_data', retries=1)
def extend_openmeteo_data():
    """Task: Extend OpenMeteo Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass

@task(name='extend_land_use', retries=1)
def extend_land_use():
    """Task: Extend Land Use"""
    # Docker execution via infrastructure
    # Image: geoapify-land-use:latest
    pass

@task(name='extend_population_density', retries=1)
def extend_population_density():
    """Task: Extend Population Density"""
    # Docker execution via infrastructure
    # Image: worldpop-density:latest
    pass

@task(name='extend_environmental_risk', retries=1)
def extend_environmental_risk():
    """Task: Extend Environmental Risk"""
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
    reconcile_data = reconcile_geocoding(wait_for=[load_data])
    extend_meteo_data = extend_openmeteo_data(wait_for=[reconcile_data])
    extend_land_data = extend_land_use(wait_for=[extend_meteo_data])
    extend_population_data = extend_population_density(wait_for=[extend_land_data])
    extend_risk_data = extend_environmental_risk(wait_for=[extend_population_data])
    save_data = save_final_data(wait_for=[extend_risk_data])

if __name__ == "__main__":
    load_and_modify_data_pipeline()