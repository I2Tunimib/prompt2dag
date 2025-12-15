from dagster import job, op, ResourceDefinition, fs_io_manager, docker_executor

@op(
    name='load_and_modify_data',
    description='Load and Modify Data',
)
def load_and_modify_data(context):
    """Op: Load and Modify Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass

@op(
    name='reconcile_geocoding',
    description='Reconcile Geocoding',
)
def reconcile_geocoding(context):
    """Op: Reconcile Geocoding"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass

@op(
    name='extend_openmeteo_data',
    description='Extend OpenMeteo Data',
)
def extend_openmeteo_data(context):
    """Op: Extend OpenMeteo Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass

@op(
    name='extend_land_use',
    description='Extend Land Use',
)
def extend_land_use(context):
    """Op: Extend Land Use"""
    # Docker execution
    # Image: geoapify-land-use:latest
    pass

@op(
    name='extend_population_density',
    description='Extend Population Density',
)
def extend_population_density(context):
    """Op: Extend Population Density"""
    # Docker execution
    # Image: worldpop-density:latest
    pass

@op(
    name='extend_environmental_risk',
    description='Extend Environmental Risk',
)
def extend_environmental_risk(context):
    """Op: Extend Environmental Risk"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass

@op(
    name='save_final_data',
    description='Save Final Data',
)
def save_final_data(context):
    """Op: Save Final Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass

@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={"io_manager": fs_io_manager, "app_network": ResourceDefinition.hardcoded_resource("app_network")},
)
def load_and_modify_data_pipeline():
    load_data = load_and_modify_data()
    reconcile_data = reconcile_geocoding(load_data)
    extend_openmeteo = extend_openmeteo_data(reconcile_data)
    extend_land = extend_land_use(extend_openmeteo)
    extend_population = extend_population_density(extend_land)
    extend_risk = extend_environmental_risk(extend_population)
    save_final_data(extend_risk)