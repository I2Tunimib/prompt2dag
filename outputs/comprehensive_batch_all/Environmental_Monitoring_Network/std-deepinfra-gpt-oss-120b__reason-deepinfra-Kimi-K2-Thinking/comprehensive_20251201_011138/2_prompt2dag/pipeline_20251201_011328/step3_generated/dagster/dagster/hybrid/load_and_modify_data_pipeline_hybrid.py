from dagster import op, job, ResourceDefinition, fs_io_manager, docker_executor


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
    name='geocode_reconciliation',
    description='Geocode Reconciliation',
)
def geocode_reconciliation(context):
    """Op: Geocode Reconciliation"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@op(
    name='open_meteo_extension',
    description='OpenMeteo Data Extension',
)
def open_meteo_extension(context):
    """Op: OpenMeteo Data Extension"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass


@op(
    name='land_use_extension',
    description='Land Use Extension',
)
def land_use_extension(context):
    """Op: Land Use Extension"""
    # Docker execution
    # Image: geoapify-land-use:latest
    pass


@op(
    name='population_density_extension',
    description='Population Density Extension',
)
def population_density_extension(context):
    """Op: Population Density Extension"""
    # Docker execution
    # Image: worldpop-density:latest
    pass


@op(
    name='environmental_risk_calculation',
    description='Environmental Risk Calculation',
)
def environmental_risk_calculation(context):
    """Op: Environmental Risk Calculation"""
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


# Placeholder resources
fs_data_dir = ResourceDefinition.hardcoded_resource("/tmp")
app_network = ResourceDefinition.hardcoded_resource("default")


@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={
        "fs_data_dir": fs_data_dir,
        "app_network": app_network,
        "io_manager": fs_io_manager,
    },
)
def load_and_modify_data_pipeline():
    load = load_and_modify_data()
    geocode = geocode_reconciliation()
    meteo = open_meteo_extension()
    land = land_use_extension()
    pop = population_density_extension()
    risk = environmental_risk_calculation()
    save = save_final_data()

    load >> geocode >> meteo >> land >> pop >> risk >> save