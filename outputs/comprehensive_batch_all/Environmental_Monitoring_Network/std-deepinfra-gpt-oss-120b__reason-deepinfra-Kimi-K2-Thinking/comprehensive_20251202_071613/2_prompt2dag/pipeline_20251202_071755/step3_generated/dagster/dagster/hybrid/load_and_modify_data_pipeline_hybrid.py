from dagster import op, job, ResourceDefinition, fs_io_manager

# Pre-Generated Task Definitions (use exactly as provided)

@op(
    name='load_and_modify_data',
    description='Load and Modify Station Data',
)
def load_and_modify_data(context):
    """Op: Load and Modify Station Data"""
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
    name='openmeteo_extension',
    description='OpenMeteo Weather Data Extension',
)
def openmeteo_extension(context):
    """Op: OpenMeteo Weather Data Extension"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass


@op(
    name='land_use_extension',
    description='Land Use Classification Extension',
)
def land_use_extension(context):
    """Op: Land Use Classification Extension"""
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
    description='Save Final Enriched Dataset',
)
def save_final_data(context):
    """Op: Save Final Enriched Dataset"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass


# Dummy resource definitions to satisfy required resources
_dummy_resource = ResourceDefinition.hardcoded_resource(None)

resource_defs = {
    "risk_calc_api": _dummy_resource,
    "openmeteo_api": _dummy_resource,
    "population_api": _dummy_resource,
    "geocoding_api": _dummy_resource,
    "fs_data_dir": _dummy_resource,
    "load_modify_api": _dummy_resource,
    "save_api": _dummy_resource,
    "land_use_api": _dummy_resource,
    "io_manager": fs_io_manager,
}


@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    resource_defs=resource_defs,
)
def load_and_modify_data_pipeline():
    # Sequential wiring of ops
    load_and_modify_data() \
        >> geocode_reconciliation() \
        >> openmeteo_extension() \
        >> land_use_extension() \
        >> population_density_extension() \
        >> environmental_risk_calculation() \
        >> save_final_data()