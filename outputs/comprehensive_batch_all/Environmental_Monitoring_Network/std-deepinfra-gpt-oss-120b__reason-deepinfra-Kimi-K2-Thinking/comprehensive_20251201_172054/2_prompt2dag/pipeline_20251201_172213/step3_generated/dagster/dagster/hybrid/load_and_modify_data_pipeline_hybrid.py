from dagster import op, job, fs_io_manager, ResourceDefinition

@op(
    name='load_and_modify_data',
    description='Load & Modify Data',
)
def load_and_modify_data(context):
    """Op: Load & Modify Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@op(
    name='geocode_reconciliation',
    description='Geocoding Reconciliation',
)
def geocode_reconciliation(context):
    """Op: Geocoding Reconciliation"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@op(
    name='openmeteo_extension',
    description='OpenMeteo Data Extension',
)
def openmeteo_extension(context):
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
    name='calculate_environmental_risk',
    description='Environmental Calculation (Column Extension)',
)
def calculate_environmental_risk(context):
    """Op: Environmental Calculation (Column Extension)"""
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
    resource_defs={
        "fs_io_manager": fs_io_manager,
        "geoapify_api": ResourceDefinition.hardcoded_resource(None),
        "worldpop_api": ResourceDefinition.hardcoded_resource(None),
        "save_service": ResourceDefinition.hardcoded_resource(None),
        "openmeteo_api": ResourceDefinition.hardcoded_resource(None),
        "here_api": ResourceDefinition.hardcoded_resource(None),
        "column_extension_api": ResourceDefinition.hardcoded_resource(None),
        "data_dir_fs": ResourceDefinition.hardcoded_resource(None),
    },
)
def load_and_modify_data_pipeline():
    load_and_modify_data() \
        >> geocode_reconciliation() \
        >> openmeteo_extension() \
        >> land_use_extension() \
        >> population_density_extension() \
        >> calculate_environmental_risk() \
        >> save_final_data()