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
    name='data_reconciliation',
    description='Data Reconciliation',
)
def data_reconciliation(context):
    """Op: Data Reconciliation"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass

@op(
    name='openmeteo_data_extension',
    description='OpenMeteo Data Extension',
)
def openmeteo_data_extension(context):
    """Op: OpenMeteo Data Extension"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass

@op(
    name='column_extension',
    description='Column Extension',
)
def column_extension(context):
    """Op: Column Extension"""
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
    resource_defs={
        "io_manager": fs_io_manager,
        "geocoding_api": ResourceDefinition.none_resource(),
        "data_directory": ResourceDefinition.none_resource(),
        "openmeteo_api": ResourceDefinition.none_resource(),
    },
)
def load_and_modify_data_pipeline():
    load_data = load_and_modify_data()
    reconcile_data = data_reconciliation(load_data)
    extend_openmeteo_data = openmeteo_data_extension(reconcile_data)
    extend_columns = column_extension(extend_openmeteo_data)
    save_final_data(extend_columns)