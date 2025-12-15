from dagster import job, op, graph, In, Out, ResourceDefinition, fs_io_manager, docker_executor

# Task Definitions
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
    name='reconcile_data',
    description='Data Reconciliation',
)
def reconcile_data(context):
    """Op: Data Reconciliation"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass

@op(
    name='extend_with_weather_data',
    description='OpenMeteo Data Extension',
)
def extend_with_weather_data(context):
    """Op: OpenMeteo Data Extension"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass

@op(
    name='extend_columns',
    description='Column Extension',
)
def extend_columns(context):
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

# Job Definition
@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "openmeteo_api": ResourceDefinition.mock_resource(),
        "geocoding_api": ResourceDefinition.mock_resource(),
        "data_directory": ResourceDefinition.mock_resource(),
    },
)
def load_and_modify_data_pipeline():
    load_data = load_and_modify_data()
    reconcile_data(load_data)
    extend_with_weather_data(reconcile_data)
    extend_columns(extend_with_weather_data)
    save_final_data(extend_columns)