from dagster import op, job, ResourceDefinition, fs_io_manager
from dagster_docker import DockerExecutor


# Dummy resource implementations (replace with real logic as needed)
def _app_network_resource(_):
    return None


def _data_dir_fs_resource(_):
    return None


@op(
    name="load_and_modify_data",
    description="Load and Modify Data",
)
def load_and_modify_data(context):
    """Op: Load and Modify Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@op(
    name="reconcile_city_names",
    description="Data Reconciliation",
)
def reconcile_city_names(context):
    """Op: Data Reconciliation"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@op(
    name="enrich_with_openmeteo",
    description="OpenMeteo Data Extension",
)
def enrich_with_openmeteo(context):
    """Op: OpenMeteo Data Extension"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass


@op(
    name="extend_columns",
    description="Column Extension",
)
def extend_columns(context):
    """Op: Column Extension"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@op(
    name="save_final_data",
    description="Save Final Data",
)
def save_final_data(context):
    """Op: Save Final Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=DockerExecutor(),
    resource_defs={
        "app_network": ResourceDefinition.resource_fn(_app_network_resource),
        "data_dir_fs": ResourceDefinition.resource_fn(_data_dir_fs_resource),
        "io_manager": fs_io_manager,
    },
)
def load_and_modify_data_pipeline():
    # Sequential execution: each op depends on the previous one.
    load_and_modify_data() >> reconcile_city_names() >> enrich_with_openmeteo() >> extend_columns() >> save_final_data()