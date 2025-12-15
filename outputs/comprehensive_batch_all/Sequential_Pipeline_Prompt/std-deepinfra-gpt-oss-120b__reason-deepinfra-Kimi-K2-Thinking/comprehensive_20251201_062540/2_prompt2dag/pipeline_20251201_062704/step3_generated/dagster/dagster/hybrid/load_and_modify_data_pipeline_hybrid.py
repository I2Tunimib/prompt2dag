from dagster import op, job, fs_io_manager, ResourceDefinition
from dagster_docker import docker_executor


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


# Simple hard‑coded resource for the required `data_dir`
data_dir_resource = ResourceDefinition.hardcoded_resource("/tmp/data")


@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "data_dir": data_dir_resource,
    },
)
def load_and_modify_data_pipeline():
    load_and_modify_data() >> reconcile_city_names() >> enrich_with_openmeteo() >> extend_columns() >> save_final_data()