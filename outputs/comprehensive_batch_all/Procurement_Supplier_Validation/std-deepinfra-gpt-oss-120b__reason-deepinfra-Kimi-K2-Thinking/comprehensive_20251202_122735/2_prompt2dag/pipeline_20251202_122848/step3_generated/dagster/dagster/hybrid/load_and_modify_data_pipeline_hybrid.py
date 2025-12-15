from dagster import op, job, ResourceDefinition, fs_io_manager
from dagster_docker import docker_executor


def _data_dir_resource(_):
    # Placeholder implementation; replace with actual logic as needed
    return "/tmp/data"


data_dir = ResourceDefinition(resource_fn=_data_dir_resource)


@op(
    name="load_and_modify_data",
    description="Load and Modify Supplier Data",
)
def load_and_modify_data(context):
    """Op: Load and Modify Supplier Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@op(
    name="reconcile_supplier_names",
    description="Entity Reconciliation (Wikidata)",
)
def reconcile_supplier_names(context):
    """Op: Entity Reconciliation (Wikidata)"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@op(
    name="save_validated_data",
    description="Save Final Validated Supplier Data",
)
def save_validated_data(context):
    """Op: Save Final Validated Supplier Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@job(
    name="load_and_modify_data_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={"data_dir": data_dir, "io_manager": fs_io_manager},
)
def load_and_modify_data_pipeline():
    load = load_and_modify_data()
    reconcile = reconcile_supplier_names().after(load)
    save_validated_data().after(reconcile)