from dagster import job, op, ResourceDefinition, fs_io_manager, dagster_type_loader, dagster_type_materializer

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
    name='entity_reconciliation',
    description='Entity Reconciliation',
)
def entity_reconciliation(context):
    """Op: Entity Reconciliation"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
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
    name='load_and_modify_data_pipeline',
    description='No description provided.',
    executor_def=ResourceDefinition.docker_executor(),
    resource_defs={'io_manager': fs_io_manager, 'data_dir': ResourceDefinition.string_resource()},
)
def load_and_modify_data_pipeline():
    load_data = load_and_modify_data()
    reconcile_data = entity_reconciliation(load_data)
    save_final_data(reconcile_data)