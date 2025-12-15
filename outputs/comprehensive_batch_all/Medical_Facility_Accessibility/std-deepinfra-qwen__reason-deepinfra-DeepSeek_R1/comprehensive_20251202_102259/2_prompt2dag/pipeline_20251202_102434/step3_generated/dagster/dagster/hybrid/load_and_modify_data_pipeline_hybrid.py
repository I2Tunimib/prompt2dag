from dagster import job, op, graph, fs_io_manager, docker_executor, ResourceDefinition

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
    name='calculate_distance_pt',
    description='Calculate Distance to Public Transport',
)
def calculate_distance_pt(context):
    """Op: Calculate Distance to Public Transport"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass

@op(
    name='calculate_distance_residential',
    description='Calculate Distance to Residential Areas',
)
def calculate_distance_residential(context):
    """Op: Calculate Distance to Residential Areas"""
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
    name='load_and_modify_data_pipeline',
    description='No description provided.',
    executor_def=docker_executor,
    resource_defs={'io_manager': fs_io_manager, 'app_network': ResourceDefinition.none_resource()},
)
def load_and_modify_data_pipeline():
    load_data = load_and_modify_data()
    reconcile = reconcile_geocoding(load_data)
    calc_distance_pt = calculate_distance_pt(reconcile)
    calc_distance_residential = calculate_distance_residential(calc_distance_pt)
    save_final_data(calc_distance_residential)