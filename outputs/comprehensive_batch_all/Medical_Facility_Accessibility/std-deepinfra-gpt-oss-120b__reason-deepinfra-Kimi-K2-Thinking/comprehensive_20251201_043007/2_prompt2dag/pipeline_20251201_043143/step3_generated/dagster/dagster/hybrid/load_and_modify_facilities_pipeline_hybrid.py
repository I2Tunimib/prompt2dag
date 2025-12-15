from dagster import op, job, DockerExecutor, fs_io_manager, resource


@op(
    name="load_and_modify_facilities",
    description="Load & Modify Facilities Data",
)
def load_and_modify_facilities(context):
    """Op: Load & Modify Facilities Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@op(
    name="geocode_facilities_here",
    description="Geocode Facilities (HERE)",
)
def geocode_facilities_here(context):
    """Op: Geocode Facilities (HERE)"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@op(
    name="calculate_distance_to_public_transport",
    description="Calculate Distance to Public Transport",
)
def calculate_distance_to_public_transport(context):
    """Op: Calculate Distance to Public Transport"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@op(
    name="calculate_distance_to_residential_areas",
    description="Calculate Distance to Residential Areas",
)
def calculate_distance_to_residential_areas(context):
    """Op: Calculate Distance to Residential Areas"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@op(
    name="save_facilities_accessibility_csv",
    description="Save Final Facility Accessibility Data",
)
def save_facilities_accessibility_csv(context):
    """Op: Save Final Facility Accessibility Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@resource
def fs_data_dir_resource(_):
    """Placeholder resource for filesystem data directory."""
    return "/tmp/data"


@job(
    name="load_and_modify_facilities_pipeline",
    description="No description provided.",
    executor_def=DockerExecutor(),
    resource_defs={"fs_data_dir": fs_data_dir_resource},
    io_manager_defs={"fs_io_manager": fs_io_manager},
)
def load_and_modify_facilities_pipeline():
    load = load_and_modify_facilities()
    geocode = geocode_facilities_here()
    distance_pt = calculate_distance_to_public_transport()
    distance_res = calculate_distance_to_residential_areas()
    save = save_facilities_accessibility_csv()

    load >> geocode >> distance_pt >> distance_res >> save