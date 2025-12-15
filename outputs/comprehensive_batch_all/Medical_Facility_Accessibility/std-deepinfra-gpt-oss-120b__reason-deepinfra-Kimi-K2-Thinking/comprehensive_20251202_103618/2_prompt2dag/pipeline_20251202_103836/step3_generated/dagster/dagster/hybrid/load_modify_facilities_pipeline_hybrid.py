from dagster import op, job, fs_io_manager, ResourceDefinition
try:
    from dagster_docker import docker_executor
except ImportError:  # pragma: no cover
    # Fallback to the default in‑process executor if dagster_docker is unavailable
    from dagster import executor

    @executor
    def docker_executor(_):
        return None


@op(
    name="load_modify_facilities",
    description="Load and Modify Facilities Data",
)
def load_modify_facilities(context):
    """Op: Load and Modify Facilities Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@op(
    name="geocode_facilities",
    description="Geocode Facilities Using HERE API",
)
def geocode_facilities(context):
    """Op: Geocode Facilities Using HERE API"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@op(
    name="calculate_distance_to_public_transport",
    description="Calculate Distance to Nearest Public Transport",
)
def calculate_distance_to_public_transport(context):
    """Op: Calculate Distance to Nearest Public Transport"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@op(
    name="calculate_distance_to_residential_areas",
    description="Calculate Distance to Nearest Residential Area",
)
def calculate_distance_to_residential_areas(context):
    """Op: Calculate Distance to Nearest Residential Area"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@op(
    name="save_facilities_accessibility",
    description="Save Enriched Facility Accessibility Data",
)
def save_facilities_accessibility(context):
    """Op: Save Enriched Facility Accessibility Data"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@job(
    name="load_modify_facilities_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "data_dir_fs": ResourceDefinition.hardcoded_resource({}),
    },
)
def load_modify_facilities_pipeline():
    """Sequential pipeline wiring the provided ops."""
    load = load_modify_facilities()
    geo = geocode_facilities()
    dist_pub = calculate_distance_to_public_transport()
    dist_res = calculate_distance_to_residential_areas()
    save = save_facilities_accessibility()

    # Enforce sequential execution order
    load >> geo >> dist_pub >> dist_res >> save