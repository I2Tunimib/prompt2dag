from dagster import op, job, resource, fs_io_manager, DockerExecutor, ResourceDefinition


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
    name="reconcile_supplier_names",
    description="Entity Reconciliation (Wikidata)",
)
def reconcile_supplier_names(context):
    """Op: Entity Reconciliation (Wikidata)"""
    # Docker execution
    # Image: i2t-backendwithintertwino6-reconciliation:latest
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


@resource
def fs_data_dir():
    """Placeholder resource for filesystem data directory."""
    # In a real deployment, this could be configured via environment variables or Dagster config.
    return "/tmp/data"


@job(
    name="procurement_supplier_validation_pipeline",
    description="Validates and standardizes supplier data by reconciling names against a known database (Wikidata) for improved data quality in procurement systems. Ingestes a CSV of basic supplier information, converts it to JSON, reconciles supplier names, and exports the enriched data to CSV.",
    executor_def=DockerExecutor(),
    resource_defs={
        "fs_data_dir": fs_data_dir,
        "io_manager": fs_io_manager,
    },
)
def procurement_supplier_validation_pipeline():
    # Sequential execution: load_and_modify_data -> reconcile_supplier_names -> save_final_data
    load_and_modify_data() >> reconcile_supplier_names() >> save_final_data()