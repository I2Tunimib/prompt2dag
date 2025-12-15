from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name="load_and_modify_data", retries=1)
def load_and_modify_data():
    """Task: Load and Modify Supplier Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name="reconcile_supplier_names", retries=1)
def reconcile_supplier_names():
    """Task: Entity Reconciliation (Wikidata)"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@task(name="save_validated_data", retries=1)
def save_validated_data():
    """Task: Save Final Validated Supplier Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@flow(
    name="load_and_modify_data_pipeline",
    task_runner=SequentialTaskRunner(),
)
def load_and_modify_data_pipeline():
    """Sequential pipeline to load, reconcile, and save supplier data."""
    # Step 1: Load and modify data
    load_result = load_and_modify_data()

    # Step 2: Reconcile supplier names (depends on load_and_modify_data)
    reconcile_result = reconcile_supplier_names(wait_for=[load_result])

    # Step 3: Save validated data (depends on reconcile_supplier_names)
    save_validated_data(wait_for=[reconcile_result])

    # Optionally return final result
    return {"status": "pipeline completed"}


if __name__ == "__main__":
    load_and_modify_data_pipeline()