from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name='reconcile_supplier_names', retries=1)
def reconcile_supplier_names():
    """Task: Entity Reconciliation (Wikidata)"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@task(name='save_final_data', retries=1)
def save_final_data():
    """Task: Save Final Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@flow(
    name="procurement_supplier_validation_pipeline",
    task_runner=SequentialTaskRunner()
)
def procurement_supplier_validation_pipeline():
    """Main flow for the Procurement Supplier Validation Pipeline."""
    load_and_modify_data()
    reconcile_supplier_names()
    save_final_data()


if __name__ == "__main__":
    procurement_supplier_validation_pipeline()