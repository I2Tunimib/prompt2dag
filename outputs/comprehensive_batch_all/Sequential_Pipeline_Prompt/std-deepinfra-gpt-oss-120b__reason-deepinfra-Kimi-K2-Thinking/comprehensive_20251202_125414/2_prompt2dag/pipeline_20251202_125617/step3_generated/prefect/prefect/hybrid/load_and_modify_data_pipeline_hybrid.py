from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='load_and_modify_data', retries=1)
def load_and_modify_data():
    """Task: Load and Modify Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-load-and-modify:latest
    pass


@task(name='reconcile_city_names', retries=1)
def reconcile_city_names():
    """Task: Data Reconciliation"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-reconciliation:latest
    pass


@task(name='enrich_with_openmeteo', retries=1)
def enrich_with_openmeteo():
    """Task: OpenMeteo Data Extension"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-openmeteo-extension:latest
    pass


@task(name='extend_columns', retries=1)
def extend_columns():
    """Task: Column Extension"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-column-extension:latest
    pass


@task(name='save_final_data', retries=1)
def save_final_data():
    """Task: Save Final Data"""
    # Docker execution via infrastructure
    # Image: i2t-backendwithintertwino6-save:latest
    pass


@flow(
    name="load_and_modify_data_pipeline",
    task_runner=SequentialTaskRunner(),
)
def load_and_modify_data_pipeline():
    """Sequential pipeline orchestrating data loading, transformation, enrichment, and saving."""
    # Entry point
    load_and_modify_data()
    # Subsequent steps respecting dependencies
    reconcile_city_names()
    enrich_with_openmeteo()
    extend_columns()
    save_final_data()


if __name__ == "__main__":
    load_and_modify_data_pipeline()