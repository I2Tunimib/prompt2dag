from prefect import flow, task, get_run_logger


@task
def start_task() -> None:
    """Entry point dummy task."""
    logger = get_run_logger()
    logger.info("Start task")


@task
def notebook_task() -> str:
    """Run the helloworld notebook on an existing Databricks cluster."""
    logger = get_run_logger()
    logger.info(
        "Executing helloworld notebook on existing Databricks cluster (notebook_task)"
    )
    # Placeholder for actual Databricks run logic.
    return "notebook_task_done"


@task
def dummy_task_1() -> None:
    """Second dummy task."""
    logger = get_run_logger()
    logger.info("Dummy task 1")


@task
def branch_task() -> str:
    """Evaluate branching logic; always selects dummy_task_3."""
    logger = get_run_logger()
    logger.info("Evaluating branch condition")
    # In the original Airflow DAG the branch always resolves to dummy_task_3.
    return "dummy_task_3"


@task
def dummy_task_3() -> None:
    """Parallel dummy task executed after branching."""
    logger = get_run_logger()
    logger.info("Dummy task 3 (parallel branch)")


@task
def notebook_task_2() -> str:
    """Run the helloworld notebook a second time on the same cluster."""
    logger = get_run_logger()
    logger.info(
        "Executing helloworld notebook on existing Databricks cluster (notebook_task_2)"
    )
    # Placeholder for actual Databricks run logic.
    return "notebook_task_2_done"


@task
def dummy_task_2(notebook_result: str) -> None:
    """Task that runs after notebook_task_2 completes."""
    logger = get_run_logger()
    logger.info("Dummy task 2 after notebook_task_2")
    # notebook_result is not used further; it only creates the dependency.


@task
def end_task(dummy2_result: None, dummy3_result: None) -> None:
    """Final task marking pipeline completion."""
    logger = get_run_logger()
    logger.info("End task – pipeline completed")


@flow
def databricks_pipeline() -> None:
    """
    Prefect flow replicating the described Airflow DAG.

    Manual trigger only – no schedule configured.
    """
    start_task()
    notebook_task()
    dummy_task_1()
    branch = branch_task()

    # The branch always selects dummy_task_3, but we keep the variable for clarity.
    if branch == "dummy_task_3":
        # Run dummy_task_3 and notebook_task_2 concurrently.
        dummy3_future = dummy_task_3.submit()
        notebook2_future = notebook_task_2.submit()

        # dummy_task_2 must wait for notebook_task_2 to finish.
        dummy2_future = dummy_task_2.submit(notebook2_future)

        # End task waits for both parallel branches to finish.
        end_task.submit(dummy2_future, dummy3_future)
    else:
        # Fallback path (not used in this pipeline).
        logger = get_run_logger()
        logger.warning("Unexpected branch result: %s", branch)


if __name__ == "__main__":
    databricks_pipeline()