from prefect import flow, task, get_run_logger


@task
def start_task() -> None:
    """Entry point dummy task."""
    logger = get_run_logger()
    logger.info("Starting pipeline.")


@task
def notebook_task() -> None:
    """Execute the helloworld notebook on an existing Databricks cluster."""
    logger = get_run_logger()
    logger.info("Running notebook_task on Databricks cluster (helloworld notebook).")
    # Placeholder for actual Databricks execution logic.
    # Example: client = DatabricksClient(...); client.run_notebook(...)


@task
def dummy_task_1() -> None:
    """Intermediate dummy task."""
    logger = get_run_logger()
    logger.info("Executing dummy_task_1.")


@task
def branch_task() -> str:
    """Determine the next step; always selects dummy_task_3."""
    logger = get_run_logger()
    logger.info("Evaluating branch condition.")
    # In this pipeline the branch always selects dummy_task_3.
    selected_branch = "dummy_task_3"
    logger.info(f"Branch selected: {selected_branch}")
    return selected_branch


@task
def dummy_task_3() -> None:
    """Dummy task executed in parallel after branching."""
    logger = get_run_logger()
    logger.info("Executing dummy_task_3 (parallel branch).")


@task
def notebook_task_2() -> None:
    """Execute the helloworld notebook again on the same Databricks cluster."""
    logger = get_run_logger()
    logger.info("Running notebook_task_2 on Databricks cluster (helloworld notebook).")
    # Placeholder for actual Databricks execution logic.


@task
def dummy_task_2() -> None:
    """Task that runs after notebook_task_2 completes."""
    logger = get_run_logger()
    logger.info("Executing dummy_task_2 after notebook_task_2.")


@task
def end_task() -> None:
    """Final task marking pipeline completion."""
    logger = get_run_logger()
    logger.info("Pipeline completed successfully.")


@flow
def databricks_pipeline_flow() -> None:
    """
    Prefect flow orchestrating the described pipeline.

    Manual trigger only; no schedule configured.
    """
    # Linear execution up to the branching point.
    start_task()
    notebook_task()
    dummy_task_1()
    branch = branch_task()

    # Branch always leads to dummy_task_3; execute parallel tasks.
    if branch == "dummy_task_3":
        dummy3_future = dummy_task_3.submit()
        notebook2_future = notebook_task_2.submit()

        # dummy_task_2 runs after notebook_task_2 finishes.
        dummy2 = dummy_task_2(wait_for=[notebook2_future])

        # End task waits for both parallel branches to finish.
        end_task(wait_for=[dummy2, dummy3_future])
    else:
        # Placeholder for alternative branches if needed in the future.
        end_task()


if __name__ == "__main__":
    databricks_pipeline_flow()