from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    ResourceParam,
    Definitions,
    In,
    Out,
    Nothing,
)
from typing import List
import time
import logging


# Simplified Databricks resource stub
class DatabricksResource:
    """Stub Databricks resource that simulates notebook execution."""

    def __init__(self, host: str, token: str):
        self.host = host
        self.token = token
        self.logger = logging.getLogger(__name__)

    def submit_notebook(self, notebook_path: str, cluster_id: str) -> str:
        """Simulate submitting a notebook to Databricks."""
        self.logger.info(
            f"Submitting notebook '{notebook_path}' to cluster '{cluster_id}'"
        )
        # Simulate execution time
        time.sleep(2)
        run_id = f"run_{int(time.time())}"
        self.logger.info(f"Notebook execution completed with run_id: {run_id}")
        return run_id


# Configuration classes
class NotebookConfig(Config):
    notebook_path: str
    cluster_id: str


class BranchConfig(Config):
    """Configuration for branch logic."""
    # In this pipeline, branch always returns the same path
    always_return: str = "dummy_task_3_path"


# Resources configuration
DATABRICKS_HOST = "https://your-databricks-instance.cloud.databricks.com"
DATABRICKS_TOKEN = "your-databricks-token"  # In production, use secrets
EXISTING_CLUSTER_ID = "your-existing-cluster-id"


# Ops
@op(description="Pipeline entry point")
def start_task(context: OpExecutionContext) -> None:
    """Dummy start task."""
    context.log.info("Pipeline started")


@op(description="Execute Databricks notebook")
def notebook_task(
    context: OpExecutionContext,
    databricks: ResourceParam[DatabricksResource],
    config: NotebookConfig,
) -> None:
    """Execute a Databricks notebook."""
    context.log.info(f"Executing notebook: {config.notebook_path}")
    run_id = databricks.submit_notebook(config.notebook_path, config.cluster_id)
    context.log.info(f"Notebook executed successfully: {run_id}")


@op(description="Dummy task 1")
def dummy_task_1(context: OpExecutionContext, _prev: None) -> None:
    """Placeholder task between notebook and branch."""
    context.log.info("Dummy task 1 completed")


@op(description="Branch task that determines execution path")
def branch_task(
    context: OpExecutionContext, config: BranchConfig, _prev: None
) -> List[str]:
    """Evaluate branch logic and return downstream task IDs."""
    context.log.info("Evaluating branch logic")
    # In this pipeline, branch always selects the same path
    # Return list of task IDs to run in parallel
    downstream_tasks = [config.always_return, "notebook_task_2"]
    context.log.info(f"Branch selected tasks: {downstream_tasks}")
    return downstream_tasks


@op(description="Dummy task 3 - branch destination")
def dummy_task_3(context: OpExecutionContext, _prev: None) -> None:
    """Placeholder task from branch."""
    context.log.info("Dummy task 3 completed")


@op(description="Execute Databricks notebook in parallel")
def notebook_task_2(
    context: OpExecutionContext,
    databricks: ResourceParam[DatabricksResource],
    config: NotebookConfig,
    _prev: None,
) -> None:
    """Execute second Databricks notebook in parallel path."""
    context.log.info(f"Executing notebook: {config.notebook_path}")
    run_id = databricks.submit_notebook(config.notebook_path, config.cluster_id)
    context.log.info(f"Notebook executed successfully: {run_id}")


@op(description="Dummy task 2 after notebook")
def dummy_task_2(context: OpExecutionContext, _prev: None) -> None:
    """Placeholder task after second notebook."""
    context.log.info("Dummy task 2 completed")


@op(description="Pipeline completion marker")
def end_task(context: OpExecutionContext, _prev1: None, _prev2: None) -> None:
    """Final task marking pipeline completion."""
    context.log.info("Pipeline completed successfully")


# Job definition
@job(
    description="Databricks notebook orchestration pipeline with conditional branching",
    resource_defs={
        "databricks": DatabricksResource(
            host=DATABRICKS_HOST, token=DATABRICKS_TOKEN
        )
    },
)
def databricks_orchestration_job():
    # Start the pipeline
    start = start_task()
    
    # Execute first notebook
    notebook1 = notebook_task(
        config=NotebookConfig(
            notebook_path="/Shared/helloworld",
            cluster_id=EXISTING_CLUSTER_ID,
        )
    )
    start >> notebook1
    
    # Dummy task after first notebook
    dummy1 = dummy_task_1(notebook1)
    
    # Branch task
    branch = branch_task(
        config=BranchConfig(always_return="dummy_task_3"),
        _prev=dummy1,
    )
    
    # Parallel tasks after branch
    dummy3 = dummy_task_3(branch)
    notebook2 = notebook_task_2(
        config=NotebookConfig(
            notebook_path="/Shared/helloworld",
            cluster_id=EXISTING_CLUSTER_ID,
        ),
        _prev=branch,
    )
    
    # Task after second notebook
    dummy2 = dummy_task_2(notebook2)
    
    # End task depends on both parallel paths
    end_task(dummy3, dummy2)


# Launch pattern
if __name__ == "__main__":
    result = databricks_orchestration_job.execute_in_process()
    assert result.success