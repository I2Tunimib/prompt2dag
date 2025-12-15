from dagster import op, job, resource
from typing import Any, Dict


@resource
def databricks_config() -> Dict[str, Any]:
    """Resource providing Databricks configuration."""
    return {
        "existing_cluster_id": "existing-cluster-id",
        "connection_id": "databricks_default",
        "node_type_id": "n2-highmem-4",
        "spark_version": "12.2.x-scala2.12",
        "delta_preview_enabled": True,
    }


@op
def start_task_op() -> None:
    """Dummy start task as pipeline entry point."""
    pass


@op(required_resource_keys={"databricks"})
def notebook_task_op(context, _prev: None) -> bool:
    """Execute helloworld notebook on existing Databricks cluster."""
    config = context.resources.databricks
    context.log.info(
        f"Submitting notebook 'helloworld' to Databricks cluster "
        f"{config['existing_cluster_id']}"
    )
    return True


@op
def dummy_task_1_op(_prev: None) -> None:
    """Dummy task 1."""
    pass


@op
def branch_task_op(_prev: None) -> str:
    """Branch task that evaluates path selection.
    
    Always selects dummy_task_3 path based on branch_func logic.
    """
    return "dummy_task_3"


@op
def dummy_task_3_op(_branch_decision: str) -> None:
    """Dummy task 3 executed in parallel path."""
    pass


@op(required_resource_keys={"databricks"})
def notebook_task_2_op(context, _branch_decision: str) -> bool:
    """Execute helloworld notebook on Databricks in parallel path."""
    config = context.resources.databricks
    context.log.info(
        f"Submitting notebook 'helloworld' to Databricks cluster "
        f"{config['existing_cluster_id']} (parallel execution)"
    )
    return True


@op
def dummy_task_2_op(_notebook_result: bool) -> None:
    """Dummy task 2 triggered after notebook_task_2 completion."""
    pass


@op
def end_task_op(_from_dummy3: None, _from_dummy2: None) -> None:
    """Final end task marking pipeline completion."""
    pass


@job(resource_defs={"databricks": databricks_config})
def databricks_pipeline_job() -> None:
    """Databricks pipeline with conditional branching and parallel execution."""
    start = start_task_op()
    notebook = notebook_task_op(start)
    dummy1 = dummy_task_1_op(notebook)
    
    branch_decision = branch_task_op(dummy1)
    
    dummy3 = dummy_task_3_op(branch_decision)
    notebook2 = notebook_task_2_op(branch_decision)
    dummy2 = dummy_task_2_op(notebook2)
    
    end_task_op(dummy3, dummy2)


if __name__ == "__main__":
    result = databricks_pipeline_job.execute_in_process()