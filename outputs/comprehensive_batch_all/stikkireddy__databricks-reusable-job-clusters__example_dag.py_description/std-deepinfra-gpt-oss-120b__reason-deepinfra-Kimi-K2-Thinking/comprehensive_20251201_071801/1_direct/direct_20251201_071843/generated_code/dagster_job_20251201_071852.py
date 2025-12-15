from dagster import op, job, In, Out, Nothing, Output, DynamicOut, DynamicOutput, ConfigurableResource, ResourceDefinition


class DatabricksResource(ConfigurableResource):
    """Placeholder resource for Databricks interactions."""

    def run_notebook(self, notebook_path: str, cluster_id: str) -> None:
        """Simulate running a Databricks notebook."""
        print(f"Running notebook '{notebook_path}' on cluster '{cluster_id}'.")


@op(required_resource_keys={"databricks"})
def start_task() -> Nothing:
    """Entry point of the pipeline."""
    print("Pipeline started.")
    return None


@op(required_resource_keys={"databricks"})
def notebook_task() -> Nothing:
    """Execute the first Databricks notebook."""
    databricks: DatabricksResource = op.context.resources.databricks
    databricks.run_notebook(notebook_path="/Workspace/helloworld", cluster_id="existing_cluster_id")
    return None


@op
def dummy_task_1() -> Nothing:
    """A dummy task after the first notebook."""
    print("Executing dummy_task_1.")
    return None


@op(out={"branch": Out(str)})
def branch_task() -> str:
    """Determine the next step; always selects 'dummy_task_3'."""
    # In a real scenario, this could evaluate complex logic.
    branch = "dummy_task_3"
    print(f"branch_task selected branch: {branch}")
    return branch


@op
def dummy_task_3() -> Nothing:
    """Dummy task that runs in parallel after branching."""
    print("Executing dummy_task_3.")
    return None


@op(required_resource_keys={"databricks"})
def notebook_task_2() -> Nothing:
    """Execute the second Databricks notebook."""
    databricks: DatabricksResource = op.context.resources.databricks
    databricks.run_notebook(notebook_path="/Workspace/helloworld", cluster_id="existing_cluster_id")
    return None


@op
def dummy_task_2() -> Nothing:
    """Dummy task that runs after notebook_task_2."""
    print("Executing dummy_task_2.")
    return None


@op
def end_task() -> Nothing:
    """Final task marking pipeline completion."""
    print("Pipeline completed.")
    return None


@job(
    resource_defs={"databricks": DatabricksResource()},
)
def databricks_pipeline_job():
    """Dagster job mirroring the described Airflow pipeline."""
    start = start_task()
    nb1 = notebook_task()
    dt1 = dummy_task_1()
    branch = branch_task()

    # Parallel execution after branching
    dt3 = dummy_task_3()
    nb2 = notebook_task_2()

    # Ensure dummy_task_3 runs after the branch decision
    dt3.after(branch)

    # dummy_task_2 runs after notebook_task_2 completes
    dt2 = dummy_task_2()
    dt2.after(nb2)

    # End task runs after dummy_task_2
    end = end_task()
    end.after(dt2)


if __name__ == "__main__":
    result = databricks_pipeline_job.execute_in_process()
    assert result.success