from typing import Any, Dict

from dagster import Config, In, Out, OpExecutionContext, op, job, Field, String


class NotebookConfig(Config):
    notebook_path: str = Field(
        default="helloworld",
        description="Path to the Databricks notebook to execute.",
    )
    cluster_id: str = Field(
        default="existing_cluster_id",
        description="ID of the existing Databricks cluster.",
    )
    # In a real deployment you would include authentication fields, etc.


@op
def start_task(context: OpExecutionContext) -> None:
    """Entry point of the pipeline."""
    context.log.info("Pipeline started.")


@op(config_schema=NotebookConfig)
def notebook_task(context: OpExecutionContext, config: NotebookConfig) -> str:
    """Simulates execution of a Databricks notebook."""
    context.log.info(
        f"Running notebook '{config.notebook_path}' on cluster '{config.cluster_id}'."
    )
    # Placeholder for real Databricks API call.
    return f"notebook_{config.notebook_path}_completed"


@op
def dummy_task_1(context: OpExecutionContext) -> None:
    """A placeholder dummy task."""
    context.log.info("Executing dummy_task_1.")


@op
def branch_task(context: OpExecutionContext) -> str:
    """Evaluates a branching function; always selects dummy_task_3."""
    # In the original Airflow DAG this would be a Python callable.
    # Here we simply return the name of the selected downstream task.
    selected = "dummy_task_3"
    context.log.info(f"Branch decision: selected '{selected}'.")
    return selected


@op
def dummy_task_3(context: OpExecutionContext) -> None:
    """A dummy task that runs in parallel after branching."""
    context.log.info("Executing dummy_task_3.")


@op(config_schema=NotebookConfig)
def notebook_task_2(context: OpExecutionContext, config: NotebookConfig) -> str:
    """Second notebook execution, runs in parallel with dummy_task_3."""
    context.log.info(
        f"Running notebook '{config.notebook_path}' on cluster '{config.cluster_id}'."
    )
    # Placeholder for real Databricks API call.
    return f"notebook_{config.notebook_path}_2_completed"


@op
def dummy_task_2(context: OpExecutionContext) -> None:
    """Runs after notebook_task_2 completes."""
    context.log.info("Executing dummy_task_2.")


@op
def end_task(context: OpExecutionContext) -> None:
    """Marks the end of the pipeline."""
    context.log.info("Pipeline completed.")


@job
def databricks_branching_job():
    """Dagster job mirroring the described Airflow pipeline."""
    start = start_task()
    nb1 = notebook_task()
    dt1 = dummy_task_1()
    branch = branch_task()

    # Parallel branch: dummy_task_3 and notebook_task_2
    dt3 = dummy_task_3()
    nb2 = notebook_task_2()

    dt2 = dummy_task_2()
    end = end_task()

    # Define dependencies
    start >> nb1 >> dt1 >> branch
    branch >> dt3
    branch >> nb2
    nb2 >> dt2 >> end
    dt3 >> end


if __name__ == "__main__":
    result = databricks_branching_job.execute_in_process()
    if result.success:
        print("Job executed successfully.")
    else:
        print("Job failed.")