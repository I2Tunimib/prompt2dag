"""Airflow DAG for orchestrating Databricks notebook execution with branching.

The DAG runs manually (no schedule) and demonstrates:
- Dummy start and end tasks
- Two Databricks notebook executions on an existing cluster
- Branching logic using BranchPythonOperator
- Parallel execution of a dummy task and a notebook task
- Sequential completion after parallel tasks
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Attempt to import the custom cluster reuse builder; if unavailable, define a stub.
try:
    from airflow.providers.databricks.operators.databricks import AirflowDBXClusterReuseBuilder
except ImportError:  # pragma: no cover
    class AirflowDBXClusterReuseBuilder:  # type: ignore
        """Fallback stub for AirflowDBXClusterReuseBuilder."""

        def __init__(self, **kwargs):
            self.config = kwargs

        def to_dict(self):
            return self.config


# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# Cluster reuse configuration (example values)
cluster_reuse_builder = AirflowDBXClusterReuseBuilder(
    existing_cluster_id="my-existing-cluster-id",
    job_cluster={
        "new_cluster": {
            "spark_version": "12.2.x-scala2.12",
            "node_type_id": "n2-highmem-4",
            "num_workers": 2,
            "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
        }
    },
)

# Common notebook task payload
def _notebook_task_payload(notebook_path: str) -> dict:
    """Return the payload for DatabricksSubmitRunOperator."""
    return {
        "existing_cluster_id": "my-existing-cluster-id",
        "notebook_task": {"notebook_path": notebook_path},
        "libraries": [],
        "timeout_seconds": 3600,
        "run_name": f"Run {notebook_path}",
    }


def branch_func(**kwargs) -> str:
    """Determine the next task after branching.

    This example always selects ``dummy_task_3``.
    """
    return "dummy_task_3"


with DAG(
    dag_id="databricks_notebook_branching",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example", "databricks", "branching"],
) as dag:
    # Entry point
    start_task = DummyOperator(task_id="start_task")

    # First notebook execution
    notebook_task = DatabricksSubmitRunOperator(
        task_id="notebook_task",
        json=_notebook_task_payload("/Workspace/helloworld"),
        databricks_conn_id="databricks_default",
        # Include the cluster reuse builder if the operator supports it
        # (the actual parameter name may differ based on the provider version)
        # cluster_builder=cluster_reuse_builder,
    )

    # Simple dummy step
    dummy_task_1 = DummyOperator(task_id="dummy_task_1")

    # Branching decision
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_func,
        provide_context=False,
    )

    # Path selected by the branch (always dummy_task_3 in this example)
    dummy_task_3 = DummyOperator(task_id="dummy_task_3")

    # Second notebook execution (runs in parallel with dummy_task_3)
    notebook_task_2 = DatabricksSubmitRunOperator(
        task_id="notebook_task_2",
        json=_notebook_task_payload("/Workspace/helloworld"),
        databricks_conn_id="databricks_default",
        # cluster_builder=cluster_reuse_builder,
    )

    # Task that waits for both parallel branches
    dummy_task_2 = DummyOperator(task_id="dummy_task_2")

    # End of the pipeline
    end_task = DummyOperator(task_id="end_task")

    # Define the workflow dependencies
    start_task >> notebook_task >> dummy_task_1 >> branch_task
    branch_task >> dummy_task_3
    branch_task >> notebook_task_2

    # Parallel branches converge before dummy_task_2
    dummy_task_3 >> dummy_task_2
    notebook_task_2 >> dummy_task_2

    dummy_task_2 >> end_task