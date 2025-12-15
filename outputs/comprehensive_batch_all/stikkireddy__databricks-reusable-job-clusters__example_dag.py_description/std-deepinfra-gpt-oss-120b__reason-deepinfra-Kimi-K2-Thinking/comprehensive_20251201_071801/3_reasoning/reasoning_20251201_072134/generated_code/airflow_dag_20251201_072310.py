"""
Databricks Conditional Pipeline DAG.

Orchestrates Databricks notebook execution with conditional branching logic
and cluster reuse capabilities. Features manual triggering, parallel execution
paths, and custom cluster management.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator


class AirflowDBXClusterReuseBuilder(DatabricksSubmitRunOperator):
    """
    Custom Databricks operator for cluster reuse optimization.
    
    Supports both existing clusters and job cluster creation with
    standardized configuration for cluster reuse patterns.
    """
    
    def __init__(self, *args, **kwargs):
        # Apply default job cluster config if no cluster specified
        if 'new_cluster' not in kwargs and 'existing_cluster_id' not in kwargs:
            kwargs['new_cluster'] = {
                'spark_version': '12.2.x-scala2.12',
                'node_type_id': 'n2-highmem-4',
                'num_workers': 2,
                'spark_conf': {
                    'spark.databricks.delta.preview.enabled': 'true',
                },
            }
        
        # Set default connection if not specified
        if 'databricks_conn_id' not in kwargs:
            kwargs['databricks_conn_id'] = 'databricks_default'
            
        super().__init__(*args, **kwargs)


def branch_func(**kwargs):
    """
    Branching logic that always selects dummy_task_3 path.
    
    In production, this would contain actual decision logic based on
    upstream task results or external conditions.
    """
    # Always return dummy_task_3 as per pipeline requirements
    return 'dummy_task_3'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # Secrets integration would be configured here in production
    # using Airflow Secrets backend for Databricks host and token
}

with DAG(
    dag_id='databricks_conditional_pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    default_args=default_args,
    tags=['databricks', 'branching', 'cluster-reuse'],
    doc_md=__doc__,
) as dag:
    
    # Pipeline entry point
    start_task = DummyOperator(
        task_id='start_task',
        doc_md='Pipeline entry point marker'
    )
    
    # First Databricks notebook execution on existing cluster
    notebook_task = AirflowDBXClusterReuseBuilder(
        task_id='notebook_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id='existing-cluster-id',  # Configure actual cluster ID
        notebook_task={
            'notebook_path': '/Shared/helloworld',
        },
    )
    
    # Intermediate dummy task
    dummy_task_1 = DummyOperator(task_id='dummy_task_1')
    
    # Branching decision point
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
    )
    
    # Branch path: dummy task (always selected)
    dummy_task_3 = DummyOperator(task_id='dummy_task_3')
    
    # Parallel Databricks notebook execution on existing cluster
    notebook_task_2 = AirflowDBXClusterReuseBuilder(
        task_id='notebook_task_2',
        databricks_conn_id='databricks_default',
        existing_cluster_id='existing-cluster-id',  # Configure actual cluster ID
        notebook_task={
            'notebook_path': '/Shared/helloworld',
        },
    )
    
    # Sequential task after notebook_task_2
    dummy_task_2 = DummyOperator(task_id='dummy_task_2')
    
    # Pipeline completion marker
    end_task = DummyOperator(
        task_id='end_task',
        doc_md='Pipeline completion marker',
        trigger_rule='none_failed',  # Ensure completion if parallel paths succeed
    )
    
    # Define task dependencies
    start_task >> notebook_task >> dummy_task_1 >> branch_task
    branch_task >> dummy_task_3
    branch_task >> notebook_task_2 >> dummy_task_2
    [dummy_task_3, dummy_task_2] >> end_task