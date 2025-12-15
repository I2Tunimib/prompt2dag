from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator


def branch_func(**kwargs):
    """
    Branch function that determines the next tasks to execute.
    Returns a list of task IDs to run in parallel after the branch point.
    """
    return ['dummy_task_3', 'notebook_task_2']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    dag_id='databricks_conditional_pipeline',
    description='Orchestrates Databricks notebook execution with conditional branching and cluster reuse',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['databricks', 'branching', 'cluster-reuse'],
) as dag:

    start_task = DummyOperator(task_id='start_task')

    notebook_task = DatabricksSubmitRunOperator(
        task_id='notebook_task',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1234-567890-abcde123',
        notebook_task={
            'notebook_path': '/Shared/helloworld',
        },
    )

    dummy_task_1 = DummyOperator(task_id='dummy_task_1')

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
    )

    dummy_task_3 = DummyOperator(task_id='dummy_task_3')

    notebook_task_2 = DatabricksSubmitRunOperator(
        task_id='notebook_task_2',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1234-567890-abcde123',
        notebook_task={
            'notebook_path': '/Shared/helloworld',
        },
    )

    dummy_task_2 = DummyOperator(task_id='dummy_task_2')

    end_task = DummyOperator(task_id='end_task')

    start_task >> notebook_task >> dummy_task_1 >> branch_task
    branch_task >> dummy_task_3 >> end_task
    branch_task >> notebook_task_2 >> dummy_task_2 >> end_task