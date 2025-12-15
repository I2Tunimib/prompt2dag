from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': 300,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Databricks cluster configuration
existing_cluster_id = 'your_existing_cluster_id'
databricks_conn_id = 'databricks_default'
notebook_path = '/Users/your_username/helloworld'

# DAG definition
with DAG(
    dag_id='databricks_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG for orchestrating Databricks notebook execution with conditional branching',
) as dag:

    # Dummy start task
    start_task = DummyOperator(task_id='start_task')

    # Databricks notebook task
    notebook_task = DatabricksSubmitRunOperator(
        task_id='notebook_task',
        existing_cluster_id=existing_cluster_id,
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': notebook_path,
        },
    )

    # Dummy task 1
    dummy_task_1 = DummyOperator(task_id='dummy_task_1')

    # Branch task
    def branch_func():
        return 'dummy_task_3'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
    )

    # Dummy task 3
    dummy_task_3 = DummyOperator(task_id='dummy_task_3')

    # Databricks notebook task 2
    notebook_task_2 = DatabricksSubmitRunOperator(
        task_id='notebook_task_2',
        existing_cluster_id=existing_cluster_id,
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': notebook_path,
        },
    )

    # Dummy task 2
    dummy_task_2 = DummyOperator(task_id='dummy_task_2')

    # End task
    end_task = DummyOperator(task_id='end_task')

    # Define task dependencies
    start_task >> notebook_task >> dummy_task_1 >> branch_task
    branch_task >> dummy_task_3
    branch_task >> notebook_task_2
    dummy_task_3 >> dummy_task_2
    notebook_task_2 >> dummy_task_2
    dummy_task_2 >> end_task