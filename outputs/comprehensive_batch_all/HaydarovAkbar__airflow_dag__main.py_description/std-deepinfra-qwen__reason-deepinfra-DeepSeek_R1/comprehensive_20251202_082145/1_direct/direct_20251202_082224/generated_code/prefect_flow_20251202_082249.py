from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import time

# Mock imports for demonstration purposes
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailSender
from prefect_sensors import SqlSensor, ExternalTaskSensor
from prefect_triggers import TriggerDagRunOperator

# Constants
SQL_ALCHEMY_CONNECTOR = "your_sql_alchemy_connector"
EMAIL_SENDER = "your_email_sender"
SAP_HTTP_CONNECTION = "your_sap_http_connection"
POSTGRESQL_DWH_CONNECTION = "your_postgresql_dwh_connection"

# Task definitions
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def wait_for_l2_full_load():
    """SqlSensor monitors for successful L1 to L2 data load completion."""
    with SqlAlchemyConnector.load(SQL_ALCHEMY_CONNECTOR) as database:
        query = "SELECT * FROM metadata_table WHERE flag = 'L2_full_load_complete'"
        sensor = SqlSensor(query=query, database=database, poke_interval=60, timeout=100)
        sensor.run()

@task
def get_load_id():
    """Retrieves a unique load identifier for session tracking."""
    with SqlAlchemyConnector.load(SQL_ALCHEMY_CONNECTOR) as database:
        query = "SELECT nextval('load_id_seq')"
        result = database.execute(query).fetchone()
        return result[0]

@task
def workflow_registration(load_id: int):
    """Registers the workflow session in metadata tables and initiates logging."""
    with SqlAlchemyConnector.load(SQL_ALCHEMY_CONNECTOR) as database:
        query = f"INSERT INTO workflow_sessions (load_id, status) VALUES ({load_id}, 'started')"
        database.execute(query)

@task
def wait_for_success_end():
    """ExternalTaskSensor waits for the previous day's execution of this same DAG to complete successfully."""
    sensor = ExternalTaskSensor(
        external_dag_id="your_dag_id",
        external_task_id="end",
        execution_delta=timedelta(days=1),
        timeout=86400,
        poke_interval=60
    )
    sensor.run()

@task
def run_sys_kill_all_session_pg():
    """Triggers a system utility DAG to kill all PostgreSQL sessions."""
    trigger = TriggerDagRunOperator(
        task_id="run_sys_kill_all_session_pg",
        trigger_dag_id="sys_kill_all_session_pg"
    )
    trigger.run()

@task
def run_wf_data_preparation_for_reports():
    """Launches data preparation workflow for reports."""
    trigger = TriggerDagRunOperator(
        task_id="run_wf_data_preparation_for_reports",
        trigger_dag_id="wf_data_preparation_for_reports"
    )
    trigger.run()

@task
def load_ds_client_segmentation():
    """Triggers client segmentation data loading workflow."""
    trigger = TriggerDagRunOperator(
        task_id="load_ds_client_segmentation",
        trigger_dag_id="l1_to_l2_p_load_data_ds_client_segmentation_full"
    )
    trigger.run()

@task
def send_flg_to_sap(load_id: int):
    """Sends completion flag to SAP system with row count from segmentation results."""
    with SqlAlchemyConnector.load(SQL_ALCHEMY_CONNECTOR) as database:
        query = f"SELECT COUNT(*) FROM segmentation_results WHERE load_id = {load_id}"
        row_count = database.execute(query).fetchone()[0]
        # Mock HTTP request to SAP
        # requests.post(SAP_HTTP_CONNECTION, json={"load_id": load_id, "row_count": row_count})

@task
def end(load_id: int):
    """Updates metadata tables to mark the workflow session as successfully completed."""
    with SqlAlchemyConnector.load(SQL_ALCHEMY_CONNECTOR) as database:
        query = f"UPDATE workflow_sessions SET status = 'completed' WHERE load_id = {load_id}"
        database.execute(query)

@task
def email_on_failure(failure_message: str):
    """Sends failure notification if any upstream task fails."""
    email_sender = EmailSender(email_sender=EMAIL_SENDER)
    email_sender.send_email(subject="Pipeline Failure", message=failure_message)

# Flow definition
@flow(name="L2 Data Load and Segmentation Pipeline")
def l2_data_load_pipeline():
    try:
        wait_for_l2_full_load()
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end()
        run_sys_kill_all_session_pg()

        # Parallel execution
        data_prep_future = run_wf_data_preparation_for_reports.submit()
        segmentation_future = load_ds_client_segmentation.submit()
        send_flg_to_sap_future = send_flg_to_sap.submit(segmentation_future)

        # Wait for parallel tasks to complete
        data_prep_future.result()
        send_flg_to_sap_future.result()

        end(load_id)
    except Exception as e:
        email_on_failure(f"Pipeline failed with error: {str(e)}")

if __name__ == "__main__":
    l2_data_load_pipeline()