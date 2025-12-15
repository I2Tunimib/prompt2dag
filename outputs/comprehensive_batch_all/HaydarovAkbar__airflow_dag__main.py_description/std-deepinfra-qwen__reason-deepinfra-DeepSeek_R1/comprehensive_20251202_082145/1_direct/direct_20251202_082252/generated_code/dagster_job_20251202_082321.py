from dagster import op, job, resource, RetryPolicy, Failure, Field, String, Int, Any, In, Out, graph
from datetime import timedelta
import pendulum

# Resources
@resource(config_schema={"smtp_server": Field(String, default_value="smtp.example.com"),
                         "smtp_port": Field(Int, default_value=587),
                         "smtp_user": Field(String, default_value="user"),
                         "smtp_password": Field(String, default_value="password"),
                         "recipients": Field(String, default_value="admin@example.com")})
def email_resource(context):
    return {
        "smtp_server": context.resource_config["smtp_server"],
        "smtp_port": context.resource_config["smtp_port"],
        "smtp_user": context.resource_config["smtp_user"],
        "smtp_password": context.resource_config["smtp_password"],
        "recipients": context.resource_config["recipients"]
    }

@resource(config_schema={"db_url": Field(String, default_value="postgresql://user:password@localhost:5432/dwh")})
def postgres_resource(context):
    return context.resource_config["db_url"]

@resource(config_schema={"sap_url": Field(String, default_value="http://sap.example.com/api/notify")})
def sap_resource(context):
    return context.resource_config["sap_url"]

# Ops
@op(required_resource_keys={"postgres"})
def wait_for_l2_full_load(context):
    """SqlSensor monitors for successful L1 to L2 data load completion by checking a flag in the metadata table."""
    db_url = context.resources.postgres
    # Simulate SQL check
    context.log.info(f"Checking L2 full load completion in {db_url}")
    # Placeholder for actual SQL check
    return True

@op
def get_load_id(context):
    """PythonOperator retrieves a unique load identifier for session tracking."""
    load_id = "unique_load_id_12345"
    context.log.info(f"Retrieved load ID: {load_id}")
    return load_id

@op(required_resource_keys={"postgres"})
def workflow_registration(context, load_id):
    """PythonOperator registers the workflow session in metadata tables and initiates logging."""
    db_url = context.resources.postgres
    context.log.info(f"Registering workflow session {load_id} in {db_url}")
    # Placeholder for actual registration
    return True

@op
def wait_for_success_end(context):
    """ExternalTaskSensor waits for the previous day's execution of this same DAG to complete successfully."""
    context.log.info("Waiting for previous day's execution to complete")
    # Placeholder for actual wait
    return True

@op
def run_sys_kill_all_session_pg(context):
    """TriggerDagRunOperator triggers a system utility DAG to kill all PostgreSQL sessions."""
    context.log.info("Triggering sys_kill_all_session_pg DAG")
    # Placeholder for actual trigger
    return True

@op
def run_wf_data_preparation_for_reports(context):
    """TriggerDagRunOperator launches data preparation workflow for reports."""
    context.log.info("Triggering wf_data_preparation_for_reports DAG")
    # Placeholder for actual trigger
    return True

@op(required_resource_keys={"postgres"})
def load_ds_client_segmentation(context):
    """TriggerDagRunOperator triggers client segmentation data loading workflow."""
    db_url = context.resources.postgres
    context.log.info(f"Triggering client segmentation data loading workflow in {db_url}")
    # Placeholder for actual trigger
    return True

@op(required_resource_keys={"sap"})
def send_flg_to_sap(context, row_count):
    """PythonOperator sends completion flag to SAP system with row count from segmentation results."""
    sap_url = context.resources.sap
    context.log.info(f"Sending completion flag to SAP at {sap_url} with row count {row_count}")
    # Placeholder for actual HTTP request
    return True

@op(required_resource_keys={"postgres"})
def end(context, load_id):
    """PythonOperator updates metadata tables to mark the workflow session as successfully completed."""
    db_url = context.resources.postgres
    context.log.info(f"Marking workflow session {load_id} as completed in {db_url}")
    # Placeholder for actual update
    return True

@op(required_resource_keys={"email"})
def email_on_failure(context, error):
    """EmailOperator sends failure notification if any upstream task fails."""
    email_config = context.resources.email
    context.log.error(f"Sending failure notification to {email_config['recipients']}")
    # Placeholder for actual email sending
    return True

# Job
@job(
    resource_defs={
        "postgres": postgres_resource,
        "sap": sap_resource,
        "email": email_resource
    },
    tags={
        "pipeline": "L2 Segmentation and SAP Integration",
        "timezone": "Asia/Tashkent",
        "max_concurrent_runs": 20,
        "retry_policy": RetryPolicy(max_retries=3, delay=60),
        "on_failure": email_on_failure
    },
    description="Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration."
)
def l2_segmentation_and_sap_integration():
    l2_full_load = wait_for_l2_full_load()
    load_id = get_load_id()
    workflow_registered = workflow_registration(load_id)
    success_end = wait_for_success_end()
    sys_kill_all_session_pg = run_sys_kill_all_session_pg()

    data_preparation = run_wf_data_preparation_for_reports()
    client_segmentation = load_ds_client_segmentation()
    sap_notification = send_flg_to_sap(client_segmentation)

    end_op = end(load_id)

    l2_full_load >> workflow_registered >> success_end >> sys_kill_all_session_pg >> [data_preparation, client_segmentation] >> sap_notification >> end_op

if __name__ == '__main__':
    result = l2_segmentation_and_sap_integration.execute_in_process()