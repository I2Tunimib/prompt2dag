from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    ResourceParam,
    get_dagster_logger,
    Definitions,
    In,
    Out,
    Nothing,
)
import time
from datetime import datetime, timedelta
import pytz

logger = get_dagster_logger()


# Minimal resource stubs (in practice, these would be proper resource classes)
# class PostgresResource:
#     def get_connection(self):
#         pass
#
# class SapResource:
#     def send_flag(self, row_count: int):
#         pass
#
# class EmailResource:
#     def send_failure_email(self, error_msg: str):
#         pass


class PipelineConfig(Config):
    """Configuration for the pipeline."""
    polling_interval: int = 60  # seconds
    max_wait_time: int = 3600  # 1 hour max wait
    timezone: str = "Asia/Tashkent"
    metadata_table: str = "metadata.workflow_status"
    sap_endpoint: str = "https://sap.example.com/notification"
    email_recipients: list[str] = ["data-team@example.com"]


@op(
    description="Monitors for successful L1 to L2 data load completion by checking a flag in the metadata table"
)
def wait_for_l2_full_load(context: OpExecutionContext, config: PipelineConfig) -> bool:
    """Simulates SQL sensor that waits for L1->L2 load completion."""
    # In a real implementation, this would query PostgreSQL:
    # SELECT load_completed FROM metadata.l1_l2_status WHERE date = CURRENT_DATE
    
    logger.info(f"Waiting for L1->L2 load completion (polling every {config.polling_interval}s)")
    
    elapsed = 0
    while elapsed < config.max_wait_time:
        # Simulated check - replace with actual DB query
        # For demo purposes, we'll assume it completes after a short wait
        if elapsed > 30:  # Simulate success after 30 seconds
            logger.info("L1->L2 load completed successfully")
            return True
        
        time.sleep(config.polling_interval)
        elapsed += config.polling_interval
        logger.info(f"Still waiting... elapsed: {elapsed}s")
    
    raise TimeoutError("L1->L2 load did not complete within max wait time")


@op(description="Retrieves a unique load identifier for session tracking")
def get_load_id(context: OpExecutionContext) -> str:
    """Generates a unique load ID."""
    tz = pytz.timezone("Asia/Tashkent")
    timestamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    load_id = f"load_{timestamp}_{context.run_id[:8]}"
    logger.info(f"Generated load ID: {load_id}")
    return load_id


@op(description="Registers the workflow session in metadata tables and initiates logging")
def workflow_registration(context: OpExecutionContext, load_id: str) -> str:
    """Registers workflow session in metadata."""
    # In real implementation, would call md_dwh module:
    # md_dwh.register_workflow(load_id=load_id, workflow_name="l2_to_l2_segmentation")
    
    logger.info(f"Registering workflow session for load_id: {load_id}")
    # Simulate registration
    session_id = f"session_{load_id}"
    return session_id


@op(description="Waits for the previous day's execution of this same DAG to complete successfully")
def wait_for_success_end(context: OpExecutionContext, config: PipelineConfig) -> bool:
    """Waits for previous run completion with 24-hour delta."""
    tz = pytz.timezone(config.timezone)
    today = datetime.now(tz).date()
    previous_run_date = today - timedelta(days=1)
    
    logger.info(f"Waiting for previous day's run (date: {previous_run_date})")
    
    # In real implementation, would query Dagster run history or metadata table
    # SELECT status FROM metadata.workflow_runs WHERE run_date = %s
    
    # Simulate check - assume previous run succeeded after a short wait
    time.sleep(10)
    logger.info("Previous day's run completed successfully")
    return True


@op(description="Triggers a system utility DAG to kill all PostgreSQL sessions")
def run_sys_kill_all_session_pg(context: OpExecutionContext) -> Nothing:
    """Triggers system utility for killing PostgreSQL sessions."""
    logger.info("Triggering sys_kill_all_session_pg utility DAG")
    # In real implementation, would use Dagster's external execution API
    # or trigger another Dagster job via GraphQL API or CLI
    # For now, simulate the action
    time.sleep(5)
    logger.info("PostgreSQL sessions killed (simulated)")


@op(description="Launches data preparation workflow for reports")
def run_wf_data_preparation_for_reports(context: OpExecutionContext) -> Nothing:
    """Triggers data preparation workflow."""
    logger.info("Triggering data preparation workflow for reports")
    # Would trigger external Dagster job or similar
    time.sleep(10)
    logger.info("Data preparation workflow completed (simulated)")


@op(description="Triggers client segmentation data loading workflow")
def load_ds_client_segmentation(context: OpExecutionContext) -> int:
    """Triggers segmentation loading and returns row count."""
    logger.info("Triggering client segmentation data loading workflow")
    # Would trigger external job and wait for completion
    # Then get row count from metadata or the job's output
    time.sleep(15)
    row_count = 15000  # Simulated row count
    logger.info(f"Segmentation loaded {row_count} rows")
    return row_count


@op(description="Sends completion flag to SAP system with row count from segmentation results")
def send_flg_to_sap(context: OpExecutionContext, row_count: int, config: PipelineConfig) -> Nothing:
    """Sends completion notification to SAP."""
    logger.info(f"Sending completion flag to SAP with row count: {row_count}")
    # In real implementation, would use SAP resource:
    # sap_client.send_completion_flag(row_count=row_count, endpoint=config.sap_endpoint)
    time.sleep(5)
    logger.info("SAP notification sent successfully")


@op(description="Updates metadata tables to mark the workflow session as successfully completed")
def end(context: OpExecutionContext, session_id: str) -> Nothing:
    """Marks workflow session as completed."""
    logger.info(f"Marking session {session_id} as successfully completed")
    # Would update metadata table:
    # UPDATE metadata.workflow_status SET status = 'completed' WHERE session_id = %s
    time.sleep(3)
    logger.info("Metadata updated successfully")


@op(description="Sends failure notification email if any upstream task fails")
def email_on_failure(context: OpExecutionContext, config: PipelineConfig, error: Exception) -> Nothing:
    """Sends failure email notification."""
    logger.error(f"Pipeline failed with error: {error}")
    # In real implementation, would use email resource:
    # email_client.send(
    #     recipients=config.email_recipients,
    #     subject=f"Pipeline failed: {context.job_name}",
    #     body=str(error)
    # )
    logger.info(f"Failure email would be sent to: {config.email_recipients}")


@job(
    description="Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration"
)
def l2_to_l2_segmentation_pipeline():
    """Defines the pipeline orchestration with dependencies."""
    
    # Sequential initial steps
    l2_load_ready = wait_for_l2_full_load()
    load_id = get_load_id()
    session_id = workflow_registration(load_id)
    previous_run_ok = wait_for_success_end()
    
    # Wait for all initial conditions
    initial_conditions_complete = run_sys_kill_all_session_pg(
        ins={
            "start_after": [
                l2_load_ready,
                load_id,
                session_id,
                previous_run_ok
            ]
        }
    )
    
    # Parallel branches after session kill
    data_prep_branch = run_wf_data_preparation_for_reports(
        ins={"start_after": [initial_conditions_complete]}
    )
    
    # Segmentation branch (sequential)
    segmentation_data = load_ds_client_segmentation(
        ins={"start_after": [initial_conditions_complete]}
    )
    sap_notification = send_flg_to_sap(segmentation_data)
    
    # Final step depends on both parallel branches
    end(
        session_id=session_id,
        ins={"start_after": [data_prep_branch, sap_notification]}
    )


# Error handling wrapper
@job(
    description="Main workflow with failure handling"
)
def l2_to_l2_segmentation_pipeline_with_error_handling():
    """Job with explicit failure handling."""
    try:
        l2_to_l2_segmentation_pipeline()
    except Exception as e:
        # This is a simplified pattern - in practice, Dagster has more sophisticated
        # error handling via hooks and resources
        email_on_failure(error=e)
        raise


# For direct execution
if __name__ == "__main__":
    # Execute the job in-process for testing
    result = l2_to_l2_segmentation_pipeline.execute_in_process(
        run_config={
            "ops": {
                "wait_for_l2_full_load": {
                    "config": {
                        "polling_interval": 10,  # Faster for demo
                        "max_wait_time": 120
                    }
                }
            }
        }
    )
    
    if result.success:
        logger.info("Pipeline executed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        # In a real scenario, failure hooks would trigger email notifications