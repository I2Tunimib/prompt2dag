from dagster import op, job, Nothing, Failure, HookContext, failure_hook
import time
import random

# Minimal stubs for external dependencies - replace with actual resource implementations
class MockDWH:
    def check_l2_load_flag(self):
        # Simulate checking metadata table for L1->L2 completion
        return True
    
    def get_load_id(self):
        return f"load_{int(time.time())}"
    
    def register_workflow(self, load_id):
        return f"session_{load_id}"
    
    def check_previous_day_success(self):
        # Simulate 24-hour delta check for previous run
        return True
    
    def update_metadata_success(self, session_id):
        print(f"Updated metadata for session {session_id} as successful")
    
    def get_segmentation_row_count(self):
        # Simulate retrieving row count from segmentation results
        return random.randint(1000, 5000)

class MockSAP:
    def send_completion_flag(self, row_count):
        print(f"Sent completion flag to SAP with row count: {row_count}")
        return True

class MockEmail:
    def send_failure_notification(self, error_msg):
        print(f"Sending failure email: {error_msg}")

# Instantiate mock resources
dwh = MockDWH()
sap = MockSAP()
email_notifier = MockEmail()

# Failure notification hook
@failure_hook(required_resource_keys={})
def email_on_failure(context: HookContext):
    """Send email notification when any op fails"""
    error_msg = f"Job {context.job_name} failed in op {context.op.name}: {context.op_exception}"
    email_notifier.send_failure_notification(error_msg)

# Pipeline ops
@op
def wait_for_l2_full_load() -> bool:
    """Monitor for successful L1 to L2 data load completion via metadata table"""
    max_attempts = 10
    for attempt in range(max_attempts):
        if dwh.check_l2_load_flag():
            return True
        time.sleep(60)  # 60-second polling interval
    raise Failure("L2 full load did not complete within expected time")

@op
def get_load_id(_l2_complete: bool) -> str:
    """Retrieve unique load identifier for session tracking"""
    return dwh.get_load_id()

@op
def workflow_registration(load_id: str) -> str:
    """Register workflow session in metadata tables and initiate logging"""
    return dwh.register_workflow(load_id)

@op
def wait_for_success_end(session_id: str) -> str:
    """Wait for previous day's execution to complete successfully (24-hour delta)"""
    max_attempts = 10
    for attempt in range(max_attempts):
        if dwh.check_previous_day_success():
            return session_id
        time.sleep(100)  # 100-second polling interval
    raise Failure("Previous day's execution did not succeed")

@op
def run_sys_kill_all_session_pg(session_id: str) -> str:
    """Trigger system utility DAG to kill all PostgreSQL sessions"""
    print("Triggered sys_kill_all_session_pg utility")
    return session_id

@op
def run_wf_data_preparation_for_reports(session_id: str) -> str:
    """Launch data preparation workflow for reports"""
    print("Triggered wf_data_preparation_for_reports")
    return session_id

@op
def load_ds_client_segmentation(session_id: str) -> str:
    """Trigger client segmentation data loading workflow"""
    print("Triggered l1_to_l2_p_load_data_ds_client_segmentation_full")
    return session_id

@op
def send_flg_to_sap(_segmentation_complete: Nothing) -> int:
    """Send completion flag to SAP system with row count from segmentation"""
    row_count = dwh.get_segmentation_row_count()
    sap.send_completion_flag(row_count)
    return row_count

@op
def end_session(session_id: str, _data_prep_result: Nothing, _sap_result: Nothing):
    """Update metadata tables to mark workflow session as successfully completed"""
    dwh.update_metadata_success(session_id)

# Main job definition
@job(
    hooks={email_on_failure},
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 20  # Support up to 20 concurrent runs
                }
            }
        }
    }
)
def dwh_l2_to_l2_segmentation_pipeline():
    """
    Main workflow for loading datasets from DWH L2 to L2 with segmentation 
    processing and SAP integration. Manual trigger only, no catchup.
    Timezone: Asia/Tashkent (configure in deployment)
    """
    
    # Sequential initialization steps
    l2_load_complete = wait_for_l2_full_load()
    load_id = get_load_id(l2_load_complete)
    session_id = workflow_registration(load_id)
    previous_success = wait_for_success_end(session_id)
    kill_sessions = run_sys_kill_all_session_pg(previous_success)
    
    # Parallel execution branches
    data_prep = run_wf_data_preparation_for_reports(kill_sessions)
    segmentation_load = load_ds_client_segmentation(kill_sessions)
    sap_notification = send_flg_to_sap(segmentation_load)
    
    # Fan-in: wait for both branches to complete before final step
    end_session(session_id, data_prep, sap_notification)

# Minimal launch pattern for local execution
if __name__ == '__main__':
    result = dwh_l2_to_l2_segmentation_pipeline.execute_in_process()
    if not result.success:
        exit(1)