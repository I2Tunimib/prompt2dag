from __future__ import annotations

import random
import time
import uuid
from typing import Any

from dagster import Failure, HookContext, In, Nothing, Out, JobDefinition, failure_hook, job, op


@op(out=Out(Nothing), description="Waits for L1 to L2 data load completion by polling a flag.")
def wait_for_l2_full_load() -> Nothing:
    """Simulate a sensor that checks a database flag."""
    # In a real implementation, poll the metadata table until the flag is set.
    print("Waiting for L1 to L2 full load to complete...")
    time.sleep(1)  # placeholder for polling delay
    print("L1 to L2 full load completed.")
    return Nothing


@op(out=Out(str), description="Retrieves a unique load identifier for session tracking.")
def get_load_id(_: Nothing) -> str:
    load_id = str(uuid.uuid4())
    print(f"Generated load_id: {load_id}")
    return load_id


@op(ins={"load_id": In(str)}, out=Out(Nothing), description="Registers the workflow session.")
def workflow_registration(load_id: str) -> Nothing:
    # Stub for metadata registration
    print(f"Registering workflow session with load_id: {load_id}")
    # Imagine inserting a row into a metadata table here.
    return Nothing


@op(ins={"_": In(Nothing)}, out=Out(Nothing), description="Waits for the previous day's DAG run to finish.")
def wait_for_success_end(_: Nothing) -> Nothing:
    """Simulate an external task sensor."""
    print("Waiting for previous day's successful execution...")
    time.sleep(1)  # placeholder for waiting
    print("Previous day's execution confirmed successful.")
    return Nothing


@op(ins={"_": In(Nothing)}, out=Out(Nothing), description="Triggers system utility DAG to kill all PostgreSQL sessions.")
def run_sys_kill_all_session_pg(_: Nothing) -> Nothing:
    print("Triggering sys_kill_all_session_pg DAG...")
    # In a real environment, this would trigger another Dagster job or external workflow.
    time.sleep(1)
    print("sys_kill_all_session_pg DAG triggered.")
    return Nothing


@op(ins={"_": In(Nothing)}, out=Out(Nothing), description="Triggers data preparation workflow for reports.")
def run_wf_data_preparation_for_reports(_: Nothing) -> Nothing:
    print("Triggering wf_data_preparation_for_reports DAG...")
    time.sleep(1)
    print("wf_data_preparation_for_reports DAG triggered.")
    return Nothing


@op(ins={"_": In(Nothing)}, out=Out(int), description="Triggers client segmentation data loading workflow.")
def load_ds_client_segmentation(_: Nothing) -> int:
    print("Triggering client segmentation load DAG...")
    time.sleep(1)
    # Simulate row count result
    row_count = random.randint(1000, 5000)
    print(f"Client segmentation loaded with {row_count} rows.")
    return row_count


@op(ins={"row_count": In(int)}, out=Out(Nothing), description="Sends completion flag to SAP with row count.")
def send_flg_to_sap(row_count: int) -> Nothing:
    print(f"Sending completion flag to SAP with row count: {row_count}...")
    # Stub for HTTP call to SAP
    time.sleep(1)
    print("SAP notified successfully.")
    return Nothing


@op(
    ins={"prep": In(Nothing), "seg": In(Nothing)},
    out=Out(Nothing),
    description="Marks the workflow session as successfully completed.",
)
def end(prep: Nothing, seg: Nothing) -> Nothing:
    print("All parallel branches completed. Updating metadata to mark success.")
    # Stub for final metadata update
    time.sleep(1)
    print("Workflow session marked as successfully completed.")
    return Nothing


@failure_hook
def email_on_failure(context: HookContext) -> None:
    """Send a failure notification email (stub implementation)."""
    recipients = ["ops@example.com"]
    subject = f"Dagster job '{context.job_name}' failed"
    body = f"The job '{context.job_name}' failed with error: {context.failure_message}"
    # In a real implementation, integrate with an SMTP server or email service.
    print(f"Sending failure email to {recipients}")
    print(f"Subject: {subject}")
    print(f"Body: {body}")


@job(hooks={email_on_failure}, description="Main workflow for loading datasets and SAP integration.")
def main_job() -> JobDefinition:
    # Sequential steps
    wait_sensor = wait_for_l2_full_load()
    load_id = get_load_id(wait_sensor)
    registration = workflow_registration(load_id)
    wait_external = wait_for_success_end(registration)
    kill_sessions = run_sys_kill_all_session_pg(wait_external)

    # Parallel branches
    prep_reports = run_wf_data_preparation_for_reports(kill_sessions)
    seg_load = load_ds_client_segmentation(kill_sessions)
    sap_notify = send_flg_to_sap(seg_load)

    # Final step depends on both parallel branches
    end(prep_reports, sap_notify)


if __name__ == "__main__":
    result = main_job.execute_in_process()
    if result.success:
        print("Dagster job completed successfully.")
    else:
        print("Dagster job failed.")