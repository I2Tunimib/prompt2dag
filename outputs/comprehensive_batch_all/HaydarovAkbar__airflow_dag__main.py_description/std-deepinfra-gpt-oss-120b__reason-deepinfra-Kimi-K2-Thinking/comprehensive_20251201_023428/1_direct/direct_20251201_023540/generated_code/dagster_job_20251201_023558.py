from __future__ import annotations

import time
from typing import Any, Dict

import dagster
from dagster import Failure, HookContext, InputDefinition, Output, OutputDefinition, ResourceDefinition, op, job, Failure, Failure, HookContext, failure_hook


# ----------------------------------------------------------------------
# Resource stubs
# ----------------------------------------------------------------------


def db_resource_factory() -> Dict[str, Any]:
    """A minimal stub for a database resource."""
    class DB:
        def query_flag(self, table: str, flag_column: str) -> bool:
            # Placeholder: always return True for demo purposes.
            return True

        def register_workflow(self, load_id: str) -> None:
            # Placeholder implementation.
            pass

        def mark_success(self, load_id: str) -> None:
            # Placeholder implementation.
            pass

    return {"db": DB()}


def http_resource_factory() -> Dict[str, Any]:
    """A minimal stub for an HTTP client used for SAP integration."""
    class HTTPClient:
        def post(self, url: str, json: Dict[str, Any]) -> None:
            # Placeholder: pretend the request succeeded.
            pass

    return {"http_client": HTTPClient()}


def email_resource_factory() -> Dict[str, Any]:
    """A minimal stub for an email sender."""
    class EmailSender:
        def send(self, subject: str, body: str, recipients: list[str]) -> None:
            # Placeholder: log the email content.
            print(f"Sending email to {recipients}: {subject}\\n{body}")

    return {"email_sender": EmailSender()}


# ----------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------


@op(required_resource_keys={"db"})
def wait_for_l2_full_load(context: dagster.OpContext) -> None:
    """Polls the metadata table until the L1‑to‑L2 load flag is set."""
    db = context.resources.db
    poll_interval = 60  # seconds
    max_attempts = 60  # roughly one hour

    for attempt in range(max_attempts):
        if db.query_flag(table="metadata", flag_column="l2_load_complete"):
            context.log.info("L2 full load flag detected.")
            return
        context.log.info(
            f"L2 load flag not set yet (attempt {attempt + 1}/{max_attempts}). Sleeping..."
        )
        time.sleep(poll_interval)

    raise Failure("Timeout waiting for L2 full load flag.")


@op
def get_load_id(context: dagster.OpContext) -> str:
    """Generates a unique load identifier for the current session."""
    load_id = f"load_{int(time.time())}"
    context.log.info(f"Generated load_id: {load_id}")
    return load_id


@op(required_resource_keys={"db"})
def workflow_registration(context: dagster.OpContext, load_id: str) -> str:
    """Registers the workflow session in metadata tables."""
    db = context.resources.db
    db.register_workflow(load_id)
    context.log.info(f"Workflow registered with load_id: {load_id}")
    return load_id


@op
def wait_for_success_end(context: dagster.OpContext) -> None:
    """Placeholder for ExternalTaskSensor logic."""
    # In a real implementation this would query the scheduler or metadata store.
    context.log.info("Assuming previous day DAG run succeeded.")
    return


@op
def run_sys_kill_all_session_pg(context: dagster.OpContext) -> None:
    """Triggers the system utility DAG to kill all PostgreSQL sessions."""
    context.log.info("Triggering sys_kill_all_session_pg DAG (stub).")
    return


@op
def run_wf_data_preparation_for_reports(context: dagster.OpContext) -> None:
    """Triggers the data preparation workflow for reports."""
    context.log.info("Triggering wf_data_preparation_for_reports DAG (stub).")
    return


@op
def load_ds_client_segmentation(context: dagster.OpContext) -> int:
    """Triggers client segmentation data loading workflow and returns row count."""
    # Stub: pretend we loaded 12345 rows.
    row_count = 12345
    context.log.info(
        f"Client segmentation loaded with row count: {row_count} (stub)."
    )
    return row_count


@op(required_resource_keys={"http_client"})
def send_flg_to_sap(context: dagster.OpContext, row_count: int) -> None:
    """Sends a completion flag to SAP with the row count."""
    http = context.resources.http_client
    payload = {"status": "completed", "row_count": row_count}
    url = "https://sap.example.com/notify"
    http.post(url=url, json=payload)
    context.log.info(
        f"Sent completion flag to SAP with payload: {payload} (stub)."
    )
    return


@op(required_resource_keys={"db"})
def end(context: dagster.OpContext, load_id: str) -> None:
    """Marks the workflow session as successfully completed."""
    db = context.resources.db
    db.mark_success(load_id)
    context.log.info(f"Workflow session {load_id} marked as successful.")
    return


# ----------------------------------------------------------------------
# Failure hook
# ----------------------------------------------------------------------


@failure_hook(required_resource_keys={"email_sender"})
def email_on_failure(context: HookContext) -> None:
    """Sends an email notification when any op in the job fails."""
    email = context.resources.email_sender
    subject = f"Dagster job failure: {context.job_name}"
    body = (
        f"Failure in op `{context.op_name}`\\n"
        f"Run ID: {context.run_id}\\n"
        f"Error: {context.failure}"
    )
    recipients = ["dataops@example.com"]
    email.send(subject=subject, body=body, recipients=recipients)


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------


@job(
    resource_defs={
        "db": ResourceDefinition.resource_fn(db_resource_factory),
        "http_client": ResourceDefinition.resource_fn(http_resource_factory),
        "email_sender": ResourceDefinition.resource_fn(email_resource_factory),
    },
    hooks={email_on_failure},
)
def l2_load_and_segmentation_job():
    """Main job orchestrating L2 load monitoring, data preparation, segmentation, and SAP notification."""
    # Sequential steps before branching
    wait_for_l2_full_load()
    load_id = get_load_id()
    registered_load_id = workflow_registration(load_id)
    wait_for_success_end()
    run_sys_kill_all_session_pg()

    # Parallel branches
    # Branch 1: data preparation for reports
    run_wf_data_preparation_for_reports()

    # Branch 2: segmentation group
    row_cnt = load_ds_client_segmentation()
    send_flg_to_sap(row_cnt)

    # Converge and finish
    end(registered_load_id)


if __name__ == "__main__":
    result = l2_load_and_segmentation_job.execute_in_process()
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")