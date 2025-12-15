from __future__ import annotations

import time
import uuid
from typing import Any, Dict

from dagster import (
    Failure,
    HookContext,
    In,
    Out,
    ResourceDefinition,
    String,
    failure_hook,
    job,
    op,
    get_dagster_logger,
)


class DummyDB:
    """A minimal stub for database interactions."""

    def __init__(self) -> None:
        self._load_flag = True
        self._sessions: Dict[str, Any] = {}

    def check_l2_load_complete(self) -> bool:
        """Simulate checking a flag in a metadata table."""
        return self._load_flag

    def register_workflow(self, load_id: str) -> None:
        self._sessions[load_id] = {"status": "running"}

    def update_workflow_status(self, load_id: str, status: str) -> None:
        if load_id in self._sessions:
            self._sessions[load_id]["status"] = status

    def get_row_count_for_segmentation(self) -> int:
        """Return a dummy row count for segmentation results."""
        return 1234


class DummyHTTP:
    """A minimal stub for HTTP interactions (e.g., SAP)."""

    def post(self, url: str, json: Dict[str, Any]) -> None:
        logger = get_dagster_logger()
        logger.info("POST to %s with payload %s", url, json)


class DummyEmail:
    """A minimal stub for sending email notifications."""

    def send(self, subject: str, body: str, to: list[str]) -> None:
        logger = get_dagster_logger()
        logger.info("Sending email to %s: %s - %s", to, subject, body)


def db_resource() -> ResourceDefinition:
    return ResourceDefinition.hardcoded_resource(DummyDB())


def http_resource() -> ResourceDefinition:
    return ResourceDefinition.hardcoded_resource(DummyHTTP())


def email_resource() -> ResourceDefinition:
    return ResourceDefinition.hardcoded_resource(DummyEmail())


@op(required_resource_keys={"db"})
def wait_for_l2_full_load(context) -> None:
    """Poll the metadata table until the L2 load flag is true."""
    db: DummyDB = context.resources.db
    logger = context.log
    max_attempts = 10
    attempt = 0
    while attempt < max_attempts:
        if db.check_l2_load_complete():
            logger.info("L2 full load completed.")
            return
        logger.info("Waiting for L2 full load... attempt %d", attempt + 1)
        time.sleep(5)  # In real use, this would be 60-100 seconds
        attempt += 1
    raise Failure("L2 full load not detected within timeout.")


@op
def get_load_id() -> str:
    """Generate a unique load identifier for the session."""
    load_id = str(uuid.uuid4())
    get_dagster_logger().info("Generated load_id: %s", load_id)
    return load_id


@op(required_resource_keys={"db"})
def workflow_registration(context, load_id: str) -> None:
    """Register the workflow session in metadata tables."""
    db: DummyDB = context.resources.db
    db.register_workflow(load_id)
    context.log.info("Workflow registered with load_id %s", load_id)


@op
def wait_for_success_end(context) -> None:
    """Simulate waiting for the previous day's DAG execution to finish."""
    logger = context.log
    logger.info("Waiting for previous day's successful run (simulated).")
    time.sleep(2)  # Placeholder for external sensor logic


@op
def run_sys_kill_all_session_pg(context) -> None:
    """Trigger a system utility DAG to kill all PostgreSQL sessions."""
    context.log.info("Triggered sys_kill_all_session_pg DAG (simulated).")


@op
def run_wf_data_preparation_for_reports(context) -> None:
    """Launch data preparation workflow for reports."""
    context.log.info("Triggered wf_data_preparation_for_reports DAG (simulated).")


@op(required_resource_keys={"db"})
def load_ds_client_segmentation(context) -> int:
    """Trigger client segmentation data loading workflow and return row count."""
    db: DummyDB = context.resources.db
    # In a real environment, this would trigger another DAG.
    context.log.info("Triggered client segmentation loading DAG (simulated).")
    row_count = db.get_row_count_for_segmentation()
    context.log.info("Segmentation produced %d rows.", row_count)
    return row_count


@op(required_resource_keys={"http"})
def send_flg_to_sap(context, row_count: int) -> None:
    """Send completion flag to SAP system with row count."""
    http: DummyHTTP = context.resources.http
    payload = {"status": "completed", "row_count": row_count}
    http.post(url="https://sap.example.com/notify", json=payload)
    context.log.info("Sent completion flag to SAP with row count %d.", row_count)


@op(required_resource_keys={"db"})
def end(context, load_id: str) -> None:
    """Mark the workflow session as successfully completed."""
    db: DummyDB = context.resources.db
    db.update_workflow_status(load_id, status="success")
    context.log.info("Workflow session %s marked as successful.", load_id)


@failure_hook
def email_on_failure(context: HookContext) -> None:
    """Send failure notification email."""
    email: DummyEmail = context.resources.email
    subject = f"Dagster Job Failure: {context.job_name}"
    body = f"Job {context.job_name} failed on step {context.op_name}.\nException: {context.exception}"
    recipients = ["ops@example.com"]
    email.send(subject=subject, body=body, to=recipients)
    get_dagster_logger().info("Failure email sent to %s.", recipients)


@job(
    resource_defs={
        "db": db_resource(),
        "http": http_resource(),
        "email": email_resource(),
    },
    hooks={email_on_failure},
)
def main_job():
    """Orchestrates the full data loading and segmentation workflow."""
    wait_for_l2_full_load()
    load_id = get_load_id()
    workflow_registration(load_id)
    wait_for_success_end()
    run_sys_kill_all_session_pg()

    # Parallel branches
    data_prep = run_wf_data_preparation_for_reports()
    seg_row_count = load_ds_client_segmentation()
    send_flg_to_sap(seg_row_count)

    # Converge
    end(load_id)


if __name__ == "__main__":
    result = main_job.execute_in_process()
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")