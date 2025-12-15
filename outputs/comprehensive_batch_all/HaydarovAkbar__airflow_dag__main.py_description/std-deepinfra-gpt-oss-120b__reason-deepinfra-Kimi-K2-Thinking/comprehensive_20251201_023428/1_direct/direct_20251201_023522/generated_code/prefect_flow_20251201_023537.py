from __future__ import annotations

import time
import uuid
from typing import Any

from prefect import flow, task, get_run_logger


@task(retries=3, retry_delay_seconds=60)
def wait_for_l2_full_load(poll_interval: int = 60, timeout: int = 3600) -> None:
    """Poll the metadata table until the L1‑to‑L2 load flag is set.

    Args:
        poll_interval: Seconds between polls.
        timeout: Maximum seconds to wait before raising an error.
    """
    logger = get_run_logger()
    elapsed = 0
    while elapsed < timeout:
        # Placeholder for actual SQL check, e.g. using psycopg2 or SQLAlchemy.
        flag_set = True  # Assume flag is set for demonstration.
        if flag_set:
            logger.info("L2 full load flag detected.")
            return
        logger.info("Waiting for L2 full load flag... (%s seconds elapsed)", elapsed)
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError("Timed out waiting for L2 full load flag.")


@task
def get_load_id() -> str:
    """Generate a unique load identifier for the session."""
    load_id = str(uuid.uuid4())
    get_run_logger().info("Generated load_id: %s", load_id)
    return load_id


@task
def workflow_registration(load_id: str) -> None:
    """Register the workflow session in metadata tables and start logging."""
    logger = get_run_logger()
    # Placeholder for actual registration logic.
    logger.info("Registering workflow session with load_id %s.", load_id)


@task(retries=3, retry_delay_seconds=60)
def wait_for_success_end(poll_interval: int = 60, max_wait: int = 86400) -> None:
    """Wait for the previous day's execution of this DAG to complete successfully.

    Args:
        poll_interval: Seconds between polls.
        max_wait: Maximum seconds to wait (24 h by default).
    """
    logger = get_run_logger()
    elapsed = 0
    while elapsed < max_wait:
        # Placeholder for external task sensor logic.
        previous_success = True  # Assume success for demonstration.
        if previous_success:
            logger.info("Previous day's DAG run completed successfully.")
            return
        logger.info(
            "Waiting for previous day's DAG run... (%s seconds elapsed)", elapsed
        )
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError("Timed out waiting for previous DAG run to succeed.")


@task
def run_sys_kill_all_session_pg() -> None:
    """Trigger the system utility DAG that kills all PostgreSQL sessions."""
    logger = get_run_logger()
    # Placeholder for triggering external DAG.
    logger.info("Triggered sys_kill_all_session_pg DAG.")


@task
def run_wf_data_preparation_for_reports() -> None:
    """Launch the data preparation workflow for reports."""
    logger = get_run_logger()
    # Placeholder for triggering external DAG.
    logger.info("Triggered wf_data_preparation_for_reports DAG.")


@task
def load_ds_client_segmentation() -> int:
    """Trigger the client segmentation data loading workflow.

    Returns:
        The number of rows processed (simulated).
    """
    logger = get_run_logger()
    # Placeholder for triggering external DAG.
    logger.info("Triggered l1_to_l2_p_load_data_ds_client_segmentation_full DAG.")
    # Simulate processing and return a row count.
    row_count = 1_000
    logger.info("Segmentation loaded %d rows.", row_count)
    return row_count


@task
def send_flg_to_sap(row_count: int) -> None:
    """Send a completion flag to the SAP system with the processed row count."""
    logger = get_run_logger()
    # Placeholder for HTTP request to SAP.
    logger.info(
        "Sent completion flag to SAP with row count %d.", row_count
    )


@task
def end() -> None:
    """Mark the workflow session as successfully completed in metadata tables."""
    logger = get_run_logger()
    # Placeholder for final metadata update.
    logger.info("Workflow session marked as successfully completed.")


@task
def email_on_failure(error_message: str) -> None:
    """Send a failure notification email.

    Args:
        error_message: Description of the failure.
    """
    logger = get_run_logger()
    # Placeholder for SMTP email sending.
    logger.error("Failure notification sent. Error: %s", error_message)


@flow
def main_flow() -> None:
    """Orchestrates the L2 loading and segmentation pipeline."""
    logger = get_run_logger()
    try:
        wait_for_l2_full_load()
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end()
        run_sys_kill_all_session_pg()

        # Parallel branch: data preparation and segmentation group.
        prep_future = run_wf_data_preparation_for_reports.submit()
        seg_future = load_ds_client_segmentation.submit()

        # After segmentation finishes, send flag to SAP.
        row_count = seg_future.result()
        send_flg_to_sap.submit(row_count)

        # Wait for the data preparation branch to finish.
        prep_future.result()

        end()
        logger.info("Pipeline completed successfully.")
    except Exception as exc:  # pragma: no cover
        email_on_failure(str(exc))
        raise


# Deployment note:
# This flow is intended for manual triggering only (no schedule). Deploy with
# Prefect Cloud/Server and configure concurrency limits as needed.

if __name__ == "__main__":
    main_flow()