import os
import subprocess
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from dagster import (
    OpExecutionContext,
    RetryPolicy,
    Config,
    Field,
    List as DagsterList,
    Int,
    Bool,
    op,
    job,
    DynamicOut,
    DynamicOutput,
)


class GetDirectoriesConfig(Config):
    """Configuration for the start op that provides the list of log directories to clean."""

    directories: DagsterList[str] = Field(
        default_value=[],
        description="Absolute paths of top‑level Airflow log directories to process.",
    )


class CleanupWorkerConfig(Config):
    """Configuration for each cleanup worker."""

    max_log_age_in_days: Int = Field(
        default_value=30,
        description="Maximum age of log files (in days) before they are deleted.",
    )
    enable_delete_child_log: Bool = Field(
        default_value=False,
        description="Whether to also clean child‑process log directories.",
    )
    sleep_seconds: Int = Field(
        default_value=0,
        description="Base sleep time (seconds) to stagger workers. A random jitter is added.",
    )
    lock_dir: str = Field(
        default_value="/tmp",
        description="Directory where lock files are stored.",
    )


@op(config_schema=GetDirectoriesConfig)
def get_directories(context: OpExecutionContext) -> List[str]:
    """Entry point op that yields the directories to be cleaned."""
    dirs = context.op_config["directories"]
    if not dirs:
        context.log.warning("No directories provided; the job will do nothing.")
    else:
        context.log.info(f"Received {len(dirs)} directories to clean.")
    return dirs


@op(
    out=DynamicOut(),
    config_schema=CleanupWorkerConfig,
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def cleanup_worker(
    context: OpExecutionContext, directory: str
) -> DynamicOutput[str]:
    """Per‑directory cleanup worker.

    Performs three phases:
    1. Delete old log files.
    2. Remove empty sub‑directories.
    3. Remove empty top‑level directory.
    """
    cfg = context.op_config
    max_age = cfg["max_log_age_in_days"]
    enable_child = cfg["enable_delete_child_log"]
    base_sleep = cfg["sleep_seconds"]
    lock_dir = cfg["lock_dir"]

    # Stagger execution to reduce load spikes.
    jitter = random.uniform(0, 5)
    total_sleep = base_sleep + jitter
    if total_sleep > 0:
        context.log.info(f"Sleeping for {total_sleep:.2f}s before processing {directory}.")
        time.sleep(total_sleep)

    lock_path = Path(lock_dir) / f"airflow_log_cleanup_{Path(directory).as_posix().replace('/', '_')}.lock"

    try:
        # Acquire lock.
        if lock_path.exists():
            context.log.warning(f"Lock file {lock_path} exists; skipping {directory}.")
            return DynamicOutput(value=directory, mapping_key=directory)

        lock_path.write_text(str(os.getpid()))
        context.log.info(f"Acquired lock for {directory}.")

        # Phase 1: Delete old log files.
        find_cmd = [
            "find",
            directory,
            "-type",
            "f",
            "-mtime",
            f"+{max_age}",
            "-delete",
        ]
        if enable_child:
            # Child logs are under a known sub‑directory; adjust pattern if needed.
            pass  # No extra handling required for the generic find command.

        context.log.info(f"Running file deletion command: {' '.join(find_cmd)}")
        subprocess.run(find_cmd, check=False)

        # Phase 2: Remove empty sub‑directories.
        rmdir_sub_cmd = ["find", directory, "-type", "d", "-empty", "-delete"]
        context.log.info(f"Removing empty sub‑directories: {' '.join(rmdir_sub_cmd)}")
        subprocess.run(rmdir_sub_cmd, check=False)

        # Phase 3: Remove empty top‑level directory if it became empty.
        if not any(Path(directory).iterdir()):
            try:
                Path(directory).rmdir()
                context.log.info(f"Removed empty top‑level directory {directory}.")
            except OSError:
                context.log.info(f"Top‑level directory {directory} not empty after cleanup.")
    except Exception as exc:
        context.log.error(f"Error processing {directory}: {exc}")
        raise
    finally:
        # Ensure lock file removal.
        if lock_path.exists():
            try:
                lock_path.unlink()
                context.log.info(f"Released lock for {directory}.")
            except Exception as exc:
                context.log.error(f"Failed to remove lock file {lock_path}: {exc}")

    return DynamicOutput(value=directory, mapping_key=directory)


@job
def airflow_log_cleanup_job():
    """Dagster job that orchestrates parallel cleanup of Airflow log directories."""
    directories = get_directories()
    # Fan‑out: run a cleanup worker for each directory in parallel.
    directories.map(cleanup_worker)


if __name__ == "__main__":
    # Example execution with inline configuration.
    result = airflow_log_cleanup_job.execute_in_process(
        run_config={
            "ops": {
                "get_directories": {
                    "config": {
                        "directories": [
                            "/opt/airflow/logs/dag1",
                            "/opt/airflow/logs/dag2",
                            "/opt/airflow/logs/dag3",
                        ]
                    }
                },
                "cleanup_worker": {
                    "config": {
                        "max_log_age_in_days": 30,
                        "enable_delete_child_log": True,
                        "sleep_seconds": 2,
                        "lock_dir": "/tmp",
                    }
                },
            }
        }
    )
    assert result.success