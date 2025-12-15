from __future__ import annotations

import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import allow_failure


@task
def start_task() -> None:
    """Entry point task that does nothing but marks the start of the flow."""
    return None


@task
def get_configuration(
    max_log_age_in_days: Optional[int] = None,
    enable_delete_child_log: Optional[bool] = None,
    base_log_folder: Optional[str] = None,
) -> dict:
    """
    Resolve configuration values from flow parameters, environment variables,
    or sensible defaults.

    Returns:
        dict: Configuration dictionary with keys:
            - max_log_age_days (int)
            - enable_delete_child_log (bool)
            - base_log_folder (Path)
    """
    max_age = (
        max_log_age_in_days
        or int(os.getenv("AIRFLOW_LOG_CLEANUP_MAX_LOG_AGE_IN_DAYS", "30"))
    )
    enable_child = (
        enable_delete_child_log
        if enable_delete_child_log is not None
        else os.getenv("AIRFLOW_LOG_CLEANUP_ENABLE_DELETE_CHILD_LOG", "false")
        .lower()
        .strip()
        == "true"
    )
    base_folder = Path(
        base_log_folder or os.getenv("BASE_LOG_FOLDER", "/tmp/airflow_logs")
    ).expanduser().resolve()

    return {
        "max_log_age_days": max_age,
        "enable_delete_child_log": enable_child,
        "base_log_folder": base_folder,
    }


def _acquire_lock(lock_path: Path, timeout: int = 30) -> None:
    """
    Acquire a file‑based lock by creating a lock file atomically.
    Raises RuntimeError if the lock cannot be acquired within the timeout.
    """
    start = time.time()
    while True:
        try:
            # O_EXCL ensures the operation fails if the file already exists
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return
        except FileExistsError:
            if time.time() - start > timeout:
                raise RuntimeError(f"Unable to acquire lock {lock_path}")
            time.sleep(0.5)


def _release_lock(lock_path: Path) -> None:
    """Remove the lock file if it exists."""
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass


@task(
    retries=1,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=None,
)
def cleanup_worker(
    worker_dir: Path,
    max_log_age_days: int,
    enable_delete_child_log: bool,
    sleep_seconds: int = 0,
) -> None:
    """
    Perform the three‑phase cleanup for a single worker directory.

    Steps:
        1. Delete log files older than ``max_log_age_days``.
        2. Remove empty sub‑directories.
        3. Remove empty top‑level log directories.

    A lock file prevents concurrent execution on the same worker.
    """
    lock_path = Path(f"/tmp/airflow_log_cleanup_{worker_dir.name}.lock")
    try:
        _acquire_lock(lock_path)

        # Stagger execution to spread load
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        cutoff = datetime.now() - timedelta(days=max_log_age_days)

        # Phase 1: delete old files
        for root, _, files in os.walk(worker_dir):
            for fname in files:
                file_path = Path(root) / fname
                try:
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if mtime < cutoff:
                        file_path.unlink()
                except Exception:
                    # Continue on individual file errors
                    continue

        # Optional: also clean child process logs if enabled
        if enable_delete_child_log:
            child_log_dir = worker_dir / "scheduler_child_process_logs"
            if child_log_dir.is_dir():
                for root, _, files in os.walk(child_log_dir):
                    for fname in files:
                        file_path = Path(root) / fname
                        try:
                            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                            if mtime < cutoff:
                                file_path.unlink()
                        except Exception:
                            continue

        # Phase 2 & 3: remove empty directories (bottom‑up)
        for dir_path, _, _ in os.walk(worker_dir, topdown=False):
            path_obj = Path(dir_path)
            try:
                if not any(path_obj.iterdir()):
                    path_obj.rmdir()
            except Exception:
                continue

    finally:
        _release_lock(lock_path)


@task
def gather_worker_directories(base_log_folder: Path) -> List[Path]:
    """
    Return a list of sub‑directories under ``base_log_folder`` that will be
    processed in parallel. If the base folder does not exist, an empty list is
    returned.
    """
    if not base_log_folder.is_dir():
        return []
    return [p for p in base_log_folder.iterdir() if p.is_dir()]


@flow
def airflow_log_cleanup_flow(
    max_log_age_in_days: Optional[int] = None,
    enable_delete_child_log: Optional[bool] = None,
    base_log_folder: Optional[str] = None,
) -> None:
    """
    Orchestrates the cleanup of old Airflow log files across multiple workers.

    The flow:
        1. Starts with a dummy start task.
        2. Retrieves configuration.
        3. Discovers worker directories.
        4. Launches ``cleanup_worker`` tasks in parallel, staggering each
           with a small random sleep to distribute load.
    """
    start_task()
    config = get_configuration(
        max_log_age_in_days=max_log_age_in_days,
        enable_delete_child_log=enable_delete_child_log,
        base_log_folder=base_log_folder,
    ).result()

    workers = gather_worker_directories(config["base_log_folder"]).result()

    # Submit cleanup tasks in parallel
    futures = []
    for worker in workers:
        # Random sleep between 0‑5 seconds to stagger workers
        sleep_sec = random.randint(0, 5)
        future = cleanup_worker.submit(
            worker_dir=worker,
            max_log_age_days=config["max_log_age_days"],
            enable_delete_child_log=config["enable_delete_child_log"],
            sleep_seconds=sleep_sec,
        )
        futures.append(future)

    # Wait for all workers to finish
    for f in futures:
        f.result()


# Schedule comment:
# This flow is intended to be deployed with a daily schedule (e.g., using
# Prefect deployments with a CronSchedule("@daily")). Catchup is disabled
# and the flow starts active.

if __name__ == "__main__":
    airflow_log_cleanup_flow()