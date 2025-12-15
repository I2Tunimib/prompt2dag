import os
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from prefect import flow, task
from prefect.tasks import task_input_hash


@task
def start_task() -> None:
    """Entry point placeholder for the cleanup flow."""
    return None


@task
def get_configuration() -> Dict[str, int]:
    """
    Retrieve cleanup configuration from environment variables or defaults.

    Returns:
        dict: Configuration containing max log age (days), child log inclusion flag,
              and optional stagger sleep time (seconds).
    """
    max_age = int(
        os.getenv(
            "airflow_log_cleanup__max_log_age_in_days", "30"
        )
    )
    enable_child = int(
        os.getenv(
            "airflow_log_cleanup__enable_delete_child_log", "0"
        )
    )
    # Stagger sleep time can be set to spread load; default 0 seconds.
    stagger_seconds = int(os.getenv("AIRFLOW_LOG_CLEANUP_STAGGER_SECONDS", "0"))

    return {
        "max_log_age_days": max_age,
        "enable_delete_child_log": enable_child,
        "stagger_seconds": stagger_seconds,
    }


@task
def get_log_directories() -> List[Path]:
    """
    Determine the list of top‑level log directories to process.

    Returns:
        List[Path]: Paths to log directories.
    """
    base_log_folder = os.getenv("BASE_LOG_FOLDER", "/opt/airflow/logs")
    base_path = Path(base_log_folder).expanduser().resolve()
    if not base_path.is_dir():
        raise FileNotFoundError(f"Base log folder not found: {base_path}")

    # Include optional child process log directory if enabled via env.
    include_child = int(
        os.getenv("airflow_log_cleanup__enable_delete_child_log", "0")
    )
    dirs = [base_path]

    if include_child:
        child_dir = base_path / "scheduler"
        if child_dir.is_dir():
            dirs.append(child_dir)

    return dirs


@task(retries=1, retry_delay_seconds=60)
def cleanup_worker(
    log_dir: Path,
    config: Dict[str, int],
) -> str:
    """
    Perform the three‑phase cleanup for a single log directory.

    Args:
        log_dir (Path): Directory to clean.
        config (dict): Configuration dict.

    Returns:
        str: Message indicating success or reason for skipping.
    """
    lock_path = Path(f"/tmp/airflow_log_cleanup_{log_dir.name}.lock")
    try:
        # Acquire lock atomically; fail if already exists.
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        return f"Lock exists for {log_dir}, skipping."

    try:
        # Optional stagger to distribute load.
        stagger = config.get("stagger_seconds", 0)
        if stagger > 0:
            time.sleep(stagger)

        max_age = config.get("max_log_age_days", 30)

        # Phase 1: Delete old log files.
        delete_cmd = [
            "find",
            str(log_dir),
            "-type",
            "f",
            "-mtime",
            f"+{max_age}",
            "-delete",
        ]
        subprocess.run(delete_cmd, check=True, capture_output=True)

        # Phase 2: Remove empty sub‑directories.
        empty_subdirs_cmd = [
            "find",
            str(log_dir),
            "-type",
            "d",
            "-empty",
            "-delete",
        ]
        subprocess.run(empty_subdirs_cmd, check=True, capture_output=True)

        # Phase 3: Remove empty top‑level log directories (depth >= 1).
        empty_top_dirs_cmd = [
            "find",
            str(log_dir),
            "-mindepth",
            "1",
            "-type",
            "d",
            "-empty",
            "-delete",
        ]
        subprocess.run(empty_top_dirs_cmd, check=True, capture_output=True)

        return f"Cleanup completed for {log_dir}"
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Cleanup command failed for {log_dir}: {exc.stderr.decode()}"
        ) from exc
    finally:
        # Ensure lock file removal.
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


@flow
def airflow_log_cleanup_flow() -> None:
    """
    Orchestrates the parallel cleanup of Airflow log directories.

    The flow:
    1. Starts with a placeholder task.
    2. Retrieves configuration and target directories.
    3. Executes cleanup workers in parallel using .submit().
    """
    start_task()
    config = get_configuration()
    log_dirs = get_log_directories()

    futures = []
    for dir_path in log_dirs:
        future = cleanup_worker.submit(dir_path, config)
        futures.append(future)

    # Wait for all workers to finish.
    for future in futures:
        future.result()


# Schedule comment:
# This flow is intended to be deployed with a daily schedule (e.g., using Prefect
# Deployments with an @daily interval) and should start one day prior to the
# current date. Catch‑up is disabled, and email alerts can be configured at the
# deployment level for failure notifications.

if __name__ == "__main__":
    airflow_log_cleanup_flow()