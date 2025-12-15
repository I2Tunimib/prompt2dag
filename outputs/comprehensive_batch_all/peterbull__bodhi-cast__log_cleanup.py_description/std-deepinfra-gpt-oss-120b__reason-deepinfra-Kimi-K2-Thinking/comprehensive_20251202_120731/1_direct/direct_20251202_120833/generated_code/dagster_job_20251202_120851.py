import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from dagster import RetryPolicy, job, op


@op
def start_op() -> None:
    """Entry point for the cleanup job."""
    return None


@op(
    name="cleanup_worker",
    retry_policy=RetryPolicy(max_attempts=2, delay=60),
    config_schema={
        "worker_name": str,
        "log_dirs": list[str],
        "max_log_age_in_days": int,
        "enable_delete_child_log": bool,
        "sleep_seconds": int,
    },
)
def cleanup_worker_op(context, start: None) -> None:
    """Deletes old Airflow log files and removes empty directories.

    The operation acquires a file‑based lock to avoid concurrent executions,
    optionally sleeps to stagger workers, and then performs a three‑phase
    cleanup:
    1. Delete files older than the configured age.
    2. Remove empty sub‑directories.
    3. Remove empty top‑level log directories.
    """
    cfg = context.op_config
    lock_path = Path("/tmp/airflow_log_cleanup_worker.lock")

    # Acquire lock
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        context.log.info(f"Acquired lock for worker `{cfg['worker_name']}`.")
    except FileExistsError as exc:
        raise RuntimeError(
            f"Lock file `{lock_path}` exists; another cleanup may be running."
        ) from exc

    try:
        # Stagger execution if required
        if cfg["sleep_seconds"] > 0:
            context.log.info(
                f"Worker `{cfg['worker_name']}` sleeping for {cfg['sleep_seconds']} seconds."
            )
            time.sleep(cfg["sleep_seconds"])

        cutoff = datetime.now() - timedelta(days=cfg["max_log_age_in_days"])

        for log_dir in cfg["log_dirs"]:
            base_path = Path(log_dir)
            if not base_path.is_dir():
                context.log.warning(f"Log directory `{log_dir}` does not exist.")
                continue

            # Phase 1: Delete old files
            for file_path in base_path.rglob("*"):
                if file_path.is_file():
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if mtime < cutoff:
                        try:
                            file_path.unlink()
                            context.log.info(f"Deleted file `{file_path}`.")
                        except Exception as exc:  # pylint: disable=broad-except
                            context.log.error(f"Failed to delete `{file_path}`: {exc}")

            # Phase 2: Remove empty sub‑directories (deepest first)
            for subdir in sorted(
                base_path.rglob("*"), key=lambda p: -len(p.parts)
            ):
                if subdir.is_dir():
                    try:
                        subdir.rmdir()
                        context.log.info(f"Removed empty directory `{subdir}`.")
                    except OSError:
                        # Directory not empty
                        pass

            # Phase 3: Remove empty top‑level directory
            try:
                base_path.rmdir()
                context.log.info(f"Removed empty top‑level directory `{base_path}`.")
            except OSError:
                # Directory not empty
                pass

    finally:
        # Release lock
        try:
            lock_path.unlink()
            context.log.info(f"Released lock for worker `{cfg['worker_name']}`.")
        except FileNotFoundError:
            pass


@job
def airflow_log_cleanup_job():
    """Dagster job that runs parallel cleanup workers."""
    start = start_op()
    cleanup_worker_op.alias("worker_1")(start)
    cleanup_worker_op.alias("worker_2")(start)
    cleanup_worker_op.alias("worker_3")(start)


if __name__ == "__main__":
    result = airflow_log_cleanup_job.execute_in_process(
        run_config={
            "ops": {
                "worker_1": {
                    "config": {
                        "worker_name": "worker_1",
                        "log_dirs": ["/path/to/airflow/logs/worker1"],
                        "max_log_age_in_days": 30,
                        "enable_delete_child_log": False,
                        "sleep_seconds": 0,
                    }
                },
                "worker_2": {
                    "config": {
                        "worker_name": "worker_2",
                        "log_dirs": ["/path/to/airflow/logs/worker2"],
                        "max_log_age_in_days": 30,
                        "enable_delete_child_log": False,
                        "sleep_seconds": 5,
                    }
                },
                "worker_3": {
                    "config": {
                        "worker_name": "worker_3",
                        "log_dirs": ["/path/to/airflow/logs/worker3"],
                        "max_log_age_in_days": 30,
                        "enable_delete_child_log": False,
                        "sleep_seconds": 10,
                    }
                },
            }
        }
    )
    assert result.success, "Airflow log cleanup job failed."