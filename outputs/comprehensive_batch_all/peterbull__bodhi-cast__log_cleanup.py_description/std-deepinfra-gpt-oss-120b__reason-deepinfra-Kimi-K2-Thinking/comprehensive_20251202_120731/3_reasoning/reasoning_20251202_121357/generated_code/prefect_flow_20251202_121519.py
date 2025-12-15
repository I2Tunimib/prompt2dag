import os
import time
import subprocess
import hashlib
from pathlib import Path
from typing import List, Dict, Any

from prefect import flow, task, get_run_logger
from prefect.context import FlowRunContext

from filelock import FileLock, Timeout


@task
def get_configuration() -> Dict[str, Any]:
    """Retrieve cleanup configuration from parameters, env vars, or defaults."""
    logger = get_run_logger()
    
    try:
        context = FlowRunContext.get()
        params = context.flow_run.parameters if context and context.flow_run else {}
    except Exception:
        params = {}
    
    max_log_age_in_days = params.get(
        "max_log_age_in_days",
        int(os.getenv("AIRFLOW_LOG_CLEANUP__MAX_LOG_AGE_IN_DAYS", "30"))
    )
    
    enable_delete_child_log = params.get(
        "enable_delete_child_log",
        os.getenv("AIRFLOW_LOG_CLEANUP__ENABLE_DELETE_CHILD_LOG", "true").lower() == "true"
    )
    
    base_log_folder = os.getenv("AIRFLOW_BASE_LOG_FOLDER", "/opt/airflow/logs")
    child_log_directory = os.getenv("AIRFLOW_CHILD_PROCESS_LOG_DIRECTORY")
    worker_sleep_time = int(os.getenv("AIRFLOW_LOG_CLEANUP__WORKER_SLEEP_TIME", "5"))
    
    config = {
        "max_log_age_in_days": max_log_age_in_days,
        "enable_delete_child_log": enable_delete_child_log,
        "base_log_folder": base_log_folder,
        "child_log_directory": child_log_directory,
        "worker_sleep_time": worker_sleep_time,
        "lock_file_dir": "/tmp"
    }
    
    logger.info(f"Cleanup configuration: {config}")
    return config


@task
def discover_log_directories(config: Dict[str, Any]) -> List[str]:
    """Discover all log directories that need cleanup."""
    logger = get_run_logger()
    
    directories = []
    base_path = Path(config["base_log_folder"])
    
    if base_path.exists():
        directories.append(str(base_path))
        for subdir in base_path.iterdir():
            if subdir.is_dir():
                directories.append(str(subdir))
    
    if config["enable_delete_child_log"] and config["child_log_directory"]:
        child_path = Path(config["child_log_directory"])
        if child_path.exists():
            directories.append(str(child_path))
    
    seen = set()
    unique_directories = []
    for d in directories:
        if d not in seen:
            seen.add(d)
            unique_directories.append(d)
    
    logger.info(f"Discovered {len(unique_directories)} log directories")
    return unique_directories


@task(retries=1, retry_delay_seconds=60)
def cleanup_worker(log_dir: str, config: Dict[str, Any], worker_id: int):
    """Worker task that cleans up a specific log directory."""
    logger = get_run_logger()
    
    dir_hash = hashlib.md5(log_dir.encode()).hexdigest()[:8]
    lock_path = Path(config["lock_file_dir"]) / f"airflow_log_cleanup_{dir_hash}.lock"
    max_age = config["max_log_age_in_days"]
    sleep_time = config["worker_sleep_time"]
    
    stagger_delay = sleep_time * worker_id
    logger.info(f"Worker {worker_id}: Sleeping for {stagger_delay} seconds")
    time.sleep(stagger_delay)
    
    lock = FileLock(str(lock_path), timeout=30)
    try:
        logger.info(f"Worker {worker_id}: Acquiring lock {lock_path}")
        lock.acquire()
        logger.info(f"Worker {worker_id}: Lock acquired for {log_dir}")
        
        logger.info(f"Worker {worker_id}: Phase 1 - Deleting .log files older than {max_age} days")
        find_cmd = [
            "find", log_dir,
            "-type", "f",
            "-name", "*.log",
            "-mtime", f"+{max_age}",
            "-delete"
        ]
        result = subprocess.run(find_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"Worker {worker_id}: Find command failed: {result.stderr}")
        
        logger.info(f"Worker {worker_id}: Phase 2 - Removing empty subdirectories")
        rmdir_cmd = [
            "find", log_dir,
            "-mindepth", "1",
            "-type", "d",
            "-empty",
            "-delete"
        ]
        result = subprocess.run(rmdir_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"Worker {worker_id}: Rmdir command failed: {result.stderr}")
        
        logger.info(f"Worker {worker_id}: Phase 3 - Checking if top-level directory is empty")
        path_obj = Path(log_dir)
        if path_obj.exists() and not any(path_obj.iterdir()):
            logger.info(f"Worker {worker_id}: Removing empty top-level directory {log_dir}")
            try:
                path_obj.rmdir()
            except OSError as e:
                logger.warning(f"Worker {worker_id}: Could not remove {log_dir}: {e}")
        
        logger.info(f"Worker {worker_id}: Cleanup completed successfully for {log_dir}")
        
    except Timeout:
        logger.error(f"Worker {worker_id}: Timeout acquiring lock {lock_path}")
        raise
    except Exception as e:
        logger.error(f"Worker {worker_id}: Unexpected error: {e}")
        raise
    finally:
        try:
            lock.release()
            logger.info(f"Worker {worker_id}: Lock released")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Error releasing lock: {e}")


@flow(
    name="airflow-log-cleanup",
    description="Automated cleanup of old Airflow log files with parallel workers"
)
def log_cleanup_flow():
    """Main flow for cleaning up old Airflow log files."""
    logger = get_run_logger()
    logger.info("Starting Airflow log cleanup flow")
    
    config = get_configuration()
    log_directories = discover_log_directories(config)
    
    if not log_directories:
        logger.warning("No log directories found to clean")
        return
    
    worker_futures = []
    for idx, log_dir in enumerate(log_directories):
        future = cleanup_worker.submit(
            log_dir=log_dir,
            config=config,
            worker_id=idx
        )
        worker_futures.append(future)
    
    logger.info(f"Submitted {len(worker_futures)} cleanup workers")


if __name__ == "__main__":
    # For local execution and testing
    # Deploy with: prefect deployment build log_cleanup_flow.py:log_cleanup_flow \
    #   --name "daily-log-cleanup" --cron "0 0 * * *" --param max_log_age_in_days=30
    log_cleanup_flow()