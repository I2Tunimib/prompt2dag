import os
import time
import fcntl
from pathlib import Path
from typing import List, Dict, Any, Optional

from prefect import flow, task, get_run_logger
from prefect.variables import Variable


@task(retries=1, retry_delay_seconds=60)
def get_cleanup_config(
    max_log_age_in_days: Optional[int] = None,
    enable_delete_child_log: Optional[bool] = None
) -> Dict[str, Any]:
    """Retrieve cleanup configuration from Prefect variables and flow parameters."""
    logger = get_run_logger()
    
    try:
        default_max_age = Variable.get("airflow_log_cleanup__max_log_age_in_days", default="30")
        default_max_age = int(default_max_age)
    except Exception:
        default_max_age = 30
    
    try:
        default_enable_child = Variable.get("airflow_log_cleanup__enable_delete_child_log", default="false")
        default_enable_child = default_enable_child.lower() == "true"
    except Exception:
        default_enable_child = False
    
    config = {
        "max_log_age_in_days": max_log_age_in_days or default_max_age,
        "enable_delete_child_log": enable_delete_child_log if enable_delete_child_log is not None else default_enable_child
    }
    
    logger.info(f"Cleanup config: {config}")
    return config


@task
def start_cleanup() -> str:
    """Initiates the cleanup pipeline."""
    logger = get_run_logger()
    logger.info("Starting Airflow log cleanup pipeline")
    return "started"


@task(retries=1, retry_delay_seconds=60)
def cleanup_worker(
    worker_id: int,
    directories: List[str],
    max_age_days: int,
    enable_child_logs: bool,
    sleep_time: int = 0
) -> Dict[str, Any]:
    """
    Worker task that cleans up log files and directories.
    
    Implements file-based locking to prevent concurrent execution conflicts.
    """
    logger = get_run_logger()
    lock_file = "/tmp/airflow_log_cleanup_worker.lock"
    results = {
        "worker_id": worker_id,
        "directories_processed": 0,
        "files_deleted": 0,
        "directories_deleted": 0,
        "errors": []
    }
    
    if sleep_time > 0:
        logger.info(f"Worker {worker_id} sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)
    
    lock_acquired = False
    lock_fd = None
    
    try:
        lock_fd = open(lock_file, 'w')
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_acquired = True
            logger.info(f"Worker {worker_id} acquired lock")
        except IOError:
            logger.warning(f"Worker {worker_id} could not acquire lock, skipping")
            results["errors"].append("Lock acquisition failed")
            return results
        
        for directory in directories:
            if not os.path.exists(directory):
                logger.warning(f"Directory {directory} does not exist, skipping")
                continue
            
            logger.info(f"Worker {worker_id} processing directory: {directory}")
            dir_path = Path(directory)
            
            try:
                # Phase 1: Delete old log files
                logger.info(f"Phase 1: Deleting files older than {max_age_days} days")
                cutoff_time = time.time() - (max_age_days * 86400)
                for file_path in dir_path.rglob("*"):
                    if file_path.is_file():
                        try:
                            if file_path.stat().st_mtime < cutoff_time:
                                file_path.unlink()
                                results["files_deleted"] += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete {file_path}: {e}")
                            results["errors"].append(str(e))
                
                # Phase 2: Remove empty subdirectories
                logger.info("Phase 2: Removing empty subdirectories")
                for sub_dir in sorted(dir_path.rglob("*"), key=lambda p: len(p.parts), reverse=True):
                    if sub_dir.is_dir():
                        try:
                            if not any(sub_dir.iterdir()):
                                sub_dir.rmdir()
                                results["directories_deleted"] += 1
                        except Exception as e:
                            logger.warning(f"Failed to remove {sub_dir}: {e}")
                            results["errors"].append(str(e))
                
                # Phase 3: Remove empty top-level directories
                logger.info("Phase 3: Checking for empty top-level directories")
                try:
                    if dir_path.is_dir() and not any(dir_path.iterdir()):
                        dir_path.rmdir()
                        results["directories_deleted"] += 1
                        logger.info(f"Removed empty top-level directory: {directory}")
                except Exception as e:
                    logger.warning(f"Failed to remove top-level {dir_path}: {e}")
                    results["errors"].append(str(e))
                
                results["directories_processed"] += 1
                
            except Exception as e:
                logger.error(f"Error processing directory {directory}: {e}")
                results["errors"].append(str(e))
        
        if lock_acquired and lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            logger.info(f"Worker {worker_id} released lock")
    
    except Exception as e:
        logger.error(f"Worker {worker_id} encountered fatal error: {e}")
        results["errors"].append(str(e))
    finally:
        if lock_acquired and lock_fd:
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            except:
                pass
            lock_fd.close()
    
    logger.info(f"Worker {worker_id} completed: {results}")
    return results


@flow(
    name="airflow-log-cleanup-flow",
    description="Automated cleanup of old Airflow log files"
)
def airflow_log_cleanup_flow(
    max_log_age_in_days: Optional[int] = None,
    enable_delete_child_log: Optional[bool] = None,
    worker_count: int = 3,
    base_log_folder: str = "/opt/airflow/logs",
    child_log_folder: Optional[str] = None
):
    """
    Flow for cleaning up old Airflow log files.
    
    Args:
        max_log_age_in_days: Override for log retention period
        enable_delete_child_log: Override for child log deletion
        worker_count: Number of parallel workers
        base_log_folder: Main Airflow log directory
        child_log_folder: Optional child process log directory
    """
    logger = get_run_logger()
    
    config = get_cleanup_config(
        max_log_age_in_days=max_log_age_in_days,
        enable_delete_child_log=enable_delete_child_log
    )
    
    start_status = start_cleanup()
    
    all_directories = [base_log_folder]
    if config["enable_delete_child_log"] and child_log_folder:
        all_directories.append(child_log_folder)
    
    directories_per_worker = [[] for _ in range(worker_count)]
    for idx, directory in enumerate(all_directories):
        worker_idx = idx % worker_count
        directories_per_worker[worker_idx].append(directory)
    
    worker_futures = []
    for i in range(worker_count):
        if directories_per_worker[i]:
            sleep_time = i * 5
            
            future = cleanup_worker.submit(
                worker_id=i,
                directories=directories_per_worker[i],
                max_age_days=config["max_log_age_in_days"],
                enable_child_logs=config["enable_delete_child_log"],
                sleep_time=sleep_time
            )
            worker_futures.append(future)
    
    results = [future.result() for future in worker_futures]
    
    total_files = sum(r["files_deleted"] for r in results)
    total_dirs = sum(r["directories_deleted"] for r in results)
    total_errors = sum(len(r["errors"]) for r in results)
    
    logger.info(
        f"Cleanup completed: {total_files} files deleted, "
        f"{total_dirs} directories deleted, {total_errors} errors"
    )
    
    if total_errors > 0:
        logger.warning(f"Encountered {total_errors} errors during cleanup")


if __name__ == "__main__":
    # For local testing. To deploy with daily schedule, use:
    # prefect deployment build airflow_log_cleanup_flow.py:airflow_log_cleanup_flow \
    #   --name "daily-log-cleanup" --cron "0 0 * * *" --timezone UTC
    #   --params '{"base_log_folder": "/opt/airflow/logs"}'
    airflow_log_cleanup_flow()