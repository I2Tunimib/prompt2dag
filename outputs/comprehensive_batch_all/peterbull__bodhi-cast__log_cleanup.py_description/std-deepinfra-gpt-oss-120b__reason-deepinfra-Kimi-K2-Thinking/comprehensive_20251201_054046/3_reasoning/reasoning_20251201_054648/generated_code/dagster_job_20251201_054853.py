import os
import time
import fcntl
from contextlib import contextmanager
from typing import Dict, Any, Optional

from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    DynamicOut,
    DynamicOutput,
    get_dagster_logger,
)

logger = get_dagster_logger()

BASE_LOCK_FILE_PATH = "/tmp/airflow_log_cleanup_worker.lock"

@contextmanager
def file_lock(lock_path: str):
    """Context manager for file-based locking using fcntl."""
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    except IOError:
        raise RuntimeError(f"Could not acquire lock {lock_path}")
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except:
            pass
        lock_file.close()
        try:
            os.remove(lock_path)
        except:
            pass

class LogCleanupConfig(Config):
    """Configuration for log cleanup job."""
    max_log_age_in_days: int = 30
    enable_delete_child_log: bool = False
    base_log_folder: str = "/opt/airflow/logs"
    child_log_folder: Optional[str] = None
    worker_count: int = 3
    sleep_time: int = 5

@op(
    description="Entry point for the pipeline",
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def start_pipeline(context: OpExecutionContext, config: LogCleanupConfig) -> LogCleanupConfig:
    """Initialize the pipeline and log configuration."""
    logger.info(f"Starting log cleanup pipeline with config: {config}")
    return config

@op(
    description="Determine log directory assignments for each worker",
    out=DynamicOut(Dict[str, Any]),
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def determine_log_directories(context: OpExecutionContext, config: LogCleanupConfig):
    """Split log directories among workers for parallel processing."""
    base_folder = config.base_log_folder
    child_folder = config.child_log_folder
    worker_count = config.worker_count
    
    directories = []
    if os.path.exists(base_folder):
        directories.append(base_folder)
    
    if child_folder and os.path.exists(child_folder) and config.enable_delete_child_log:
        directories.append(child_folder)
    
    all_dirs = []
    for directory in directories:
        if os.path.isdir(directory):
            all_dirs.append(directory)
            try:
                subdirs = [
                    os.path.join(directory, d) 
                    for d in os.listdir(directory) 
                    if os.path.isdir(os.path.join(directory, d))
                ]
                all_dirs.extend(subdirs)
            except Exception as e:
                logger.warning(f"Could not list subdirectories of {directory}: {e}")
    
    for i in range(worker_count):
        worker_dirs = all_dirs[i::worker_count]
        if worker_dirs:
            yield DynamicOutput(
                {
                    "worker_id": i,
                    "directories": worker_dirs,
                    "config": config,
                },
                mapping_key=f"worker_{i}"
            )

@op(
    description="Perform log cleanup for assigned directories",
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def cleanup_worker(context: OpExecutionContext, worker_assignment: Dict[str, Any]):
    """Execute three-phase cleanup for assigned directories with file locking."""
    worker_id = worker_assignment["worker_id"]
    directories = worker_assignment["directories"]
    config = worker_assignment["config"]
    
    sleep_time = config.sleep_time
    stagger_time = sleep_time * worker_id
    logger.info(f"Worker {worker_id} sleeping for {stagger_time} seconds")
    time.sleep(stagger_time)
    
    max_age_days = config.max_log_age_in_days
    lock_path = f"{BASE_LOCK_FILE_PATH}.{worker_id}"
    
    with file_lock(lock_path):
        try:
            for directory in directories:
                if not os.path.exists(directory):
                    logger.warning(f"Directory does not exist: {directory}")
                    continue
                
                logger.info(f"Worker {worker_id} processing directory: {directory}")
                
                try:
                    cmd = f"find '{directory}' -type f -name '*.log' -mtime +{max_age_days} -delete"
                    logger.info(f"Executing: {cmd}")
                    exit_code = os.system(cmd)
                    if exit_code != 0:
                        logger.warning(f"Find command exited with code {exit_code}")
                except Exception as e:
                    logger.error(f"Error deleting log files in {directory}: {e}")
                
                try:
                    cmd = f"find '{directory}' -type d -empty -delete"
                    logger.info(f"Executing: {cmd}")
                    exit_code = os.system(cmd)
                    if exit_code != 0:
                        logger.warning(f"Find command for empty dirs exited with code {exit_code}")
                except Exception as e:
                    logger.error(f"Error removing empty subdirectories in {directory}: {e}")
                
                try:
                    if os.path.exists(directory) and os.path.isdir(directory):
                        if not os.listdir(directory):
                            os.rmdir(directory)
                            logger.info(f"Removed empty top-level directory: {directory}")
                except Exception as e:
                    logger.error(f"Error removing empty top-level directory {directory}: {e}")
            
            logger.info(f"Worker {worker_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Worker {worker_id} failed: {e}")
            raise
        finally:
            try: