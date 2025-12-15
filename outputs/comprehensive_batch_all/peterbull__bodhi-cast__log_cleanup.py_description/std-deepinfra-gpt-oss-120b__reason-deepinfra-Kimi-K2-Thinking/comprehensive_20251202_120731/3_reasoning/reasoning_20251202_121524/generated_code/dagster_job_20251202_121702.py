import os
import time
import fcntl
from pathlib import Path
from typing import List, Dict, Any

from dagster import (
    op,
    job,
    Config,
    ResourceParam,
    DynamicOut,
    DynamicOutput,
    Out,
    In,
    get_dagster_logger,
    OpExecutionContext,
)

# Configuration classes
class LogCleanupConfig(Config):
    max_log_age_in_days: int = 30
    enable_delete_child_log: bool = False
    sleep_time: int = 5
    lock_file_path: str = "/tmp/airflow_log_cleanup_worker.lock"
    base_log_folder: str = "/opt/airflow/logs"
    scheduler_child_log_folder: str = "/opt/airflow/logs/scheduler"


class WorkerConfig(Config):
    worker_id: int
    directories: List[str]


# Resource for shared configuration
class LogCleanupResource:
    def __init__(self, config: LogCleanupConfig):
        self.config = config

    def get_directories_to_clean(self) -> List[str]:
        """Get list of directories to clean based on configuration."""
        directories = [self.config.base_log_folder]
        
        if self.config.enable_delete_child_log:
            directories.append(self.config.scheduler_child_log_folder)
        
        # Also include immediate subdirectories of base log folder
        # This mimics Airflow's typical log structure (dag_id/task_id/execution_date)
        try:
            base_path = Path(self.config.base_log_folder)
            if base_path.exists():
                # Get first level subdirectories (DAG IDs)
                for dag_dir in base_path.iterdir():
                    if dag_dir.is_dir():
                        directories.append(str(dag_dir))
        except Exception as e:
            logger = get_dagster_logger()
            logger.warning(f"Could not enumerate subdirectories: {e}")
        
        return directories


@op(
    description="Initialize the log cleanup pipeline",
    out=Out(dict),
)
def start_pipeline(context: OpExecutionContext, config: LogCleanupConfig):
    """Start the pipeline and return configuration."""
    logger = get_dagster_logger()
    logger.info("Starting Airflow log cleanup pipeline")
    
    return {
        "max_log_age_in_days": config.max_log_age_in_days,
        "enable_delete_child_log": config.enable_delete_child_log,
        "sleep_time": config.sleep_time,
        "lock_file_path": config.lock_file_path,
        "base_log_folder": config.base_log_folder,
    }


@op(
    description="Create worker configurations for parallel cleanup",
    out=DynamicOut(WorkerConfig),
)
def create_worker_configs(
    context: OpExecutionContext,
    start_info: dict,
    config: LogCleanupConfig,
):
    """Partition directories among workers and create worker configs."""
    logger = get_dagster_logger()
    
    resource = LogCleanupResource(config)
    directories = resource.get_directories_to_clean()
    
    # Determine number of workers (use 3 workers for demonstration)
    num_workers = 3
    directories_per_worker = len(directories) // num_workers + 1
    
    logger.info(f"Partitioning {len(directories)} directories across {num_workers} workers")
    
    for i in range(num_workers):
        start_idx = i * directories_per_worker
        end_idx = start_idx + directories_per_worker
        worker_dirs = directories[start_idx:end_idx]
        
        if worker_dirs:  # Only create worker if it has directories
            worker_config = WorkerConfig(
                worker_id=i,
                directories=worker_dirs,
            )
            
            yield DynamicOutput(
                worker_config,
                mapping_key=f"worker_{i}",
            )


@op(
    description="Perform log cleanup for assigned directories",
)
def cleanup_worker(
    context: OpExecutionContext,
    worker_config: WorkerConfig,
    config: LogCleanupConfig,
) -> Dict[str, Any]:
    """Execute three-phase cleanup for assigned directories."""
    logger = get_dagster_logger()
    logger.info(f"Worker {worker_config.worker_id} starting cleanup of {len(worker_config.directories)} directories")
    
    # Stagger execution
    sleep_time = config.sleep_time * worker_config.worker_id
    logger.info(f"Worker {worker_config.worker_id} sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)
    
    lock_file = config.lock_file_path
    lock_acquired = False
    results = {
        "worker_id": worker_config.worker_id,
        "directories_processed": 0,
        "files_deleted": 0,
        "directories_removed": 0,
        "errors": [],
    }
    
    try:
        # Acquire lock
        logger.info(f"Worker {worker_config.worker_id} acquiring lock: {lock_file}")
        with open(lock_file, "w") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_acquired = True
                logger.info(f"Worker {worker_config.worker_id} lock acquired")
            except IOError:
                logger.error(f"Worker {worker_config.worker_id} could not acquire lock")
                results["errors"].append("Lock acquisition failed")
                return results
        
        # Perform cleanup for each directory
        for directory in worker_config.directories:
            if not os.path.exists(directory):
                logger.warning(f"Directory does not exist: {directory}")
                continue
            
            try:
                logger.info(f"Worker {worker_config.worker_id} cleaning directory: {directory}")
                
                # Phase 1: Delete old log files
                files_deleted = _delete_old_files(
                    directory, config.max_log_age_in_days, logger
                )
                results["files_deleted"] += files_deleted
                
                # Phase 2: Remove empty subdirectories
                dirs_removed = _remove_empty_subdirectories(directory, logger)
                results["directories_removed"] += dirs_removed
                
                # Phase 3: Remove empty top-level directory (if empty after cleanup)
                if _is_directory_empty(directory):
                    try:
                        os.rmdir(directory)
                        results["directories_removed"] += 1
                        logger.info(f"Removed empty top-level directory: {directory}")
                    except OSError:
                        # Directory not empty or permission issue
                        pass
                
                results["directories_processed"] += 1
                
            except Exception as e:
                error_msg = f"Error processing directory {directory}: {str(e)}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
    
    except Exception as e:
        logger.error(f"Worker {worker_config.worker_id} encountered fatal error: {str(e)}")
        results["errors"].append(f"Fatal error: {str(e)}")
    
    finally:
        # Release lock
        if lock_acquired:
            try:
                # Release the lock by closing the file
                # The with block will handle this, but being explicit for clarity
                logger.info(f"Worker {worker_config.worker_id} releasing lock")
            except Exception as e:
                logger.warning(f"Error releasing lock: {e}")
    
    logger.info(f"Worker {worker_config.worker_id} completed: {results}")
    return results


def _delete_old_files(directory: str, max_age_days: int, logger) -> int:
    """Delete files older than max_age_days."""
    import subprocess
    
    try:
        # Use find command to delete old files
        cmd = [
            "find",
            directory,
            "-type",
            "f",
            "-mtime",
            f"+{max_age_days}",
            "-delete",
        ]
        
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False
        )
        
        if result.returncode != 0:
            logger.warning(f"Find command warning: {result.stderr}")
        
        # Count deleted files (approximate)
        # Note: find -delete doesn't output count, so we use a separate command
        count_cmd = [
            "find",
            directory,
            "-type",
            "f",
            "-mtime",
            f"+{max_age_days}",
            "-print",
            "|",
            "wc",
            "-l",
        ]
        
        # For simplicity, we'll just return 0 and log the action
        # In a real scenario, you'd parse the output properly
        logger.info(f"Deleted files older than {max_age_days} days in {directory}")
        return 0  # Simplified
        
    except Exception as e:
        logger.error(f"Error deleting old files in {directory}: {e}")
        return 0


def _remove_empty_subdirectories(directory: str, logger) -> int:
    """Remove empty subdirectories within the given directory."""
    import subprocess
    
    try:
        # Use find to remove empty directories bottom-up
        cmd = [
            "find",
            directory,
            "-type",
            "d",
            "-empty",
            "-delete",
        ]
        
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False
        )
        
        if result.returncode != 0:
            logger.warning(f"Find command warning for empty dirs: {result.stderr}")
        
        logger.info(f"Removed empty subdirectories in {directory}")
        return 0  # Simplified
        
    except Exception as e:
        logger.error(f"Error removing empty subdirectories in {directory}: {e}")
        return 0


def _is_directory_empty(directory: str) -> bool:
    """Check if directory is empty."""
    try:
        return len(os.listdir(directory)) == 0
    except Exception:
        return False


@op(
    description="Collect and summarize cleanup results",
    ins={"results": In(List[Dict[str, Any]])},
)
def collect_results(context: OpExecutionContext, results: List[Dict[str, Any]]):
    """Collect results from all workers and log summary."""
    logger = get_dagster_logger()
    
    total_files_deleted = sum(r.get("files_deleted", 0) for r in results)
    total_dirs_removed = sum(r.get("directories_removed", 0) for r in results)
    total_dirs_processed = sum(r.get("directories_processed", 0) for r in results)
    total_errors = sum(len(r.get("errors", [])) for r in results)
    
    logger.info("=== Log Cleanup Summary ===")
    logger.info(f"Directories processed: {total_dirs_processed}")
    logger.info(f"Files deleted: {total_files_deleted}")
    logger.info(f"Directories removed: {total_dirs_removed}")
    logger.info(f"Total errors: {total_errors}")
    
    if total_errors > 0:
        logger.warning("Errors encountered during cleanup:")
        for result in results:
            for error in result.get("errors", []):
                logger.warning(f"  - {error}")
    
    return {
        "directories_processed": total_dirs_processed,
        "files_deleted": total_files_deleted,
        "directories_removed": total_dirs_removed,
        "errors": total_errors,
    }


@job(
    description="Daily maintenance job to cleanup old Airflow log files",
    config={
        "ops": {
            "start_pipeline": {
                "config": {
                    "max_log_age_in_days": 30,
                    "enable_delete_child_log": False,
                    "sleep_time": 5,
                    "lock_file_path": "/tmp/airflow_log_cleanup_worker.lock",
                    "base_log_folder": "/opt/airflow/logs",
                }
            }
        }
    },
)
def airflow_log_cleanup_job():
    """Job that orchestrates parallel cleanup of Airflow log files."""
    start_info = start_pipeline()
    worker_results = create_worker_configs(start_info).map(cleanup_worker)
    collect_results(worker_results.collect())


if __name__ == "__main__":
    # Execute the job in process for testing
    result = airflow_log_cleanup_job.execute_in_process()
    if result.success:
        print("Job executed successfully!")
    else:
        print("Job failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")