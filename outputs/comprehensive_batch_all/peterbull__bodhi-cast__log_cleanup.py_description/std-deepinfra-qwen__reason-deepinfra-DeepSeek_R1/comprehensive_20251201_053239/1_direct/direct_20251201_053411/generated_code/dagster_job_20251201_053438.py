from dagster import op, job, resource, RetryPolicy, Field, Int, String, In, Out, execute_in_process
import os
import time
from datetime import datetime, timedelta

@resource(config_schema={"base_log_folder": Field(String, default_value="/var/log/airflow")})
def airflow_log_config(context):
    return context.resource_config["base_log_folder"]

@op(required_resource_keys={"airflow_log_config"}, config_schema={"max_log_age_in_days": Field(Int)})
def start(context):
    """Start the pipeline and serve as the entry point."""
    context.log.info("Pipeline started")
    return context.op_config["max_log_age_in_days"]

@op(required_resource_keys={"airflow_log_config"}, config_schema={"log_directory": Field(String)})
def cleanup_log_directory(context, max_log_age_in_days):
    """Perform log cleanup for a specific directory."""
    log_directory = context.op_config["log_directory"]
    lock_file = "/tmp/airflow_log_cleanup_worker.lock"
    
    try:
        with open(lock_file, "w") as f:
            f.write("1")
        
        # Delete old log files
        context.log.info(f"Deleting old log files in {log_directory}")
        os.system(f"find {log_directory} -type f -mtime +{max_log_age_in_days} -delete")
        
        # Remove empty subdirectories
        context.log.info(f"Removing empty subdirectories in {log_directory}")
        os.system(f"find {log_directory} -type d -empty -delete")
        
        # Remove empty top-level log directories
        context.log.info(f"Removing empty top-level log directories in {log_directory}")
        os.system(f"find {log_directory} -type d -empty -delete")
    
    except Exception as e:
        context.log.error(f"Error during cleanup: {e}")
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)

@op
def finish(context):
    """Finalize the pipeline."""
    context.log.info("Pipeline completed")

@job(
    resource_defs={"airflow_log_config": airflow_log_config},
    config={
        "ops": {
            "start": {"config": {"max_log_age_in_days": 7}},
            "cleanup_log_directory_1": {"config": {"log_directory": "/var/log/airflow/dag1"}},
            "cleanup_log_directory_2": {"config": {"log_directory": "/var/log/airflow/dag2"}},
            "cleanup_log_directory_3": {"config": {"log_directory": "/var/log/airflow/dag3"}},
        }
    },
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def log_cleanup_job():
    max_log_age_in_days = start()
    cleanup_log_directory_1 = cleanup_log_directory(max_log_age_in_days)
    cleanup_log_directory_2 = cleanup_log_directory(max_log_age_in_days)
    cleanup_log_directory_3 = cleanup_log_directory(max_log_age_in_days)
    finish([cleanup_log_directory_1, cleanup_log_directory_2, cleanup_log_directory_3])

if __name__ == "__main__":
    result = log_cleanup_job.execute_in_process()