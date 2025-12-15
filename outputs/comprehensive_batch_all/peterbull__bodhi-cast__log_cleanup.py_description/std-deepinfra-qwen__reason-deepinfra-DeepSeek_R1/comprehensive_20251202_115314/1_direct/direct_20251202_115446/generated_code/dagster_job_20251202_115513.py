from datetime import datetime, timedelta
from dagster import job, op, resource, RetryPolicy, Failure, Field, Int, String, In, Out
from dagster.utils import file_relative_path
import os
import time
import subprocess


@resource(config_schema={"base_log_folder": Field(String, default_value="/var/log/airflow")})
def airflow_log_config(context):
    return context.resource_config


@op
def start(context):
    """Entry point for the pipeline."""
    context.log.info("Pipeline started.")


@op(
    config_schema={
        "worker_id": Field(Int, is_required=True),
        "max_log_age_in_days": Field(Int, is_required=True),
        "enable_delete_child_log": Field(bool, default_value=False),
    },
    required_resource_keys={"airflow_log_config"},
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def cleanup_logs(context):
    """Perform log cleanup for a specific worker."""
    worker_id = context.op_config["worker_id"]
    max_log_age_in_days = context.op_config["max_log_age_in_days"]
    enable_delete_child_log = context.op_config["enable_delete_child_log"]
    base_log_folder = context.resources.airflow_log_config["base_log_folder"]
    lock_file = "/tmp/airflow_log_cleanup_worker.lock"

    try:
        # Acquire lock
        with open(lock_file, "w") as f:
            f.write(f"Worker {worker_id} is running.")
            f.flush()
            os.fsync(f.fileno())

        # Phase 1: Delete old log files
        context.log.info(f"Worker {worker_id}: Deleting old log files.")
        subprocess.run(
            [
                "find",
                base_log_folder,
                "-type",
                "f",
                "-mtime",
                f"+{max_log_age_in_days}",
                "-delete",
            ],
            check=True,
        )

        # Phase 2: Remove empty subdirectories
        context.log.info(f"Worker {worker_id}: Removing empty subdirectories.")
        subprocess.run(
            ["find", base_log_folder, "-type", "d", "-empty", "-delete"], check=True
        )

        # Phase 3: Remove empty top-level log directories
        if enable_delete_child_log:
            context.log.info(f"Worker {worker_id}: Removing empty top-level log directories.")
            subprocess.run(
                ["find", base_log_folder, "-type", "d", "-empty", "-delete"], check=True
            )

    except subprocess.CalledProcessError as e:
        context.log.error(f"Worker {worker_id}: Error during cleanup: {e}")
        raise Failure(f"Worker {worker_id}: Error during cleanup: {e}")
    finally:
        # Release lock
        os.remove(lock_file)


@op
def finish(context):
    """Final step to indicate pipeline completion."""
    context.log.info("Pipeline completed.")


@job(
    resource_defs={"airflow_log_config": airflow_log_config},
    config={
        "ops": {
            "cleanup_logs": {
                "config": {
                    "worker_id": 1,
                    "max_log_age_in_days": 7,
                    "enable_delete_child_log": False,
                }
            }
        }
    },
    tags={"dag_id": "airflow_log_cleanup"},
    description="Automated cleanup of old Airflow log files.",
    tags={"schedule_interval": "@daily", "start_date": (datetime.now() - timedelta(days=1)).isoformat(), "catchup": "False", "retries": 1, "retry_delay": 60, "email_on_failure": True, "paused_on_creation": False},
)
def airflow_log_cleanup():
    start_op = start()
    cleanup_ops = [
        cleanup_logs.alias(f"cleanup_logs_{i}")(start_op) for i in range(3)
    ]
    finish_op = finish(*cleanup_ops)


if __name__ == "__main__":
    result = airflow_log_cleanup.execute_in_process()