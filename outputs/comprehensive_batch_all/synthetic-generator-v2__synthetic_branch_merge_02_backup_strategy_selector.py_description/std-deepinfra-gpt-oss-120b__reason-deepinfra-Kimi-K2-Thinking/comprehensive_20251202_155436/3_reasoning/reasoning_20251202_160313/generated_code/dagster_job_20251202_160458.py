from dagster import op, job, In, RetryPolicy, ScheduleDefinition, DefaultScheduleStatus
from datetime import datetime


@op
def start_backup_process():
    """Pipeline entry point."""
    return {"status": "started", "timestamp": datetime.now().isoformat()}


@op
def date_check_task(_start_result):
    """Check day of week and return backup strategy.
    
    Args:
        _start_result: Unused parameter to enforce dependency on start_backup_process.
    """
    day_of_week = datetime.now().weekday()  # 0=Monday, 5=Saturday
    
    if day_of_week == 5:  # Saturday
        return "full_backup"
    else:
        return "incremental_backup"


@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def full_backup_task(backup_strategy: str):
    """Execute full database backup (Saturday)."""
    if backup_strategy == "full_backup":
        # Simulate full backup
        return {"backup_type": "full", "status": "completed", "size_gb": 100}
    else:
        # Skip this backup
        return {"backup_type": "full", "status": "skipped"}


@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def incremental_backup_task(backup_strategy: str):
    """Execute incremental database backup (weekday)."""
    if backup_strategy == "incremental_backup":
        # Simulate incremental backup
        return {"backup_type": "incremental", "status": "completed", "size_gb": 10}
    else:
        # Skip this backup
        return {"backup_type": "incremental", "status": "skipped"}


@op
def verify_backup_task(full_result: dict, incremental_result: dict):
    """Validate backup integrity and completeness.
    
    Simulates Airflow's none_failed_min_one_success trigger rule.
    """
    full_status = full_result.get("status")
    incremental_status = incremental_result.get("status")
    
    # Check if at least one backup succeeded
    if full_status == "completed" or incremental_status == "completed":
        # Simulate verification
        return {"status": "verified", "message": "Backup verification successful"}
    else:
        raise RuntimeError("No successful backup found for verification")


@op
def backup_complete(verification_result: dict):
    """Pipeline exit point."""
    return {"status": "completed", "verification": verification_result}


@job(tags={"owner": "backup_team"})
def database_backup_job():
    """Database backup job with conditional branching."""
    start_result = start_backup_process()
    strategy = date_check_task(start_result)
    
    # Execute both backup tasks (they internally decide to run or skip)
    full_result = full_backup_task(strategy)
    incremental_result = incremental_backup_task(strategy)
    
    # Verify depends on both backup results
    verify_result = verify_backup_task(full_result, incremental_result)
    
    # Complete depends on verification
    backup_complete(verify_result)


# Schedule configuration
# Note: Dagster handles catchup differently than Airflow. The start_date and
# default_status control execution behavior. No backfill occurs for missed intervals
# when the schedule is started after the start_date.
database_backup_schedule = ScheduleDefinition(
    job=database_backup_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=DefaultScheduleStatus.RUNNING,
)


if __name__ == "__main__":
    # Execute the job directly for testing
    result = database_backup_job.execute_in_process()
    assert result.success