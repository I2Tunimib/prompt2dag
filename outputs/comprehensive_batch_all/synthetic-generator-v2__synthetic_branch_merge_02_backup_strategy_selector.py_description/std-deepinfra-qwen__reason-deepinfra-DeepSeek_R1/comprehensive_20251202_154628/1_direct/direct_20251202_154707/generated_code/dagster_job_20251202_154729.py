from datetime import datetime
from dagster import op, job, In, Out, RetryPolicy, ResourceDefinition, execute_in_process

# Resources
# Simplified resource example for database connection
db_resource = ResourceDefinition.hardcoded_resource("db_connection")

# Ops
@op(ins={"start": In()}, out=Out(str))
def date_check_task(start):
    """Evaluates the execution date's day of week and returns the appropriate backup task."""
    today = datetime.now()
    if today.weekday() == 5:  # Saturday
        return "full_backup_task"
    else:
        return "incremental_backup_task"

@op(ins={"backup_type": In(str)}, out=Out(str))
def full_backup_task(backup_type):
    """Executes a full database backup."""
    # Simulate full backup
    return "Full backup completed"

@op(ins={"backup_type": In(str)}, out=Out(str))
def incremental_backup_task(backup_type):
    """Executes an incremental database backup."""
    # Simulate incremental backup
    return "Incremental backup completed"

@op(ins={"full_backup_result": In(str), "incremental_backup_result": In(str)}, out=Out(str))
def verify_backup_task(full_backup_result, incremental_backup_result):
    """Validates backup integrity and completeness."""
    # Simulate backup verification
    return "Backup verification completed"

@op(ins={"verification_result": In(str)})
def backup_complete(verification_result):
    """Serves as the pipeline exit point."""
    # Simulate completion
    return "Backup process complete"

# Job
@job(
    resource_defs={"db": db_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def automated_backup_job():
    start = yield from start_backup_process()
    backup_type = date_check_task(start)
    
    full_backup_result = full_backup_task(backup_type).with_hooks({"full_backup_task"})
    incremental_backup_result = incremental_backup_task(backup_type).with_hooks({"incremental_backup_task"})
    
    verification_result = verify_backup_task(full_backup_result, incremental_backup_result)
    backup_complete(verification_result)

# Minimal launch pattern
if __name__ == '__main__':
    result = automated_backup_job.execute_in_process()