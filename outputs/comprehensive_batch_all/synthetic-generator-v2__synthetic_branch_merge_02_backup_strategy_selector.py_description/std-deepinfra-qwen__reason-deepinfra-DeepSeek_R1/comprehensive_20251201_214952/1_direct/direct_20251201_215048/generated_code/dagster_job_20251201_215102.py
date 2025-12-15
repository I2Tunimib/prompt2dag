from datetime import datetime
from dagster import op, job, In, Out, RetryPolicy, ResourceDefinition, execute_in_process

# Resources
# Example resource for database connection
db_resource = ResourceDefinition.hardcoded_resource("db_connection")

# Ops
@op(ins={"start": In()}, out=Out(str))
def date_check_task(start):
    """Determine the backup strategy based on the day of the week."""
    today = datetime.now().weekday()  # 0=Monday, 6=Sunday
    if today == 5:  # Saturday
        return "full_backup_task"
    else:
        return "incremental_backup_task"

@op(ins={"backup_type": In(str)}, out=Out(str))
def full_backup_task(backup_type):
    """Execute a full database backup."""
    # Simulate a full backup operation
    print("Executing full backup...")
    return "Full backup completed"

@op(ins={"backup_type": In(str)}, out=Out(str))
def incremental_backup_task(backup_type):
    """Execute an incremental database backup."""
    # Simulate an incremental backup operation
    print("Executing incremental backup...")
    return "Incremental backup completed"

@op(ins={"full_backup_result": In(str), "incremental_backup_result": In(str)}, out=Out(str))
def verify_backup_task(full_backup_result, incremental_backup_result):
    """Verify the integrity and completeness of the backup."""
    # Simulate backup verification
    print("Verifying backup...")
    return "Backup verified"

@op(ins={"verification_result": In(str)})
def backup_complete(verification_result):
    """Mark the backup process as complete."""
    print("Backup process complete")

# Job
@job(
    resource_defs={"db": db_resource},
    tags={"owner": "Backup team"},
    description="Automated database backup pipeline with conditional branching.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def database_backup_job():
    start = "Start_Backup_Process"
    backup_type = date_check_task(start)
    
    full_backup_result = full_backup_task(backup_type).with_hooks({backup_type: "full_backup_task"})
    incremental_backup_result = incremental_backup_task(backup_type).with_hooks({backup_type: "incremental_backup_task"})
    
    verification_result = verify_backup_task(full_backup_result, incremental_backup_result)
    backup_complete(verification_result)

# Launch pattern
if __name__ == '__main__':
    result = database_backup_job.execute_in_process()