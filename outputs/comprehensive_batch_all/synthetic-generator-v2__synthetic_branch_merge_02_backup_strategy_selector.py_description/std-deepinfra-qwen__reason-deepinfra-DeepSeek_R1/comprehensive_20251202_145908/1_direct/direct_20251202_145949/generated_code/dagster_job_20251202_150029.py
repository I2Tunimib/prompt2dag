from dagster import op, job, graph, In, Out, RetryPolicy, execute_in_process
from datetime import datetime

# Resources and Config
# Example resource for database connection (if needed)
# class DatabaseResource:
#     def __init__(self, connection_string):
#         self.connection_string = connection_string

# Example config for backup tasks
# class BackupConfig:
#     def __init__(self, backup_path):
#         self.backup_path = backup_path

# Ops
@op
def start_backup_process(context):
    """Entry point of the backup pipeline."""
    context.log.info("Starting backup process.")
    return True

@op
def date_check_task(context):
    """Determines the backup strategy based on the day of the week."""
    execution_date = context.op_config.get("execution_date", datetime.now())
    if execution_date.weekday() == 5:  # Saturday
        return "full_backup_task"
    else:
        return "incremental_backup_task"

@op
def full_backup_task(context):
    """Executes a full database backup."""
    context.log.info("Executing full backup.")
    # Example command: "pg_dump -U user -F c -b -v -f /path/to/backup/backup.sql.gz dbname"
    return True

@op
def incremental_backup_task(context):
    """Executes an incremental database backup."""
    context.log.info("Executing incremental backup.")
    # Example command: "pg_dump -U user -F c -b -v -f /path/to/backup/backup.sql.gz dbname --data-only"
    return True

@op(ins={"backup_result": In(bool)}, out=Out(bool))
def verify_backup_task(context, backup_result):
    """Validates backup integrity and completeness."""
    context.log.info("Verifying backup.")
    if backup_result:
        context.log.info("Backup verified successfully.")
        return True
    else:
        context.log.error("Backup verification failed.")
        return False

@op
def backup_complete(context):
    """Exit point of the backup pipeline."""
    context.log.info("Backup process completed.")
    return True

# Job
@job(
    resource_defs={},
    config={
        "ops": {
            "date_check_task": {
                "config": {
                    "execution_date": "2024-01-01"
                }
            }
        }
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def backup_pipeline():
    start = start_backup_process()
    date_check = date_check_task(start)
    
    full_backup = full_backup_task().with_hooks({date_check == "full_backup_task"})
    incremental_backup = incremental_backup_task().with_hooks({date_check == "incremental_backup_task"})
    
    verify_backup = verify_backup_task(full_backup, incremental_backup)
    backup_complete(verify_backup)

# Execution
if __name__ == '__main__':
    result = backup_pipeline.execute_in_process()