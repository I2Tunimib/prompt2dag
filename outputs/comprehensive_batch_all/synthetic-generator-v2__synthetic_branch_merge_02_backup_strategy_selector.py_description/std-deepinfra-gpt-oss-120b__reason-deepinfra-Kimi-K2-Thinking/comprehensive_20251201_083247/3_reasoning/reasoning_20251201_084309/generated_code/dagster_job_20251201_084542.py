@op
def date_check_task(context) -> str:
    # returns 'full_backup_task' or 'incremental_backup_task'

@op
def full_backup_task(context):
    # runs only if Saturday

@op
def incremental_backup_task(context):
    # runs only if weekday

@job
def backup_job():
    branch = date_check_task()
    # How to conditionally execute based on branch?