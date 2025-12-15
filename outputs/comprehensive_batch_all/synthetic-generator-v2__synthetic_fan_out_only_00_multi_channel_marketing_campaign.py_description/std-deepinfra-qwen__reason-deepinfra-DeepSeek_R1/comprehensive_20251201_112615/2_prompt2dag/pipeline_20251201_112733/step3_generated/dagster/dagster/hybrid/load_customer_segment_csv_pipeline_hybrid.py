from dagster import job, op, multiprocess_executor, fs_io_manager, resource, ScheduleDefinition

# Task Definitions
@op(
    name='load_customer_segment_csv',
    description='Load Customer Segment CSV',
)
def load_customer_segment_csv(context):
    """Op: Load Customer Segment CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_email_campaign',
    description='Send Email Campaign',
)
def send_email_campaign(context):
    """Op: Send Email Campaign"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_push_notification',
    description='Send Push Notification',
)
def send_push_notification(context):
    """Op: Send Push Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_sms_campaign',
    description='Send SMS Campaign',
)
def send_sms_campaign(context):
    """Op: Send SMS Campaign"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def email_delivery_system(context):
    pass

@resource
def sms_delivery_gateway(context):
    pass

@resource
def push_notification_service(context):
    pass

@resource
def local_filesystem(context):
    pass

# Job Definition
@job(
    name='load_customer_segment_csv_pipeline',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        'email_delivery_system': email_delivery_system,
        'sms_delivery_gateway': sms_delivery_gateway,
        'push_notification_service': push_notification_service,
        'local_filesystem': local_filesystem,
    },
    io_manager_def=fs_io_manager,
)
def load_customer_segment_csv_pipeline():
    customer_segment = load_customer_segment_csv()
    send_email_campaign(customer_segment)
    send_sms_campaign(customer_segment)
    send_push_notification(customer_segment)

# Schedule Definition
daily_schedule = ScheduleDefinition(
    job=load_customer_segment_csv_pipeline,
    cron_schedule="@daily",
)