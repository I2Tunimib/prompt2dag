from dagster import job, op, multiprocess_executor, fs_io_manager, resource, ScheduleDefinition

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

@job(
    name="load_customer_segment_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "sms_delivery_gateway": resource(config_schema={"url": str}),
        "push_notification_service": resource(config_schema={"url": str}),
        "local_filesystem": fs_io_manager,
        "email_delivery_system": resource(config_schema={"url": str}),
    },
)
def load_customer_segment_csv_pipeline():
    customer_segment_data = load_customer_segment_csv()
    send_email_campaign(customer_segment_data)
    send_sms_campaign(customer_segment_data)
    send_push_notification(customer_segment_data)

daily_schedule = ScheduleDefinition(
    job=load_customer_segment_csv_pipeline,
    cron_schedule="@daily",
)