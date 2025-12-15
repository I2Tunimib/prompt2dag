from dagster import op, job, ResourceDefinition, multiprocess_executor, fs_io_manager


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
    name='load_customer_segment_csv_pipeline',
    description='No description provided.',
    resource_defs={
        'email_service': ResourceDefinition.hardcoded_resource(None),
        'fs_local': ResourceDefinition.hardcoded_resource(None),
        'sms_gateway': ResourceDefinition.hardcoded_resource(None),
        'push_service': ResourceDefinition.hardcoded_resource(None),
        'io_manager': fs_io_manager,
    },
    executor_def=multiprocess_executor,
)
def load_customer_segment_csv_pipeline():
    load = load_customer_segment_csv()
    send_email_campaign().after(load)
    send_sms_campaign().after(load)
    send_push_notification().after(load)