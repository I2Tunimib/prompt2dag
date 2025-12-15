from dagster import op, job, ResourceDefinition, multiprocess_executor, fs_io_manager

# Resource definitions (placeholders)
local_filesystem_resource = ResourceDefinition.hardcoded_resource(None)
sms_gateway_resource = ResourceDefinition.hardcoded_resource(None)
push_service_resource = ResourceDefinition.hardcoded_resource(None)
email_service_resource = ResourceDefinition.hardcoded_resource(None)

# Pre-generated task definitions (use exactly as provided)

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


# Job definition with fanout pattern
@job(
    name="load_customer_segment_csv_pipeline",
    description="Comprehensive Pipeline Description",
    executor_def=multiprocess_executor,
    resource_defs={
        "local_filesystem": local_filesystem_resource,
        "sms_gateway": sms_gateway_resource,
        "push_service": push_service_resource,
        "email_service": email_service_resource,
    },
    io_manager_def=fs_io_manager,
)
def load_customer_segment_csv_pipeline():
    # Entry point
    load_op = load_customer_segment_csv()
    # Fanout to downstream ops
    load_op >> send_email_campaign()
    load_op >> send_sms_campaign()
    load_op >> send_push_notification()