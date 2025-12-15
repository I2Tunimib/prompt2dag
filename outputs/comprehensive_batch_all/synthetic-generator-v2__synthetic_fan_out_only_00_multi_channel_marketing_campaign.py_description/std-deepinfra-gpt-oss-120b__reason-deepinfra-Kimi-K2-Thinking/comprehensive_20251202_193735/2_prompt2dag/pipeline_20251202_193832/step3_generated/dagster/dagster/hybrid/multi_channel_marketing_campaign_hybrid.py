from dagster import op, job, resource, fs_io_manager, multiprocess_executor, schedule

# ----------------------------------------------------------------------
# Resource definitions (placeholders for actual implementations)
# ----------------------------------------------------------------------
@resource
def push_service_resource(_):
    """Placeholder push notification service resource."""
    return {"service": "push"}

@resource
def sms_gateway_resource(_):
    """Placeholder SMS gateway resource."""
    return {"gateway": "sms"}

@resource
def email_service_resource(_):
    """Placeholder email service resource."""
    return {"service": "email"}

# ----------------------------------------------------------------------
# Task (op) definitions – use exactly as provided
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# Job definition with fan‑out pattern
# ----------------------------------------------------------------------
@job(
    name="multi_channel_marketing_campaign",
    description="Executes a multi-channel marketing campaign by loading customer segment data and triggering parallel email, SMS, and push notification campaigns.",
    executor_def=multiprocess_executor,
    resource_defs={
        "push_service": push_service_resource,
        "sms_gateway": sms_gateway_resource,
        "email_service": email_service_resource,
        "local_fs": fs_io_manager,
    },
    io_manager_def=fs_io_manager,
)
def multi_channel_marketing_campaign():
    # Entry point
    load = load_customer_segment_csv()

    # Fan‑out to parallel channels, all dependent on the load step
    email = send_email_campaign()
    email = email.after(load)

    sms = send_sms_campaign()
    sms = sms.after(load)

    push = send_push_notification()
    push = push.after(load)

# ----------------------------------------------------------------------
# Daily schedule
# ----------------------------------------------------------------------
@schedule(
    cron_schedule="@daily",
    job=multi_channel_marketing_campaign,
    description="Daily execution of the multi‑channel marketing campaign.",
)
def daily_marketing_schedule():
    return {}