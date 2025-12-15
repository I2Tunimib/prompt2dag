from dagster import job, op, multiprocess_executor, fs_io_manager, resource

@op(
    name='analyze_transactions',
    description='Analyze Transactions',
)
def analyze_transactions(context):
    """Op: Analyze Transactions"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='route_transaction',
    description='Route Transaction',
)
def route_transaction(context):
    """Op: Route Transaction"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='route_to_auto_approve',
    description='Route to Auto Approve',
)
def route_to_auto_approve(context):
    """Op: Route to Auto Approve"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='route_to_manual_review',
    description='Route to Manual Review',
)
def route_to_manual_review(context):
    """Op: Route to Manual Review"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_notification',
    description='Send Notification',
)
def send_notification(context):
    """Op: Send Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@resource
def file_system():
    pass

@resource
def payment_processing_system():
    pass

@resource
def notification_system():
    pass

@resource
def manual_review_queue():
    pass

@job(
    name='analyze_transactions_pipeline',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        'file_system': file_system,
        'payment_processing_system': payment_processing_system,
        'notification_system': notification_system,
        'manual_review_queue': manual_review_queue,
    },
    io_manager_def=fs_io_manager,
)
def analyze_transactions_pipeline():
    analyze_transactions_output = analyze_transactions()
    route_transaction_output = route_transaction(analyze_transactions_output)
    route_to_manual_review_output = route_to_manual_review(route_transaction_output)
    route_to_auto_approve_output = route_to_auto_approve(route_transaction_output)
    send_notification(route_to_manual_review_output, route_to_auto_approve_output)