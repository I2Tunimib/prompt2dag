from dagster import job, op, multiprocess_executor, fs_io_manager, ResourceDefinition

# Task Definitions
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

# Job Definition
@job(
    name="analyze_transactions_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "manual_review_queue": ResourceDefinition.mock_resource(),
        "payment_processing_system": ResourceDefinition.mock_resource(),
        "notification_system": ResourceDefinition.mock_resource(),
        "file_system": ResourceDefinition.mock_resource(),
    },
    io_manager_def=fs_io_manager,
)
def analyze_transactions_pipeline():
    analyze_transactions_op = analyze_transactions()
    route_transaction_op = route_transaction(analyze_transactions_op)
    route_to_manual_review_op = route_to_manual_review(route_transaction_op)
    route_to_auto_approve_op = route_to_auto_approve(route_transaction_op)
    send_notification_op = send_notification(
        route_to_manual_review_op,
        route_to_auto_approve_op
    )