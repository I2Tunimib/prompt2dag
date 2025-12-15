from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition


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
    name='process_auto_approve',
    description='Process Auto Approval',
)
def process_auto_approve(context):
    """Op: Process Auto Approval"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='process_manual_review',
    description='Process Manual Review',
)
def process_manual_review(context):
    """Op: Process Manual Review"""
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


@job(
    name="fraud_detection_triage",
    description="No description provided.",
    resource_defs={
        "fs_transactions": ResourceDefinition.hardcoded_resource(None),
        "queue_manual_review": ResourceDefinition.hardcoded_resource(None),
        "payment_system": ResourceDefinition.hardcoded_resource(None),
        "notification_service": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
    executor_def=multiprocess_executor,
)
def fraud_detection_triage():
    # Entry point
    analyze = analyze_transactions()
    # Fan‑out
    route = route_transaction().set_dependency(analyze)
    # Parallel processing
    auto_approve = process_auto_approve().set_dependency(route)
    manual_review = process_manual_review().set_dependency(route)
    # Fan‑in
    send_notification().set_dependency([auto_approve, manual_review])