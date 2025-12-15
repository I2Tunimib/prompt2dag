from dagster import op, job, ResourceDefinition, multiprocess_executor, fs_io_manager

# ----------------------------------------------------------------------
# Task Definitions (provided exactly as given)
# ----------------------------------------------------------------------


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
    description='Route to Auto‑Approve',
)
def route_to_auto_approve(context):
    """Op: Route to Auto‑Approve"""
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


# ----------------------------------------------------------------------
# Job Definition
# ----------------------------------------------------------------------


@job(
    name='fraud_detection_triage',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        "manual_review_queue_conn": ResourceDefinition.hardcoded_resource(None),
        "notification_system_conn": ResourceDefinition.hardcoded_resource(None),
        "payment_processing_conn": ResourceDefinition.hardcoded_resource(None),
        "filesystem_conn": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def fraud_detection_triage():
    """Job implementing the fanout_fanin pattern for fraud detection triage."""

    # Entry point
    analyzed = analyze_transactions()

    # Fan‑out: route based on analysis
    routed = route_transaction(analyzed)

    # Parallel branches
    manual_review = route_to_manual_review(routed)
    auto_approve = route_to_auto_approve(routed)

    # Fan‑in: send notification after both branches complete
    send_notification().after(manual_review, auto_approve)