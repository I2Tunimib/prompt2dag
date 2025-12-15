from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition


# Task definitions (use exactly as provided)

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


# Placeholder resource definitions (replace with real implementations as needed)

manual_review_queue = ResourceDefinition.hardcoded_resource(None)
notification_service = ResourceDefinition.hardcoded_resource(None)
payment_processing_system = ResourceDefinition.hardcoded_resource(None)
fs_transactions = ResourceDefinition.hardcoded_resource(None)


# Job definition with fanout/fanin pattern

@job(
    name="analyze_transactions_pipeline",
    description="Comprehensive Pipeline Description",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "manual_review_queue": manual_review_queue,
        "notification_service": notification_service,
        "payment_processing_system": payment_processing_system,
        "fs_transactions": fs_transactions,
    },
)
def analyze_transactions_pipeline():
    # Entry point
    analyze = analyze_transactions()

    # Fan‑out from analyze to routing
    route = route_transaction()
    route.after(analyze)

    # Fan‑out to two parallel branches
    manual = route_to_manual_review()
    manual.after(route)

    auto = route_to_auto_approve()
    auto.after(route)

    # Fan‑in: notification after both branches complete
    notify = send_notification()
    notify.after(manual)
    notify.after(auto)