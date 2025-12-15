from dagster import op, job, multiprocess_executor, fs_io_manager, ScheduleDefinition, Definitions

# Pre-Generated Task Definitions

@op(
    name='fetch_brokerage_holdings',
    description='Fetch Brokerage Holdings',
)
def fetch_brokerage_holdings(context):
    """Op: Fetch Brokerage Holdings"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='analyze_portfolio',
    description='Analyze Portfolio',
)
def analyze_portfolio(context):
    """Op: Analyze Portfolio"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='aggregate_and_rebalance',
    description='Aggregate and Rebalance',
)
def aggregate_and_rebalance(context):
    """Op: Aggregate and Rebalance"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='generate_trade_orders',
    description='Generate Trade Orders',
)
def generate_trade_orders(context):
    """Op: Generate Trade Orders"""
    # Docker execution
    # Image: python:3.9
    pass


# Job Definition with Sequential Pattern

@job(
    name='fetch_brokerage_holdings_pipeline',
    description='Portfolio Rebalancing DAG',
    executor_def=multiprocess_executor,
    resource_defs={'io_manager': fs_io_manager},
)
def fetch_brokerage_holdings_pipeline():
    fetch = fetch_brokerage_holdings()
    analyze = analyze_portfolio()
    aggregate = aggregate_and_rebalance()
    generate = generate_trade_orders()

    # Define sequential dependencies
    fetch >> analyze >> aggregate >> generate


# Daily Schedule

daily_schedule = ScheduleDefinition(
    job=fetch_brokerage_holdings_pipeline,
    cron_schedule='@daily',
    name='fetch_brokerage_holdings_pipeline_schedule',
    description='Daily schedule for the Portfolio Rebalancing DAG',
    execution_timezone='UTC',
)


# Definitions for Dagster UI / CLI

defs = Definitions(
    jobs=[fetch_brokerage_holdings_pipeline],
    schedules=[daily_schedule],
)