from dagster import job, op, multiprocess_executor, fs_io_manager, resource

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

@job(
    name="fetch_brokerage_holdings_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": fs_io_manager, "mock_brokerage_api": resource(lambda: None)},
)
def fetch_brokerage_holdings_pipeline():
    fetch_brokerage_holdings_output = fetch_brokerage_holdings()
    
    analyze_portfolio_1 = analyze_portfolio.alias("analyze_portfolio_1")(fetch_brokerage_holdings_output)
    analyze_portfolio_2 = analyze_portfolio.alias("analyze_portfolio_2")(fetch_brokerage_holdings_output)
    analyze_portfolio_3 = analyze_portfolio.alias("analyze_portfolio_3")(fetch_brokerage_holdings_output)
    analyze_portfolio_4 = analyze_portfolio.alias("analyze_portfolio_4")(fetch_brokerage_holdings_output)
    analyze_portfolio_5 = analyze_portfolio.alias("analyze_portfolio_5")(fetch_brokerage_holdings_output)
    
    aggregate_and_rebalance_1 = aggregate_and_rebalance.alias("aggregate_and_rebalance_1")(
        analyze_portfolio_1,
        analyze_portfolio_2,
        analyze_portfolio_3,
        analyze_portfolio_4,
        analyze_portfolio_5
    )
    
    generate_trade_orders(aggregate_and_rebalance_1)