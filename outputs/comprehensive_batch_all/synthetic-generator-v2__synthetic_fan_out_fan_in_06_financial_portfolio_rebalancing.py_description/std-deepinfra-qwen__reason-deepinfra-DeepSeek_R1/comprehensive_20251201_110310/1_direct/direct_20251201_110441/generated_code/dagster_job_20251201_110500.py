from dagster import op, job, Out, In, Nothing

# Mock functions to simulate data fetching and processing
def fetch_holdings_data(brokerage_id):
    # Simulate fetching CSV data from a brokerage account
    return f"holdings_data_{brokerage_id}.csv"

def analyze_portfolio(holdings_data):
    # Simulate portfolio analysis
    return {
        "total_value": 100000,
        "allocations": {"AAPL": 0.3, "GOOGL": 0.25, "MSFT": 0.2, "CASH": 0.25},
        "risk_score": 0.7
    }

def aggregate_analysis(analysis_results):
    # Simulate aggregation of analysis results
    combined_value = sum(result["total_value"] for result in analysis_results)
    combined_allocations = {
        "AAPL": sum(result["allocations"]["AAPL"] for result in analysis_results) / len(analysis_results),
        "GOOGL": sum(result["allocations"]["GOOGL"] for result in analysis_results) / len(analysis_results),
        "MSFT": sum(result["allocations"]["MSFT"] for result in analysis_results) / len(analysis_results),
        "CASH": sum(result["allocations"]["CASH"] for result in analysis_results) / len(analysis_results),
    }
    target_allocations = {"AAPL": 0.3, "GOOGL": 0.25, "MSFT": 0.2, "CASH": 0.25}
    rebalancing_trades = {
        symbol: (current - target) * combined_value
        for symbol, current in combined_allocations.items()
        for target in [target_allocations[symbol]]
        if abs(current - target) > 0.02
    }
    return combined_value, combined_allocations, rebalancing_trades

def generate_trade_orders(rebalancing_trades):
    # Simulate generating trade orders CSV
    return "trade_orders.csv"

# Define ops
@op(out={"holdings_data": Out()})
def fetch_holdings(brokerage_id):
    """Fetch holdings data from a brokerage account."""
    return fetch_holdings_data(brokerage_id)

@op(ins={"holdings_data": In()}, out={"portfolio_analysis": Out()})
def analyze_portfolio_op(holdings_data):
    """Analyze the portfolio and generate analysis results."""
    return analyze_portfolio(holdings_data)

@op(ins={"analysis_results": In()}, out={"aggregated_results": Out()})
def aggregate_analysis_op(analysis_results):
    """Aggregate analysis results from multiple portfolios."""
    return aggregate_analysis(analysis_results)

@op(ins={"rebalancing_trades": In()}, out={"trade_orders_csv": Out()})
def generate_trade_orders_op(rebalancing_trades):
    """Generate trade orders CSV file."""
    return generate_trade_orders(rebalancing_trades)

# Define the job
@job
def financial_rebalancing_job():
    fetch_tasks = [fetch_holdings(brokerage_id=f"BROKERAGE_{i:03d}") for i in range(1, 6)]
    analyze_tasks = [analyze_portfolio_op(fetch_task) for fetch_task in fetch_tasks]
    aggregated_results = aggregate_analysis_op(analysis_results=analyze_tasks)
    generate_trade_orders_op(rebalancing_trades=aggregated_results)

# Launch pattern
if __name__ == '__main__':
    result = financial_rebalancing_job.execute_in_process()