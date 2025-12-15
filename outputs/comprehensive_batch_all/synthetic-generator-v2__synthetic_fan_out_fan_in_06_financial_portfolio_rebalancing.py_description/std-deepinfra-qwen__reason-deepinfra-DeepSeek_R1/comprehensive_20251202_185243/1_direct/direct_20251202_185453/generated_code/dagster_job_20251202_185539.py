from dagster import op, job, In, Out, ResourceDefinition, String, Field

# Resource for simulating API calls to brokerage accounts
class BrokerageAPI:
    def fetch_holdings(self, account_id: str) -> str:
        """Simulate fetching holdings data from a brokerage account."""
        return f"holdings_data_{account_id}.csv"

def brokerage_api_resource(config_schema: dict) -> ResourceDefinition:
    return ResourceDefinition(
        resource_fn=lambda init_context: BrokerageAPI(),
        config_schema=config_schema
    )

# Op to fetch holdings data from a brokerage account
@op(
    required_resource_keys={"brokerage_api"},
    out={"holdings_data": Out(dagster_type=str, description="CSV data of holdings")},
    config_schema={"account_id": Field(String, description="Brokerage account ID")},
)
def fetch_holdings(context):
    """Fetch holdings data from a brokerage account."""
    account_id = context.op_config["account_id"]
    api = context.resources.brokerage_api
    holdings_data = api.fetch_holdings(account_id)
    return holdings_data

# Op to analyze portfolio data
@op(
    ins={"holdings_data": In(dagster_type=str, description="CSV data of holdings")},
    out={"portfolio_analysis": Out(dagster_type=dict, description="Portfolio analysis results")},
)
def analyze_portfolio(holdings_data):
    """Analyze portfolio data to calculate total value, allocation percentages, and risk scores."""
    # Simulate portfolio analysis
    total_value = 100000  # Example total value
    allocation_percentages = {"AAPL": 0.35, "GOOGL": 0.20, "MSFT": 0.25, "CASH": 0.20}
    risk_score = 0.7  # Example risk score
    return {
        "total_value": total_value,
        "allocation_percentages": allocation_percentages,
        "risk_score": risk_score,
    }

# Op to aggregate analysis results
@op(
    ins={
        "portfolio_analysis_001": In(dagster_type=dict, description="Analysis results for account 001"),
        "portfolio_analysis_002": In(dagster_type=dict, description="Analysis results for account 002"),
        "portfolio_analysis_003": In(dagster_type=dict, description="Analysis results for account 003"),
        "portfolio_analysis_004": In(dagster_type=dict, description="Analysis results for account 004"),
        "portfolio_analysis_005": In(dagster_type=dict, description="Analysis results for account 005"),
    },
    out={"rebalancing_trades": Out(dagster_type=dict, description="Rebalancing trades needed")},
)
def aggregate_analysis(
    portfolio_analysis_001,
    portfolio_analysis_002,
    portfolio_analysis_003,
    portfolio_analysis_004,
    portfolio_analysis_005,
):
    """Aggregate portfolio analysis results and determine rebalancing trades."""
    combined_value = sum(
        pa["total_value"]
        for pa in [portfolio_analysis_001, portfolio_analysis_002, portfolio_analysis_003, portfolio_analysis_004, portfolio_analysis_005]
    )
    combined_allocation = {
        symbol: sum(pa["allocation_percentages"].get(symbol, 0) for pa in [portfolio_analysis_001, portfolio_analysis_002, portfolio_analysis_003, portfolio_analysis_004, portfolio_analysis_005]) / 5
        for symbol in ["AAPL", "GOOGL", "MSFT", "CASH"]
    }
    target_allocations = {"AAPL": 0.30, "GOOGL": 0.25, "MSFT": 0.20, "CASH": 0.25}
    rebalancing_trades = {}
    for symbol, target in target_allocations.items():
        current_allocation = combined_allocation[symbol]
        if abs(current_allocation - target) > 0.02:
            rebalancing_trades[symbol] = {
                "action": "BUY" if current_allocation < target else "SELL",
                "amount": abs(current_allocation - target) * combined_value,
                "allocation_percentage": current_allocation,
            }
    return rebalancing_trades

# Op to generate trade orders CSV
@op(
    ins={"rebalancing_trades": In(dagster_type=dict, description="Rebalancing trades needed")},
    out={"trade_orders_csv": Out(dagster_type=str, description="CSV file of trade orders")},
)
def generate_trade_orders(rebalancing_trades):
    """Generate a CSV file containing all rebalancing trade orders."""
    import csv
    from io import StringIO

    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(["Symbol", "Action", "Amount", "Allocation Percentage"])
    for symbol, trade in rebalancing_trades.items():
        csv_writer.writerow([symbol, trade["action"], trade["amount"], trade["allocation_percentage"]])
    return csv_buffer.getvalue()

# Define the job
@job(
    resource_defs={
        "brokerage_api": brokerage_api_resource(config_schema={"base_url": Field(String, default_value="https://api.example.com")})
    }
)
def financial_rebalancing_pipeline():
    fetch_001 = fetch_holdings.alias("fetch_holdings_001")(config={"account_id": "BROKERAGE_001"})
    fetch_002 = fetch_holdings.alias("fetch_holdings_002")(config={"account_id": "BROKERAGE_002"})
    fetch_003 = fetch_holdings.alias("fetch_holdings_003")(config={"account_id": "BROKERAGE_003"})
    fetch_004 = fetch_holdings.alias("fetch_holdings_004")(config={"account_id": "BROKERAGE_004"})
    fetch_005 = fetch_holdings.alias("fetch_holdings_005")(config={"account_id": "BROKERAGE_005"})

    analyze_001 = analyze_portfolio.alias("analyze_portfolio_001")(fetch_001)
    analyze_002 = analyze_portfolio.alias("analyze_portfolio_002")(fetch_002)
    analyze_003 = analyze_portfolio.alias("analyze_portfolio_003")(fetch_003)
    analyze_004 = analyze_portfolio.alias("analyze_portfolio_004")(fetch_004)
    analyze_005 = analyze_portfolio.alias("analyze_portfolio_005")(fetch_005)

    aggregate = aggregate_analysis(
        portfolio_analysis_001=analyze_001,
        portfolio_analysis_002=analyze_002,
        portfolio_analysis_003=analyze_003,
        portfolio_analysis_004=analyze_004,
        portfolio_analysis_005=analyze_005,
    )

    generate_trade_orders(aggregate)

# Execute the job
if __name__ == "__main__":
    result = financial_rebalancing_pipeline.execute_in_process()
    print(result.output_for_node("generate_trade_orders"))