from prefect import flow, task
import pandas as pd

# Mock function to simulate fetching data from a brokerage account
def fetch_mock_data(account_id):
    return pd.DataFrame({
        'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
        'quantity': [100, 50, 75, 200],
        'price': [150.0, 2500.0, 300.0, 1.0]
    })

@task
def fetch_holdings_data(account_id: str) -> pd.DataFrame:
    """Fetch holdings data from a brokerage account."""
    return fetch_mock_data(account_id)

@task
def analyze_portfolio(holdings_data: pd.DataFrame) -> dict:
    """Analyze a single portfolio and return analysis results."""
    total_value = (holdings_data['quantity'] * holdings_data['price']).sum()
    allocation_percentages = (holdings_data['quantity'] * holdings_data['price']) / total_value
    risk_score = len(holdings_data)
    return {
        'total_value': total_value,
        'allocation_percentages': allocation_percentages.to_dict(),
        'risk_score': risk_score
    }

@task
def aggregate_analysis_results(analyses: list) -> dict:
    """Aggregate analysis results from multiple portfolios."""
    combined_value = sum(analysis['total_value'] for analysis in analyses)
    combined_allocations = {symbol: 0 for symbol in ['AAPL', 'GOOGL', 'MSFT', 'CASH']}
    
    for analysis in analyses:
        for symbol, percentage in analysis['allocation_percentages'].items():
            combined_allocations[symbol] += percentage * analysis['total_value']
    
    combined_allocations = {symbol: value / combined_value for symbol, value in combined_allocations.items()}
    
    target_allocations = {'AAPL': 0.30, 'GOOGL': 0.25, 'MSFT': 0.20, 'CASH': 0.25}
    rebalancing_trades = {}
    
    for symbol, current_allocation in combined_allocations.items():
        target_allocation = target_allocations[symbol]
        if abs(current_allocation - target_allocation) > 0.02:
            rebalancing_trades[symbol] = target_allocation - current_allocation
    
    return {
        'combined_value': combined_value,
        'combined_allocations': combined_allocations,
        'rebalancing_trades': rebalancing_trades
    }

@task
def generate_trade_orders(analysis_results: dict) -> pd.DataFrame:
    """Generate trade orders based on rebalancing needs."""
    trades = []
    for symbol, adjustment in analysis_results['rebalancing_trades'].items():
        action = 'BUY' if adjustment > 0 else 'SELL'
        amount = abs(adjustment) * analysis_results['combined_value']
        trades.append({
            'symbol': symbol,
            'action': action,
            'amount': amount,
            'allocation_percentage': abs(adjustment)
        })
    return pd.DataFrame(trades)

@flow
def financial_rebalancing_pipeline():
    """Financial portfolio rebalancing pipeline."""
    account_ids = ['BROKERAGE_001', 'BROKERAGE_002', 'BROKERAGE_003', 'BROKERAGE_004', 'BROKERAGE_005']
    
    # Fetch holdings data from all accounts in parallel
    holdings_data = [fetch_holdings_data.submit(account_id) for account_id in account_ids]
    
    # Analyze each portfolio independently
    analyses = [analyze_portfolio.submit(holdings) for holdings in holdings_data]
    
    # Aggregate analysis results
    aggregated_results = aggregate_analysis_results(analyses)
    
    # Generate trade orders
    trade_orders = generate_trade_orders(aggregated_results)
    
    return trade_orders

if __name__ == '__main__':
    # Run the flow locally
    trade_orders = financial_rebalancing_pipeline()
    print(trade_orders)