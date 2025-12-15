from prefect import flow, task
import pandas as pd

# Mock function to simulate fetching data from a brokerage account
def fetch_holdings(account_id: str) -> pd.DataFrame:
    # Simulate fetching data
    data = {
        'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
        'quantity': [100, 50, 75, 200],
        'price': [150.0, 2500.0, 300.0, 1.0]
    }
    return pd.DataFrame(data)

# Task to fetch holdings data from a brokerage account
@task
def fetch_account_holdings(account_id: str) -> pd.DataFrame:
    return fetch_holdings(account_id)

# Task to analyze a single portfolio
@task
def analyze_portfolio(holdings: pd.DataFrame) -> dict:
    total_value = (holdings['quantity'] * holdings['price']).sum()
    holdings['allocation'] = (holdings['quantity'] * holdings['price']) / total_value
    risk_score = len(holdings)
    return {
        'total_value': total_value,
        'allocations': holdings[['symbol', 'allocation']].to_dict(orient='records'),
        'risk_score': risk_score
    }

# Task to aggregate analysis results
@task
def aggregate_analysis(results: list) -> dict:
    combined_value = sum(result['total_value'] for result in results)
    combined_allocations = {}
    for result in results:
        for allocation in result['allocations']:
            symbol = allocation['symbol']
            if symbol not in combined_allocations:
                combined_allocations[symbol] = 0
            combined_allocations[symbol] += allocation['allocation'] * (result['total_value'] / combined_value)
    
    target_allocations = {'AAPL': 0.30, 'GOOGL': 0.25, 'MSFT': 0.20, 'CASH': 0.25}
    rebalancing_trades = []
    for symbol, current_allocation in combined_allocations.items():
        target_allocation = target_allocations.get(symbol, 0)
        if abs(current_allocation - target_allocation) > 0.02:
            action = 'BUY' if current_allocation < target_allocation else 'SELL'
            amount = abs(current_allocation - target_allocation) * combined_value
            rebalancing_trades.append({
                'symbol': symbol,
                'action': action,
                'amount': amount,
                'allocation': current_allocation
            })
    
    return {
        'combined_value': combined_value,
        'combined_allocations': combined_allocations,
        'rebalancing_trades': rebalancing_trades
    }

# Task to generate trade orders CSV
@task
def generate_trade_orders_csv(trades: list, output_path: str) -> None:
    trade_df = pd.DataFrame(trades)
    trade_df.to_csv(output_path, index=False)

# Main flow to orchestrate the pipeline
@flow
def financial_rebalancing_pipeline():
    account_ids = ['BROKERAGE_001', 'BROKERAGE_002', 'BROKERAGE_003', 'BROKERAGE_004', 'BROKERAGE_005']
    
    # Fetch holdings data from all accounts in parallel
    fetch_tasks = [fetch_account_holdings.submit(account_id) for account_id in account_ids]
    holdings_data = [fetch_task.result() for fetch_task in fetch_tasks]
    
    # Analyze each portfolio independently
    analyze_tasks = [analyze_portfolio.submit(holdings) for holdings in holdings_data]
    analysis_results = [analyze_task.result() for analyze_task in analyze_tasks]
    
    # Aggregate analysis results
    aggregated_results = aggregate_analysis(analysis_results)
    
    # Generate trade orders CSV
    output_path = 'rebalancing_trades.csv'
    generate_trade_orders_csv(aggregated_results['rebalancing_trades'], output_path)

if __name__ == '__main__':
    financial_rebalancing_pipeline()