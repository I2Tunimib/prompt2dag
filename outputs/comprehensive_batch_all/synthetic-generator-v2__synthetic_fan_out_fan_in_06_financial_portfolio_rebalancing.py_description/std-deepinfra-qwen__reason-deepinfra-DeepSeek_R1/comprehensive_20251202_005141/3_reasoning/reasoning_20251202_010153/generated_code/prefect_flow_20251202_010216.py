from prefect import flow, task
from typing import List, Dict
import pandas as pd
import numpy as np

@task
def fetch_holdings(account_id: str) -> pd.DataFrame:
    """Simulate fetching holdings data from brokerage API"""
    # Mock data generation
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'CASH']
    np.random.seed(hash(account_id) % 1000)
    return pd.DataFrame({
        'symbol': symbols,
        'quantity': np.random.randint(1, 100, size=len(symbols)),
        'price': np.random.uniform(50, 500, size=len(symbols))
    })

@task
def analyze_portfolio(holdings: pd.DataFrame) -> Dict:
    """Analyze portfolio and calculate metrics"""
    total_value = (holdings['quantity'] * holdings['price']).sum()
    allocations = (holdings['quantity'] * holdings['price']) / total_value
    return {
        'total_value': total_value,
        'allocations': dict(zip(holdings['symbol'], allocations)),
        'risk_score': len(holdings) / 10  # Simplified risk metric
    }

@task
def aggregate_analyses(results: List[Dict]) -> Dict:
    """Aggregate analysis results and calculate rebalancing needs"""
    combined_value = sum(r['total_value'] for r in results)
    target_allocations = {'AAPL': 0.3, 'GOOGL': 0.25, 'MSFT': 0.2, 'CASH': 0.25}
    
    current_allocations = {}
    for r in results:
        for sym, alloc in r['allocations'].items():
            current_allocations[sym] = current_allocations.get(sym, 0) + (alloc * r['total_value'] / combined_value)
    
    rebalancing = {}
    for sym, target in target_allocations.items():
        current = current_allocations.get(sym, 0)
        if abs(current - target) > 0.02:
            rebalancing[sym] = {
                'action': 'BUY' if current < target else 'SELL',
                'amount': combined_value * abs(target - current),
                'target_alloc': target,
                'current_alloc': current
            }
    
    return {
        'combined_value': combined_value,
        'rebalancing_actions': rebalancing
    }

@task
def generate_trade_orders(aggregated: Dict) -> None:
    """Generate CSV file with trade orders"""
    df = pd.DataFrame.from_dict(
        {k: v for k, v in aggregated['rebalancing_actions'].items()},
        orient='index'
    )
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'symbol'}, inplace=True)
    df.to_csv('trade_orders.csv', index=False)

@flow
def portfolio_rebalancing_flow(account_ids: List[str] = ['BROKERAGE_001', 'BROKERAGE_002', 
                                                         'BROKERAGE_003', 'BROKERAGE_004', 
                                                         'BROKERAGE_005']):
    # Schedule comment: Configure deployment with daily schedule
    # Example: prefect deploy --name daily-rebalance --schedule "0 9 * * *"
    
    # Parallel fetch and analysis
    fetch_tasks = [fetch_holdings.submit(acc_id) for acc_id in account_ids]
    analysis_tasks = [analyze_portfolio.submit(task.result()) for task in fetch_tasks]
    
    # Aggregation and generation
    aggregated = aggregate_analyses.submit([task.result() for task in analysis_tasks])
    generate_trade_orders.submit(aggregated.result())

if __name__ == '__main__':
    portfolio_rebalancing_flow()