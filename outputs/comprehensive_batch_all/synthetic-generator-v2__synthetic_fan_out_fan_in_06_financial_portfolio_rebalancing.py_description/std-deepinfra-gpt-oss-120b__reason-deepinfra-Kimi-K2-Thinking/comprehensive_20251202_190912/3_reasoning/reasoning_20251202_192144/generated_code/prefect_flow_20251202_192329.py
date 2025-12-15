import csv
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from prefect import flow, task


TARGET_ALLOCATIONS = {
    'AAPL': 0.30,
    'GOOGL': 0.25,
    'MSFT': 0.20,
    'CASH': 0.25
}
REBALANCING_THRESHOLD = 0.02


@task
def fetch_holdings(brokerage_id: str) -> pd.DataFrame:
    """Simulate fetching holdings CSV from a brokerage account."""
    mock_data = {
        'BROKERAGE_001': [
            {'symbol': 'AAPL', 'quantity': 100, 'price': 150.0},
            {'symbol': 'GOOGL', 'quantity': 50, 'price': 2800.0},
            {'symbol': 'CASH', 'quantity': 10000, 'price': 1.0}
        ],
        'BROKERAGE_002': [
            {'symbol': 'MSFT', 'quantity': 200, 'price': 300.0},
            {'symbol': 'AAPL', 'quantity': 75, 'price': 150.0},
            {'symbol': 'CASH', 'quantity': 5000, 'price': 1.0}
        ],
        'BROKERAGE_003': [
            {'symbol': 'GOOGL', 'quantity': 30, 'price': 2800.0},
            {'symbol': 'MSFT', 'quantity': 150, 'price': 300.0},
            {'symbol': 'AAPL', 'quantity': 50, 'price': 150.0}
        ],
        'BROKERAGE_004': [
            {'symbol': 'AAPL', 'quantity': 120, 'price': 150.0},
            {'symbol': 'CASH', 'quantity': 15000, 'price': 1.0}
        ],
        'BROKERAGE_005': [
            {'symbol': 'MSFT', 'quantity': 100, 'price': 300.0},
            {'symbol': 'GOOGL', 'quantity': 40, 'price': 2800.0},
            {'symbol': 'CASH', 'quantity': 8000, 'price': 1.0}
        ]
    }
    
    data = mock_data.get(brokerage_id, [])
    df = pd.DataFrame(data)
    df['brokerage_id'] = brokerage_id
    return df


@task
def analyze_portfolio(holdings: pd.DataFrame) -> Dict[str, Any]:
    """Analyze a single portfolio and calculate metrics."""
    if holdings.empty:
        return {
            'brokerage_id': None,
            'total_value': 0.0,
            'allocations': {},
            'risk_score': 0.0
        }
    
    holdings['value'] = holdings['quantity'] * holdings['price']
    total_value = holdings['value'].sum()
    
    allocations = {}
    for symbol in TARGET_ALLOCATIONS.keys():
        symbol_value = holdings[holdings['symbol'] == symbol]['value'].sum()
        allocations[symbol] = symbol_value / total_value if total_value > 0 else 0.0
    
    risk_score = min(len(holdings), 10)
    
    return {
        'brokerage_id': holdings['brokerage_id'].iloc[0],
        'total_value': float(total_value),
        'allocations': allocations,
        'risk_score': float(risk_score)
    }


@task
def aggregate_analyses(analyses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate all portfolio analyses and determine rebalancing trades."""
    if not analyses:
        return []
    
    combined_value = sum(analysis['total_value'] for analysis in analyses)
    current_allocations = {symbol: 0.0 for symbol in TARGET_ALLOCATIONS.keys()}
    
    for analysis in analyses:
        weight = analysis['total_value'] / combined_value if combined_value > 0 else 0.0
        for symbol, allocation in analysis['allocations'].items():
            current_allocations[symbol] += allocation * weight
    
    trades = []
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = current_allocations[symbol]
        diff = current_pct - target_pct
        
        if abs(diff) > REBALANCING_THRESHOLD:
            trade_amount = diff * combined_value
            action = 'SELL' if diff > 0 else 'BUY'
            
            trades.append({
                'symbol': symbol,
                'action': action,
                'amount': abs(trade_amount),
                'current_allocation': current_pct,
                'target_allocation': target_pct,
                'difference': diff
            })
    
    return trades


@task
def generate_trade_orders_csv(trades: List[Dict[str, Any]]) -> str:
    """Generate CSV file with rebalancing trade orders."""
    if not trades:
        return "no_trades_needed.csv"
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"trade_orders_{timestamp}.csv"
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['symbol', 'action', 'amount', 'current_allocation',
                     'target_allocation', 'difference']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade)
    
    return filename


@flow
def portfolio_rebalancing_flow():
    """Main flow for portfolio rebalancing across multiple brokerage accounts.
    
    Note: For daily scheduling, deploy with a Cron schedule:
    schedule=Cron("0 9 * * *")  # Daily at 9 AM
    """
    brokerage_ids = [f'BROKERAGE_{i:03d}' for i in range(1, 6)]
    
    # Fan-out: Fetch holdings from all brokerages in parallel
    fetch_tasks = [fetch_holdings.submit(bid) for bid in brokerage_ids]
    
    # Fan-in/Fan-out: Analyze each portfolio in parallel
    analyze_tasks = [analyze_portfolio.submit(fetch_task) for fetch_task in fetch_tasks]
    
    # Wait for all analyses and aggregate
    analyses = [task.result() for task in analyze_tasks]
    trades = aggregate_analyses(analyses)
    
    # Generate final CSV
    output_file = generate_trade_orders_csv(trades)
    
    return output_file


if __name__ == '__main__':
    result = portfolio_rebalancing_flow()
    print(f"Flow completed. Trade orders saved to: {result}")