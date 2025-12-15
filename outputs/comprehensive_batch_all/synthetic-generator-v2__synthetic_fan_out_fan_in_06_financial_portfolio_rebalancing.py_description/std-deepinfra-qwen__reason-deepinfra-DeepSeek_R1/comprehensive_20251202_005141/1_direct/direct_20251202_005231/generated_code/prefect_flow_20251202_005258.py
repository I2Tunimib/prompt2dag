from prefect import flow, task
import pandas as pd

# Mock function to simulate fetching data from a brokerage account
def fetch_data_from_brokerage(brokerage_id):
    # Simulate fetching data
    data = {
        'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
        'quantity': [100, 50, 75, 2500],
        'price': [150.0, 2500.0, 300.0, 1.0]
    }
    return pd.DataFrame(data)

# Task to fetch holdings data from a brokerage account
@task
def fetch_holdings(brokerage_id):
    return fetch_data_from_brokerage(brokerage_id)

# Task to analyze a portfolio
@task
def analyze_portfolio(holdings):
    total_value = (holdings['quantity'] * holdings['price']).sum()
    holdings['allocation_percentage'] = (holdings['quantity'] * holdings['price']) / total_value * 100
    risk_score = len(holdings)
    return {
        'total_value': total_value,
        'allocation_percentages': holdings[['symbol', 'allocation_percentage']].to_dict(orient='records'),
        'risk_score': risk_score
    }

# Task to aggregate analysis results
@task
def aggregate_analysis(results):
    combined_value = sum(result['total_value'] for result in results)
    combined_allocations = {}
    for result in results:
        for allocation in result['allocation_percentages']:
            symbol = allocation['symbol']
            percentage = allocation['allocation_percentage']
            if symbol in combined_allocations:
                combined_allocations[symbol] += percentage
            else:
                combined_allocations[symbol] = percentage
    combined_allocations = {k: v / len(results) for k, v in combined_allocations.items()}
    
    target_allocations = {'AAPL': 30, 'GOOGL': 25, 'MSFT': 20, 'CASH': 25}
    rebalancing_trades = []
    for symbol, target_percentage in target_allocations.items():
        current_percentage = combined_allocations.get(symbol, 0)
        if abs(current_percentage - target_percentage) > 2:
            action = 'BUY' if current_percentage < target_percentage else 'SELL'
            amount = (target_percentage - current_percentage) * combined_value / 100
            rebalancing_trades.append({
                'symbol': symbol,
                'action': action,
                'amount': amount,
                'allocation_percentage': target_percentage
            })
    return rebalancing_trades

# Task to generate trade orders CSV
@task
def generate_trade_orders_csv(trades):
    df = pd.DataFrame(trades)
    df.to_csv('trade_orders.csv', index=False)

# Main flow to orchestrate the pipeline
@flow
def financial_portfolio_rebalancing_flow():
    # Fetch holdings data from five brokerage accounts in parallel
    fetch_tasks = [fetch_holdings.submit(f'BROKERAGE_{i:03d}') for i in range(1, 6)]
    holdings_data = [fetch_task.result() for fetch_task in fetch_tasks]

    # Analyze each portfolio independently
    analyze_tasks = [analyze_portfolio.submit(holdings) for holdings in holdings_data]
    analysis_results = [analyze_task.result() for analyze_task in analyze_tasks]

    # Aggregate analysis results
    rebalancing_trades = aggregate_analysis(analysis_results)

    # Generate trade orders CSV
    generate_trade_orders_csv(rebalancing_trades)

if __name__ == '__main__':
    financial_portfolio_rebalancing_flow()