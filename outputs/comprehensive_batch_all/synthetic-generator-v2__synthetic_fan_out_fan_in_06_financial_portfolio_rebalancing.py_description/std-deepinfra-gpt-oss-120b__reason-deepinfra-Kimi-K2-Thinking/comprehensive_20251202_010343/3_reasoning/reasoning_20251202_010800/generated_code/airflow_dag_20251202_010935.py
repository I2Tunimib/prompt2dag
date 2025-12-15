import csv
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'financial-team',
    'depends_on_past': False,
    'email': ['finance-ops@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_holdings(account_id, **context):
    """Simulates fetching holdings CSV from a brokerage account."""
    holdings = [
        {'symbol': 'AAPL', 'quantity': 150, 'price': 175.50},
        {'symbol': 'GOOGL', 'quantity': 50, 'price': 138.20},
        {'symbol': 'MSFT', 'quantity': 100, 'price': 380.75},
        {'symbol': 'CASH', 'quantity': 5000, 'price': 1.00},
    ]
    
    output_dir = '/tmp/portfolio_data'
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = f'{output_dir}/{account_id}_holdings.csv'
    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['symbol', 'quantity', 'price'])
        writer.writeheader()
        writer.writerows(holdings)
    
    context['task_instance'].xcom_push(key='holdings_path', value=file_path)
    return file_path


def analyze_portfolio(account_id, **context):
    """Analyzes a single portfolio: calculates value, allocations, and risk score."""
    ti = context['task_instance']
    holdings_path = ti.xcom_pull(task_ids=f'fetch_{account_id}', key='holdings_path')
    
    if not holdings_path or not os.path.exists(holdings_path):
        raise FileNotFoundError(f'Holdings file not found for {account_id}')
    
    holdings = []
    with open(holdings_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['quantity'] = float(row['quantity'])
            row['price'] = float(row['price'])
            holdings.append(row)
    
    total_value = sum(h['quantity'] * h['price'] for h in holdings)
    
    for h in holdings:
        h['allocation'] = (h['quantity'] * h['price'] / total_value) * 100
    
    risk_score = min(len(holdings) * 2, 10)
    
    analysis = {
        'account_id': account_id,
        'total_value': total_value,
        'holdings': holdings,
        'risk_score': risk_score,
    }
    
    ti.xcom_push(key='analysis', value=json.dumps(analysis))
    return analysis


def aggregate_analyses(**context):
    """Aggregates all portfolio analyses and calculates rebalancing trades."""
    ti = context['task_instance']
    account_ids = [f'BROKERAGE_{i:03d}' for i in range(1, 6)]
    
    analyses = []
    total_combined_value = 0
    
    for account_id in account_ids:
        analysis_json = ti.xcom_pull(task_ids=f'analyze_{account_id}', key='analysis')
        if analysis_json:
            analysis = json.loads(analysis_json)
            analyses.append(analysis)
            total_combined_value += analysis['total_value']
    
    if not analyses:
        raise ValueError('No analyses found for aggregation')
    
    combined_holdings = {}
    for analysis in analyses:
        for h in analysis['holdings']:
            symbol = h['symbol']
            value = h['quantity'] * h['price']
            combined_holdings[symbol] = combined_holdings.get(symbol, 0) + value
    
    current_allocations = {
        symbol: (value / total_combined_value) * 100
        for symbol, value in combined_holdings.items()
    }
    
    TARGET_ALLOCATIONS = {
        'AAPL': 30.0,
        'GOOGL': 25.0,
        'MSFT': 20.0,
        'CASH': 25.0,
    }
    
    REBALANCE_THRESHOLD = 2.0
    trades = []
    
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = current_allocations.get(symbol, 0)
        diff = current_pct - target_pct
        
        if abs(diff) > REBALANCE_THRESHOLD:
            trade_value = (diff / 100) * total_combined_value
            action = 'SELL' if diff > 0 else 'BUY'
            
            trades.append({
                'symbol': symbol,
                'action': action,
                'trade_value': abs(trade_value),
                'current_allocation': current_pct,
                'target_allocation': target_pct,
                'difference': diff,
            })
    
    aggregation_result = {
        'total_combined_value': total_combined_value,
        'current_allocations': current_allocations,
        'target_allocations': TARGET_ALLOCATIONS,
        'trades': trades,
    }
    
    ti.xcom_push(key='aggregation', value=json.dumps(aggregation_result))
    return aggregation_result


def generate_trade_orders(**context):
    """Generates the final trade orders CSV file."""
    ti = context['task_instance']
    aggregation_json = ti.xcom_pull(task_ids='aggregate_analyses', key='aggregation')
    
    if not aggregation_json:
        raise ValueError('No aggregation result found')
    
    aggregation = json.loads(aggregation_json)
    trades = aggregation['trades']
    
    output_dir = '/tmp/portfolio_data'
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = f'{output_dir}/rebalancing_trade_orders.csv'
    
    with open(output_path, 'w', newline='') as f:
        fieldnames = ['symbol', 'action', 'trade_value', 'current_allocation', 
                     'target_allocation', 'difference']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)
    
    print(f'Trade orders generated: {output_path}')
    return output_path


with DAG(
    dag_id='financial_portfolio_rebalancing',
    default_args=default_args,
    description='Daily financial portfolio rebalancing across multiple brokerage accounts',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'portfolio', 'rebalancing'],
) as dag:
    
    # Fan-out: Fetch holdings from 5 brokerage accounts
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_BROKERAGE_{i:03d}',
            python_callable=fetch_holdings,
            op_kwargs={'account_id': f'BROKERAGE_{i:03d}'},
        )
        for i in range(1, 6)
    ]
    
    # Fan-out: Analyze each portfolio independently
    analyze_tasks = [
        PythonOperator(
            task_id=f'analyze_BROKERAGE_{i:03d}',
            python_callable=analyze_portfolio,
            op_kwargs={'account_id': f'BROKERAGE_{i:03d}'},
        )
        for i in range(1, 6)
    ]
    
    # Fan-in: Aggregate all analyses
    aggregate_task = PythonOperator(
        task_id='aggregate_analyses',
        python_callable=aggregate_analyses,
    )
    
    # Generate final trade orders
    generate_task = PythonOperator(
        task_id='generate_trade_orders',
        python_callable=generate_trade_orders,
    )
    
    # Define dependencies: fetch -> analyze -> aggregate -> generate
    for fetch_task, analyze_task in zip(fetch_tasks, analyze_tasks):
        fetch_task >> analyze_task
    
    analyze_tasks >> aggregate_task >> generate_task