from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import json


default_args = {
    'owner': 'financial-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_holdings(brokerage_id, **context):
    """Simulate fetching holdings data from a brokerage account."""
    mock_data = {
        'BROKERAGE_001': """symbol,quantity,price
AAPL,100,150.00
GOOGL,50,2800.00
MSFT,75,300.00
CASH,5000,1.00""",
        'BROKERAGE_002': """symbol,quantity,price
AAPL,80,150.00
GOOGL,60,2800.00
MSFT,90,300.00
CASH,4000,1.00""",
        'BROKERAGE_003': """symbol,quantity,price
AAPL,120,150.00
GOOGL,40,2800.00
MSFT,60,300.00
CASH,6000,1.00""",
        'BROKERAGE_004': """symbol,quantity,price
AAPL,90,150.00
GOOGL,55,2800.00
MSFT,80,300.00
CASH,4500,1.00""",
        'BROKERAGE_005': """symbol,quantity,price
AAPL,110,150.00
GOOGL,45,2800.00
MSFT,70,300.00
CASH,5500,1.00""",
    }
    return mock_data[brokerage_id]


def analyze_portfolio(brokerage_id, **context):
    """Analyze a single portfolio's holdings."""
    ti = context['ti']
    account_num = brokerage_id.split('_')[-1]
    csv_data = ti.xcom_pull(task_ids=f'fetch_brokerage_{account_num}')
    
    df = pd.read_csv(StringIO(csv_data))
    df['value'] = df['quantity'] * df['price']
    total_value = df['value'].sum()
    
    df['allocation_pct'] = (df['value'] / total_value) * 100
    holdings_count = len(df)
    risk_score = max(0, 100 - (holdings_count * 10))
    
    allocations = df.set_index('symbol')['allocation_pct'].to_dict()
    
    return {
        'brokerage_id': brokerage_id,
        'total_value': total_value,
        'allocations': allocations,
        'risk_score': risk_score,
        'holdings_count': holdings_count
    }


def aggregate_portfolios(**context):
    """Aggregate all portfolio analyses and calculate rebalancing trades."""
    ti = context['ti']
    
    analysis_results = []
    for i in range(1, 6):
        result = ti.xcom_pull(task_ids=f'analyze_portfolio_{i:03d}')
        analysis_results.append(result)
    
    target_allocations = {
        'AAPL': 30.0,
        'GOOGL': 25.0,
        'MSFT': 20.0,
        'CASH': 25.0
    }
    
    combined_holdings = {}
    total_combined_value = 0
    
    for result in analysis_results:
        total_combined_value += result['total_value']
        for symbol, pct in result['allocations'].items():
            value = (pct / 100) * result['total_value']
            combined_holdings[symbol] = combined_holdings.get(symbol, 0) + value
    
    current_allocations = {
        symbol: (value / total_combined_value) * 100
        for symbol, value in combined_holdings.items()
    }
    
    rebalancing_trades = []
    tolerance = 2.0
    
    for symbol, target_pct in target_allocations.items():
        current_pct = current_allocations.get(symbol, 0)
        diff = current_pct - target_pct
        
        if abs(diff) > tolerance:
            trade_value = (diff / 100) * total_combined_value
            action = 'SELL' if diff > 0 else 'BUY'
            
            rebalancing_trades.append({
                'symbol': symbol,
                'action': action,
                'trade_value': abs(trade_value),
                'current_pct': current_pct,
                'target_pct': target_pct,
                'diff_pct': diff
            })
    
    rebalancing_trades.sort(key=lambda x: x['symbol'])
    
    return {
        'total_combined_value': total_combined_value,
        'current_allocations': current_allocations,
        'target_allocations': target_allocations,
        'rebalancing_trades': rebalancing_trades,
        'analysis_summary': analysis_results
    }


def generate_trade_orders(**context):
    """Generate CSV file with rebalancing trade orders."""
    ti = context['ti']
    aggregation_result = ti.xcom_pull(task_ids='aggregate_portfolios')
    
    trades = aggregation_result['rebalancing_trades']
    
    if not trades:
        print("No rebalancing trades needed. Portfolio is within tolerance.")
        return
    
    df = pd.DataFrame(trades)
    df['timestamp'] = datetime.now().isoformat()
    
    column_order = ['timestamp', 'symbol', 'action', 'trade_value', 
                   'current_pct', 'target_pct', 'diff_pct']
    df = df[column_order]
    
    output_path = '/tmp/trade_orders.csv'
    df.to_csv(output_path, index=False)
    
    print(f"Generated trade orders CSV at {output_path}")
    print(f"Total trades: {len(trades)}")
    
    return output_path


with DAG(
    dag_id='financial_portfolio_rebalancing',
    default_args=default_args,
    description='Daily financial portfolio rebalancing across multiple brokerage accounts',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'portfolio', 'rebalancing'],
) as dag:
    
    # Fan-out: Fetch holdings from 5 brokerage accounts
    fetch_tasks = []
    for i in range(1, 6):
        task = PythonOperator(
            task_id=f'fetch_brokerage_{i:03d}',
            python_callable=fetch_holdings,
            op_kwargs={'brokerage_id': f'BROKERAGE_{i:03d}'},
        )
        fetch_tasks.append(task)
    
    # Fan-out: Analyze each portfolio independently
    analyze_tasks = []
    for i in range(1, 6):
        task = PythonOperator(
            task_id=f'analyze_portfolio_{i:03d}',
            python_callable=analyze_portfolio,
            op_kwargs={'brokerage_id': f'BROKERAGE_{i:03d}'},
        )
        analyze_tasks.append(task)
    
    # Fan-in: Aggregate all analysis results
    aggregate_task = PythonOperator(
        task_id='aggregate_portfolios',
        python_callable=aggregate_portfolios,
    )
    
    # Generate final trade orders CSV
    generate_task = PythonOperator(
        task_id='generate_trade_orders',
        python_callable=generate_trade_orders,
    )
    
    # Define task dependencies
    # Fetch -> Analyze (parallel for each account)
    for i in range(5):
        fetch_tasks[i] >> analyze_tasks[i]
    
    # Analyze -> Aggregate (all analyses feed into aggregation)
    analyze_tasks >> aggregate_task
    
    # Aggregate -> Generate
    aggregate_task >> generate_task