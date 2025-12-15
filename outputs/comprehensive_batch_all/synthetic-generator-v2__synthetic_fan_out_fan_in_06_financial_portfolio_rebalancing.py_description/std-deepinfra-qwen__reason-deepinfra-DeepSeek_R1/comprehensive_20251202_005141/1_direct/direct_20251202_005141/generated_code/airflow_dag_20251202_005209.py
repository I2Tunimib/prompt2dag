from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 300,
}

# Define the DAG
with DAG(
    dag_id='financial_portfolio_rebalancing',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def fetch_holdings_data(brokerage_id):
        """Simulate fetching holdings data from a brokerage account."""
        # Simulate API call to fetch data
        data = {
            'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
            'quantity': [100, 50, 75, 25],
            'price': [150.0, 2500.0, 300.0, 1.0]
        }
        return pd.DataFrame(data)

    def analyze_portfolio(df):
        """Analyze the portfolio data."""
        total_value = (df['quantity'] * df['price']).sum()
        df['allocation_percentage'] = (df['quantity'] * df['price']) / total_value
        df['risk_score'] = len(df)  # Simplified risk score based on holdings count
        return df

    def aggregate_results(dfs):
        """Aggregate results from all portfolios."""
        combined_df = pd.concat(dfs)
        total_combined_value = (combined_df['quantity'] * combined_df['price']).sum()
        combined_df['combined_allocation_percentage'] = (combined_df['quantity'] * combined_df['price']) / total_combined_value

        target_allocations = {'AAPL': 0.30, 'GOOGL': 0.25, 'MSFT': 0.20, 'CASH': 0.25}
        rebalancing_trades = []

        for symbol, target_allocation in target_allocations.items():
            current_allocation = combined_df[combined_df['symbol'] == symbol]['combined_allocation_percentage'].sum()
            if abs(current_allocation - target_allocation) > 0.02:
                action = 'BUY' if current_allocation < target_allocation else 'SELL'
                amount = abs(target_allocation - current_allocation) * total_combined_value
                rebalancing_trades.append({
                    'symbol': symbol,
                    'action': action,
                    'amount': amount,
                    'allocation_percentage': target_allocation
                })

        return pd.DataFrame(rebalancing_trades)

    def generate_trade_orders_csv(df, output_path):
        """Generate a CSV file with the rebalancing trade orders."""
        df.to_csv(output_path, index=False)

    # Fetch holdings data from five brokerage accounts
    fetch_tasks = []
    for i in range(1, 6):
        fetch_task = PythonOperator(
            task_id=f'fetch_holdings_data_{i}',
            python_callable=fetch_holdings_data,
            op_kwargs={'brokerage_id': f'BROKERAGE_{i:03d}'},
        )
        fetch_tasks.append(fetch_task)

    # Analyze each portfolio
    analyze_tasks = []
    for i, fetch_task in enumerate(fetch_tasks):
        analyze_task = PythonOperator(
            task_id=f'analyze_portfolio_{i+1}',
            python_callable=analyze_portfolio,
            op_kwargs={'df': fetch_task.output},
        )
        fetch_task >> analyze_task
        analyze_tasks.append(analyze_task)

    # Aggregate results from all portfolios
    aggregate_task = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
        op_kwargs={'dfs': [task.output for task in analyze_tasks]},
    )
    analyze_tasks >> aggregate_task

    # Generate trade orders CSV
    generate_trade_orders_task = PythonOperator(
        task_id='generate_trade_orders_csv',
        python_callable=generate_trade_orders_csv,
        op_kwargs={'df': aggregate_task.output, 'output_path': '/path/to/trade_orders.csv'},
    )
    aggregate_task >> generate_trade_orders_task