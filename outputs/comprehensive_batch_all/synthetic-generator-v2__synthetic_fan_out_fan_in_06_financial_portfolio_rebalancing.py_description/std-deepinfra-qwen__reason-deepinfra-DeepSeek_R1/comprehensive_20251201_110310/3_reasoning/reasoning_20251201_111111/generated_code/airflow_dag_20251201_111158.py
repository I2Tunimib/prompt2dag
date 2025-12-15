<think>
Okay, I need to create an Apache Airflow DAG for this financial portfolio rebalancing pipeline. Let me start by understanding the requirements from the pipeline description.

First, the pipeline has a fan-out fan-in pattern. There are five parallel tasks to fetch data from different brokerage accounts, then five parallel analysis tasks, followed by aggregation and generating a CSV. So dependencies are fetch → analyze → aggregate → generate.

The user specified using Airflow 2.x style imports, so I'll make sure to import DAG and PythonOperator correctly. Since they mentioned using the TaskFlow API, maybe using the @task decorator would be appropriate here for simplicity and cleaner code.

The DAG should have a daily schedule, catchup probably set to False, and default_args with some sensible parameters like owner, start_date, maybe retries. The tasks need explicit dependencies using >> operators.

Each fetch task is for a different brokerage (001 to 005). Since they are parallel, I can create a loop or use a function that generates these tasks dynamically. Similarly, the analysis tasks are also parallel per account.

Wait, the steps mention that each fetch task retrieves a CSV, then each analysis task processes the fetched data. So each analysis task depends on its corresponding fetch task. But since all fetch tasks are parallel, and all analysis tasks are parallel too, but each analysis task depends on one fetch task. Then after all analysis tasks complete, the aggregation runs, followed by generate.

So the structure would be:

fetch_001 → analyze_001

fetch_002 → analyze_002

... and so on for all five. Then all analyze tasks must complete before aggregation, which then triggers generate.

So in Airflow, after creating all fetch and analyze tasks, I can set the dependencies such that each analyze task depends on its fetch task. Then, the aggregation task depends on all analyze tasks, and generate depends on aggregation.

Using the TaskFlow API with @task decorator can simplify this. For example, creating a function for fetch that takes brokerage ID, and another for analysis. Then, in the DAG, loop over the broker IDs to create the tasks.

But wait, the user said to use PythonOperator. Wait, the pipeline description says "Uses Airflow 2.x TaskFlow API with PythonOperator". Hmm, maybe they meant using the TaskFlow API which under the hood uses PythonOperator, but perhaps the user wants to use the decorators. Let me check.

The TaskFlow API allows using @task decorators for Python functions, which are converted into PythonOperator instances. So using @task would be appropriate here, making the code cleaner.

So, I can define a function for fetching data, decorated with @task, and another for analysis. Since there are five of each, perhaps using a loop to generate these tasks.

Wait, but each fetch and analyze task is per brokerage, so maybe I can create a list of broker IDs and for each, call the fetch and analyze functions with the ID as a parameter. Then, collect all the fetch tasks and analysis tasks, and set up the dependencies.

Alternatively, using dynamic task mapping. But since the number is fixed at five, maybe a simple loop would suffice.

Let me outline the steps:

1. Define default_args with necessary parameters like owner, start_date, retries, etc.

2. Instantiate the DAG with daily schedule_interval, catchup=False.

3. Create fetch tasks for each brokerage. Using a decorator @task, maybe a function that takes brokerage_id as argument.

Wait, but with the @task decorator, can I pass parameters when calling the function? For example:

@task
def fetch_holdings(brokerage_id):
    # code here

Then, in the DAG, loop over the broker IDs and call fetch_holdings for each. Each call would create a task instance. So:

broker_ids = ['001', '002', '003', '004', '005']

fetch_tasks = []
for bid in broker_ids:
    fetch_task = fetch_holdings(bid)
    fetch_tasks.append(fetch_task)

Similarly for analysis tasks. Then, each analysis task depends on the corresponding fetch task. Wait, but how to link them? Because each analysis task is also generated in a loop.

Alternatively, perhaps the analysis function is also a @task, and for each broker_id, we first fetch, then analyze, then collect all analyze tasks.

Wait, maybe:

for broker_id in broker_ids:
    fetch_task = fetch_holdings.override(task_id=f'fetch_{broker_id}')(broker_id)
    analyze_task = analyze_portfolio.override(task_id=f'analyze_{broker_id}')(broker_id)
    fetch_task >> analyze_task
    analyze_tasks.append(analyze_task)

Then, aggregation_task = aggregate_analysis(analyze_tasks_outputs) but wait, how to pass the outputs? Since each analyze task produces some result, the aggregation task would need to collect all of them.

But in Airflow, when using TaskFlow, the outputs of tasks are XComs. So the aggregation function would take the list of results from all analyze tasks.

Wait, but when using @task, functions can return values which are automatically pushed to XCom. So the analyze function would return some data, and the aggregation function would take all those as inputs.

But how to structure that. For example:

@task
def analyze_portfolio(brokerage_id, data):
    # process data
    return analysis_result

But the fetch function would return the data, which is then passed to analyze.

Wait, perhaps:

fetch_outputs = []
analyze_outputs = []
for broker_id in broker_ids:
    fetched = fetch_holdings(broker_id)
    analyzed = analyze_portfolio(fetched)
    fetch_outputs.append(fetched)
    analyze_outputs.append(analyzed)

But in this case, each analyze task depends on the corresponding fetch task. Then, the aggregation task would depend on all analyze tasks and take their outputs as input.

So aggregation_task = aggregate_analysis(analyze_outputs)

Then generate_task = generate_trades(aggregation_task)

So the dependencies would be automatically set because the functions are called in order.

Wait, but in Airflow TaskFlow, the dependencies are determined by the order in which tasks are called and how their outputs are used. So if the aggregation task is called with the list of analyze_outputs, Airflow knows that it depends on all those tasks.

Yes, that's correct. So using the TaskFlow API, the code can be structured with loops to generate the tasks, and the dependencies are handled implicitly through the function invocations.

Now, the functions:

The fetch_holdings function would simulate fetching data from a brokerage. It could return a mock DataFrame or a dictionary representing the holdings.

The analyze_portfolio function would take that data, compute total value, allocation percentages, risk scores. Then return those results.

The aggregate_analysis function would take all the analysis results, compute combined totals, compare to target allocations, determine trades needed.

The generate_trades function would take the aggregated data and create a CSV.

Now, implementing these functions with mock data.

But the user said to avoid placeholders, so the functions should have minimal but functional code. For example, in fetch_holdings, generate a mock CSV data. Maybe using a dictionary with some sample symbols and quantities.

In the aggregation task, the target allocations are given: AAPL 30%, GOOGL 25%, MSFT 20%, CASH 25%. The code should compare current allocations against these and generate trades if the difference exceeds 2%.

Now, putting it all together.

Also, the DAG should have a DAG context manager, proper imports, and PEP8 compliance.

Imports needed: from airflow import DAG, from airflow.decorators import task, datetime for start_date, maybe others.

Wait, the user specified to use Airflow 2.x style imports. So for PythonOperator, it's from airflow.operators.python import PythonOperator. But if using the TaskFlow API, we can import the task decorator from airflow.decorators.

So the imports would be:

from datetime import datetime
from airflow import DAG
from airflow.decorators import task

Then, default_args could include things like retries, email settings, etc., but the user didn't specify, so I can set some defaults.

The DAG definition:

with DAG(
    dag_id='portfolio_rebalancing',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance'],
) as dag:

Then, the task functions.

But wait, the user said "No sensors or branching logic present", so no need for sensors or BranchPythonOperator.

Now, code outline:

import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='portfolio_rebalancing',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    @task
    def fetch_holdings(brokerage_id):
        # Mock fetching CSV data
        holdings = [
            {'symbol': 'AAPL', 'quantity': 100, 'price': 150},
            {'symbol': 'GOOGL', 'quantity': 50, 'price': 2500},
            {'symbol': 'MSFT', 'quantity': 75, 'price': 300},
            {'symbol': 'CASH', 'quantity': 10000, 'price': 1},
        ]
        return {
            'brokerage_id': brokerage_id,
            'holdings': holdings
        }

    @task
    def analyze_portfolio(data):
        holdings = data['holdings']
        total_value = sum(h['quantity'] * h['price'] for h in holdings)
        allocation = {}
        for h in holdings:
            symbol = h['symbol']
            value = h['quantity'] * h['price']
            allocation[symbol] = (value / total_value) * 100
        risk_score = len(holdings)  # Simplified risk score
        return {
            'brokerage_id': data['brokerage_id'],
            'total_value': total_value,
            'allocation': allocation,
            'risk_score': risk_score
        }

    @task
    def aggregate_analysis(analysis_results):
        combined_value = sum(result['total_value'] for result in analysis_results)
        combined_allocation = {}
        for result in analysis_results:
            for symbol, percent in result['allocation'].items():
                if symbol in combined_allocation:
                    combined_allocation[symbol] += (percent * result['total_value']) / combined_value
                else:
                    combined_allocation[symbol] = (percent * result['total_value']) / combined_value

        target_allocations = {
            'AAPL': 30,
            'GOOGL': 25,
            'MSFT': 20,
            'CASH': 25
        }

        trades = []
        for symbol, current in combined_allocation.items():
            target = target_allocations.get(symbol, 0)
            if abs(current - target) > 2:
                action = 'BUY' if current < target else 'SELL'
                amount = abs(current - target) * combined_value / 100
                trades.append({
                    'symbol': symbol,
                    'action': action,
                    'amount': amount,
                    'current_allocation': current,
                    'target_allocation': target
                })

        return {
            'combined_value': combined_value,
            'trades': trades
        }

    @task
    def generate_trades(data):
        df = pd.DataFrame(data['trades'])
        df.to_csv('/tmp/trade_orders.csv', index=False)
        return '/tmp/trade_orders.csv'

    # Task flow
    broker_ids = ['001', '002', '003', '004', '005']
    fetch_tasks = [fetch_holdings(bid) for bid in broker_ids]
    analyze_tasks = [analyze_portfolio.override(task_id=f'analyze_{bid}')(data) for bid, data in zip(broker_ids, fetch_tasks)]
    aggregation_task = aggregate_analysis(analyze_tasks)
    generate_task = generate_trades(aggregation_task)

    # Set dependencies
    # Since using TaskFlow, dependencies are inferred, but explicit >> may be needed if not using XComArgs
    # Alternatively, chain using >> between lists
    # However, in TaskFlow, the order is determined by the function calls, but to ensure, perhaps:

    # fetch_tasks >> analyze_tasks (but each analyze depends on its fetch)
    # But in the list comprehensions above, each analyze task is created by passing the corresponding fetch task's output.
    # So Airflow automatically sets the dependency between each fetch and analyze task.

    # Then, aggregation_task depends on all analyze_tasks, which is handled by passing analyze_tasks as a list to aggregate_analysis.

    # Finally, generate_task depends on aggregation_task.

    # So the code as written should set up the dependencies correctly.

Wait, but when creating analyze_tasks as a list comprehension where each element is analyze_portfolio(data) where data is the output of fetch_holdings(bid), Airflow will automatically set the dependency between each fetch and analyze task.

Then, aggregation_task takes the list of analyze_tasks as input, so it depends on all of them.

Then generate_task depends on aggregation_task.

So the code should correctly set up the dependencies.

But in the code above, when we do:

analyze_tasks = [analyze_portfolio.override(task_id=f'analyze_{bid}')(data) for bid, data in zip(broker_ids, fetch_tasks)]

Each data is the output of fetch_holdings(bid), which is an XComArg. So each analyze_portfolio task depends on the corresponding fetch_holdings task.

Then aggregation_task = aggregate_analysis(analyze_tasks) will depend on all elements in analyze_tasks.

Then generate_task = generate_trades(aggregation_task) depends on aggregation_task.

So the dependencies are correctly set.

But wait, in the code as written, the task_id for analyze_portfolio would be the same for each unless overridden. Because the @task decorator generates a task_id based on the function name. So if we call analyze_portfolio multiple times, the task_id would be the same, causing duplicate task IDs. Hence, we need to override the task_id for each analyze_portfolio task.

Hence, in the code, when creating analyze_tasks, we use analyze_portfolio.override(task_id=f'analyze_{bid}') for each broker_id. That way, each task has a unique ID.

Similarly for fetch_holdings, but since each is called with a different broker_id in a separate task, the task_id would be automatically generated as 'fetch_holdings_<broker_id>', but perhaps better to override the task_id to have consistent names like 'fetch_001', etc.

Wait, the @task decorator allows passing a task_id_prefix, but when you call the function multiple times, Airflow appends a number to the task_id. To have explicit task IDs, we need to override them.

So for fetch_holdings, when called with different broker_ids, each task should have a unique task_id. So:

Instead of:

fetch_tasks = [fetch_holdings(bid) for bid in broker_ids]

We should override the task_id for each fetch_holdings task:

fetch_tasks = [fetch_holdings.override(task_id=f'fetch_{bid}')(bid) for bid in broker_ids]

Similarly for analyze_portfolio.

So adjusting the code:

broker_ids = ['001', '002', '003', '004', '005']
fetch_tasks = [fetch_holdings.override(task_id=f'fetch_{bid}')(bid) for bid in broker_ids]
analyze_tasks = [analyze_portfolio.override(task_id=f'analyze_{bid}')(data) for bid, data in zip(broker_ids, fetch_tasks)]

This way, each fetch and analyze task has a unique task_id.

Now, putting all together.

Also, the generate_trades function uses pandas to create a CSV. So need to import pandas as pd.

But the user didn't mention installing packages, but assuming pandas is available, the code should run.

Now, checking for PEP8 compliance: line lengths, proper indentation, etc.

The functions have docstrings? The user said to include minimal docstrings where helpful. So maybe adding a one-liner.

For example:

@task
def fetch_holdings(brokerage_id):
    """Fetch mock holdings data for a brokerage account."""
    # code

Similarly for others.

Now, testing if the code is executable. The functions return mock data, which is acceptable.

The target_allocations in aggregate_analysis are hardcoded, which matches the pipeline description.

The aggregation calculates combined allocation percentages correctly by weighting each account's allocation by its total value.

The trade generation checks for differences exceeding 2% and creates BUY/SELL actions accordingly.

The generate_trades task uses pandas to write a CSV, which is then returned (path).

Thus, the code should meet all requirements.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'financial_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='portfolio_rebalancing',
    default_args=default_args,
    description='Daily portfolio rebalancing pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'rebalancing'],
) as dag:

    @task(task_id='fetch_holdings')
    def fetch_holdings(brokerage_id: str):
        """Mock fetching holdings data from brokerage API"""
        return {
            'brokerage_id': brokerage_id,
            'holdings': [
                {'symbol': 'AAPL', 'quantity': 150, 'price': 185.25},
                {'symbol': 'GOOGL', 'quantity': 45, 'price': 2750.30},
                {'symbol': 'MSFT', 'quantity': 80, 'price': 305.75},
                {'symbol': 'CASH', 'quantity': 50000, 'price': 1.0}
            ]
        }

    @task(task_id='analyze_portfolio')
    def analyze_portfolio(data: dict):
        """Perform portfolio analysis for individual account"""
        total_value = sum(h['quantity'] * h['price'] for h in data['holdings'])
        allocation = {h['symbol']: (h['quantity'] * h['price']) / total_value * 100 
                      for h in data['holdings']}
        return {
            'brokerage_id': data['brokerage_id'],
            'total_value': total_value,
            'allocation': allocation,
            'risk_score': len(data['holdings'])
        }

    @task(task_id='aggregate_analysis')
    def aggregate_analysis(results: list):
        """Aggregate results and calculate rebalancing needs"""
        total_value = sum(r['total_value'] for r in results)
        target_allocations = {'AAPL': 30, 'GOOGL': 25, 'MSFT': 20, 'CASH': 25}
        
        current_allocation = {}
        for result in results:
            for symbol, percent in result['allocation'].items():
                weighted_percent = percent * (result['total_value'] / total_value)
                current_allocation[symbol] = current_allocation.get(symbol, 0.0) + weighted_percent

        trades = []
        for symbol, target in target_allocations.items():
            current = current_allocation.get(symbol, 0.0)
            if abs(current - target) > 2.0:
                trades.append({
                    'symbol': symbol,
                    'action': 'BUY' if current < target else 'SELL',
                    'amount': abs(current - target) * total_value / 100,
                    'current_alloc': round(current, 2),
                    'target_alloc': target
                })
        
        return {'total_value': total_value, 'trades': trades}

    @task(task_id='generate_trade_orders')
    def generate_trade_orders(data: dict):
        """Generate consolidated trade orders CSV"""
        import pandas as pd
        df = pd.DataFrame(data['trades'])
        df.to_csv('/tmp/rebalancing_orders.csv', index=False)
        return '/tmp/rebalancing_orders.csv'

    # Task orchestration
    broker_ids = [f'BROKERAGE_{i:03d}' for i in range(1, 6)]
    fetch_tasks = [fetch_holdings.override(task_id=f'fetch_{bid}')(bid) for bid in broker_ids]
    analysis_tasks = [analyze_portfolio.override(task_id=f'analyze_{bid}')(data) 
                      for bid, data in zip(broker_ids, fetch_tasks)]
    aggregation = aggregate_analysis(analysis_tasks)
    generate_report = generate_trade_orders(aggregation)

    # Explicit dependency declaration
    fetch_tasks >> analysis_tasks >> aggregation >> generate_report