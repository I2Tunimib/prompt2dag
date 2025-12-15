<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements. The pipeline has several steps that need to be structured as tasks within a flow. The key points are parallel execution of fetching and analysis tasks, followed by aggregation and generating a CSV.

First, the user mentioned using @task and @flow decorators. Each step in the pipeline should be a task. The fetch and analysis steps are done in parallel for five brokerage accounts. In Prefect, to run tasks in parallel, I should use .submit() which returns futures. Then, the aggregation task depends on all analysis results, so I need to pass all the futures to it.

The steps are: fetch holdings (parallel) → analyze portfolio (parallel) → aggregate → generate CSV. So each fetch task is followed by an analysis task for the same account, but all five can run in parallel. Then the aggregation waits for all five analysis tasks to complete.

I'll start by defining the tasks. The fetch_holdings task will take an account ID and return a DataFrame. Since the data sources are simulated, I'll create mock data with symbols, quantities, and prices. Then, the analyze_portfolio task takes the holdings DataFrame and calculates total value, allocation percentages, and risk score. Maybe return a dictionary with these values.

The aggregate_results task will take the five analysis results, compute the combined totals, compare against target allocations, and determine the trades needed. The target allocations are given as AAPL 30%, GOOGL 25%, MSFT 20%, CASH 25%. The aggregation needs to check if the current allocation differs by more than 2% and generate trades accordingly.

Then, generate_trade_orders_csv will take the aggregated data and create a CSV file. The user mentioned a consolidated CSV, so this task will handle that.

For the flow, I'll structure it so that each fetch task is submitted in parallel, then each corresponding analysis task is also submitted. Since each analysis depends on a specific fetch task, I can chain them. Then, wait for all analysis tasks to finish before aggregating. Finally, generate the CSV.

Since the user mentioned using .submit() for parallel steps, I'll use the submit method on the tasks. However, in Prefect 2, when using the task runner, you can use .submit() to run tasks asynchronously. But I need to make sure that the tasks are properly awaited. Alternatively, using a DaskTaskRunner might be necessary for parallel execution, but the user didn't specify that. The code should assume that the tasks can be submitted normally with .submit().

Wait, in Prefect 2, to run tasks in parallel, you can use the task.submit() method which returns a future. Then, when you need the result, you call future.result(). So for each fetch task, I'll submit them, collect the futures, then for each analysis, submit with the corresponding fetch future. Then collect all analysis futures and pass them to aggregate.

So the flow would look like:

@flow
def portfolio_rebalancing_flow():
    # Fetch all holdings in parallel
    fetch_futures = [fetch_holdings.submit(f"BROKERAGE_{i:03}") for i in range(1,6)]
    # Analyze each portfolio in parallel
    analysis_futures = [analyze_portfolio.submit(fetch_future.result()) for fetch_future in fetch_futures]
    # Wait for all analyses to complete
    analysis_results = [future.result() for future in analysis_futures]
    # Aggregate
    aggregated_data = aggregate_results.submit(analysis_results).result()
    # Generate CSV
    generate_trade_orders_csv(aggregated_data)

Wait, but the aggregation task needs to process all analysis results. So perhaps the analysis_futures are passed to aggregate_results as a list. But when using .submit(), each analysis task returns a future, so when you call .result() on each, it blocks until that task is done. However, since all are submitted in parallel, once all are submitted, collecting their results would wait for all to complete.

Alternatively, maybe the flow should first submit all fetch tasks, then for each fetch future, submit an analysis task. Then collect all analysis futures and pass them to aggregate_results.

Wait, the correct approach is:

1. Submit all fetch tasks (parallel).
2. For each fetch task's future, submit an analysis task (also parallel, so 5 analysis tasks running in parallel).
3. Collect all analysis futures.
4. Once all analysis tasks are done, aggregate the results.
5. Generate CSV.

So in code:

@flow
def portfolio_rebalancing_flow():
    # Fetch all holdings in parallel
    fetch_results = []
    for account_id in ["BROKERAGE_001", "BROKERAGE_002", "BROKERAGE_003", "BROKERAGE_004", "BROKERAGE_005"]:
        fetch_future = fetch_holdings.submit(account_id)
        fetch_results.append(fetch_future)
    
    # Analyze each portfolio in parallel
    analysis_results = []
    for fetch_future in fetch_results:
        analysis_future = analyze_portfolio.submit(fetch_future.result())
        analysis_results.append(analysis_future)
    
    # Wait for all analysis results
    # Collect the results from the futures
    analysis_data = [ar.result() for ar in analysis_results]
    
    # Aggregate
    aggregated = aggregate_results(analysis_data)
    
    # Generate CSV
    generate_trade_orders_csv(aggregated)

Wait, but when using .submit(), each call returns a future. However, in Prefect, when you call .submit(), the task is submitted to the task runner for execution. The flow will wait for the futures when .result() is called. So in the code above, when we loop through fetch_results and call .result() on each, the flow will block until each fetch task is done. But since they were submitted in parallel, the first .result() will wait for the first fetch task, but the others may still be running. Hmm, maybe that's not the right approach. Instead, perhaps we should collect all fetch futures first, then wait for all of them to complete before proceeding to analysis.

Wait, no. Because when you submit all fetch tasks, they are all running in parallel. Then, when you iterate over each fetch_future and call .result(), each call will block until that particular future is done. But since they are submitted in parallel, the order in which they complete may vary. However, the loop will process each future in the order they were submitted. So for example, if the first fetch task takes longer, the loop will wait on the first .result(), then proceed to the next. This would not be parallel. Wait, no—submitting the tasks starts them all, but when you call .result() on each future in sequence, you're effectively waiting for each task in order, which would serialize the waiting. But the tasks are already running in parallel. So perhaps this approach is okay.

Alternatively, maybe using as_completed or some other method, but Prefect's futures may not support that. Alternatively, collect all fetch futures, then for each, submit the analysis task as soon as the fetch is done. But that might complicate things.

Alternatively, perhaps the correct way is to submit all fetch tasks, then collect their results in a list (which will wait for all to complete), then submit all analysis tasks. But that would not allow the analysis tasks to start as soon as a fetch task completes. However, the user's pipeline description says that the fetch and analysis steps are in parallel. Wait, the description says "Five parallel portfolio analysis tasks process the fetched data". So each analysis task depends on its corresponding fetch task, but all five analysis tasks can run in parallel once their respective fetch tasks are done.

So the correct approach is:

- Submit all 5 fetch tasks in parallel.
- For each fetch task's future, submit an analysis task as soon as the fetch task completes. But since all fetch tasks are submitted at once, their analysis tasks can be submitted as their results become available. However, in the code above, when you loop through the fetch_results and call .result() on each, you are waiting for each fetch task in the order they were submitted, which may not be optimal. But perhaps Prefect's task runner handles this efficiently.

Alternatively, maybe using a task runner like DaskTaskRunner with parallelism set to 5. But the user didn't mention that, so perhaps the code should not include that. The code should just use the default task runner, which allows for some parallelism.

In any case, the code structure would be as follows:

Each fetch_holdings task is submitted, then each analyze_portfolio task is submitted with the result of the corresponding fetch_holdings. Then, all analysis results are collected and passed to aggregate_results.

Now, implementing the tasks:

The fetch_holdings task takes an account_id and returns a DataFrame. Since the data is simulated, I'll create a mock DataFrame with some holdings. For example, generate a few rows with symbols, quantities, and prices.

The analyze_portfolio task takes the holdings DataFrame and computes total portfolio value, allocation percentages, and risk score. The risk score is based on holdings count. Maybe if holdings count is more than X, risk is higher. But the exact logic isn't specified, so I'll make a simple calculation.

The aggregate_results task takes a list of analysis results (each being a dictionary with total_value, allocation_percentages, risk_score). It sums the total_values to get the combined portfolio value. Then, for each symbol, sum the allocation percentages weighted by their portfolio's total value. Then compare against target allocations. If the difference exceeds 2%, generate a trade. The target allocations are given as AAPL:30%, GOOGL:25%, MSFT:20%, CASH:25%. So the aggregation needs to compute the current combined allocation and determine the required trades.

The generate_trade_orders_csv task takes the aggregated data (which includes the necessary trades) and writes a CSV. The CSV should have columns like symbol, action (BUY/SELL), amount, allocation_percent.

Now, let's code each task with these steps.

For the fetch_holdings task, since it's a simulation, I'll create a mock DataFrame. For example, each account might have some holdings of AAPL, GOOGL, MSFT, and CASH. The quantities and prices can be randomized, but for simplicity, perhaps fixed values. But since the user said to avoid placeholders, I'll use minimal defaults.

In the analyze_portfolio task, total_value is sum(quantity * price). Allocation percentages are (quantity * price) / total_value for each holding. Risk score could be 1 if holdings count <=3, else 0.8, but the exact logic isn't specified, so I'll compute it as 1 / (1 + holdings_count), but perhaps the user mentioned "risk scores based on holdings count", so maybe holdings_count is the number of different symbols. For example, if there are 4 holdings, risk score is 4. But the description says "Generate risk scores based on holdings count", so perhaps the risk score is the count. But the example isn't clear. So I'll compute the count of unique symbols and use that as the risk score, but maybe normalized. Alternatively, perhaps the risk score is the number of holdings, but the exact calculation isn't critical here; just need to return a value.

The aggregation task needs to compute the combined allocation percentages. For each symbol across all accounts, sum (allocation_percentage * total_value) for that account, then divide by the combined total value. Then compare to target allocations. For example, if AAPL's combined allocation is 28%, target is 30%, difference is 2%, which is the threshold. So no action needed. But if it's 27%, difference is 3%, which exceeds 2%, so generate a trade.

The target allocations are given, so in the code, they can be a dictionary. The aggregation task will calculate current allocations, compare to targets, and generate trades where the difference exceeds 2%.

The generate_trade_orders_csv task will take the trades data and write it to a CSV. The trades could be a list of dictionaries with symbol, action, amount, etc.

Putting it all together, the code should have:

- Imports: prefect, pandas, etc.
- Tasks decorated with @task.
- Flow function with the orchestration logic.
- Mock data generation in fetch_holdings.
- The main guard.

Now, considering possible edge cases, like symbols not in the target allocations. For example, if a holding is in the data but not in the target, perhaps it's treated as needing to be sold. But the user's target allocations include AAPL, GOOGL, MSFT, and CASH. So any other symbols would need to be sold to meet the target. However, the aggregation logic should handle that.

In the aggregation task, the steps are:

1. Sum all total_values to get combined_total.
2. For each symbol in all analysis results, compute current_allocation = sum( (allocation_percentages.get(symbol, 0) * account_total) for each account ) / combined_total.
3. Compare current_allocation to target_allocation for each symbol in the target.
4. For symbols not in target, current_allocation should be reduced to 0% (sell all).
5. If the difference exceeds 2%, calculate the required adjustment.

But this might be complex. For simplicity, perhaps the aggregation task only considers the symbols in the target allocations. Other symbols are ignored, but the problem statement doesn't mention that. Alternatively, the code should handle all symbols present in the data.

But given the problem statement's target allocations include specific symbols, perhaps the aggregation only considers those four. So any other symbols in the holdings would be considered as needing to be sold to reach 0% allocation.

This requires that in the aggregation, for each symbol in the target, compute current allocation, and for other symbols, their current allocation is added to the 'other' category, but the problem statement doesn't mention that. Since the user's example includes the four symbols, perhaps the code should focus on those.

In any case, the code should implement the logic as per the problem statement.

Putting all this together, here's the code:
</think>

import pandas as pd
from prefect import flow, task

TARGET_ALLOCATIONS = {
    "AAPL": 0.30,
    "GOOGL": 0.25,
    "MSFT": 0.20,
    "CASH": 0.25
}
ALLOCATION_THRESHOLD = 0.02

@task
def fetch_holdings(account_id: str) -> pd.DataFrame:
    """Simulate fetching holdings data from brokerage account"""
    mock_data = {
        "BROKERAGE_001": [
            {"symbol": "AAPL", "quantity": 100, "price": 150},
            {"symbol": "GOOGL", "quantity": 50, "price": 200},
            {"symbol": "CASH", "quantity": 10000, "price": 1}
        ],
        "BROKERAGE_002": [
            {"symbol": "MSFT", "quantity": 75, "price": 250},
            {"symbol": "CASH", "quantity": 5000, "price": 1}
        ],
        "BROKERAGE_003": [
            {"symbol": "AAPL", "quantity": 200, "price": 150},
            {"symbol": "CASH", "quantity": 15000, "price": 1}
        ],
        "BROKERAGE_004": [
            {"symbol": "GOOGL", "quantity": 100, "price": 200},
            {"symbol": "MSFT", "quantity": 50, "price": 250},
            {"symbol": "CASH", "quantity": 20000, "price": 1}
        ],
        "BROKERAGE_005": [
            {"symbol": "CASH", "quantity": 30000, "price": 1}
        ]
    }
    return pd.DataFrame(mock_data[account_id])

@task
def analyze_portfolio(holdings: pd.DataFrame) -> dict:
    """Analyze individual portfolio holdings"""
    holdings["value"] = holdings["quantity"] * holdings["price"]
    total_value = holdings["value"].sum()
    
    allocation = holdings.groupby("symbol")["value"].sum() / total_value
    allocation_percent = {symbol: percent for symbol, percent in allocation.items()}
    
    risk_score = 1 / len(holdings) if len(holdings) > 0 else 0
    
    return {
        "total_value": total_value,
        "allocation_percent": allocation_percent,
        "risk_score": risk_score
    }

@task
def aggregate_results(analysis_outputs: list[dict]) -> dict:
    """Aggregate analysis results and calculate rebalancing needs"""
    combined_value = sum(account["total_value"] for account in analysis_outputs)
    
    current_allocations = {}
    for account in analysis_outputs:
        for symbol, percent in account["allocation_percent"].items():
            current_allocations[symbol] = current_allocations.get(symbol, 0) + (
                percent * account["total_value"] / combined_value
            )
    
    trades = []
    for symbol, target in TARGET_ALLOCATIONS.items():
        current = current_allocations.get(symbol, 0)
        difference = current - target
        
        if abs(difference) > ALLOCATION_THRESHOLD:
            action = "SELL" if difference > 0 else "BUY"
            trades.append({
                "symbol": symbol,
                "action": action,
                "target_allocation": target,
                "current_allocation": current,
                "required_change": abs(difference) * combined_value
            })
    
    return {
        "combined_value": combined_value,
        "current_allocations": current_allocations,
        "trades": trades
    }

@task
def generate_trade_orders_csv(aggregated_data: dict) -> None:
    """Generate consolidated trade orders CSV file"""
    trades_df = pd.DataFrame(aggregated_data["trades"])
    trades_df.to_csv("trade_orders.csv", index=False)
    print(f"Generated trade orders with {len(trades_df)} actions")

@flow
def portfolio_rebalancing_flow():
    # Daily execution schedule would be configured via deployment
    fetch_futures = [fetch_holdings.submit(f"BROKERAGE_00{i}") for i in range(1, 6)]
    analysis_futures = [analyze_portfolio.submit(future.result()) for future in fetch_futures]
    aggregated_data = aggregate_results.submit([future.result() for future in analysis_futures]).result()
    generate_trade_orders_csv(aggregated_data)

if __name__ == "__main__":
    portfolio_rebalancing_flow()