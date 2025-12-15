<think>
Okay, I need to convert the given financial portfolio rebalancing pipeline description into a Dagster job. Let's start by understanding the pipeline steps and how they map to Dagster's concepts.

First, the pipeline has four main stages: fetch holdings, analyze each portfolio, aggregate results, and generate trade orders. The fetch and analyze steps are parallelized across five brokerage accounts. So, in Dagster, each of these will be ops, and since they're parallel, their dependencies will be set up so that they can run concurrently.

The user mentioned using @op and @job decorators. So I'll create ops for each step. The fetch step is five separate ops, but since they're similar, maybe I can write one op function that takes a brokerage ID as a config parameter. Wait, but in the problem statement, the fetch tasks are BROKERAGE_001 to BROKERAGE_005. So perhaps each is a separate op. But that would mean writing five similar functions, which isn't DRY. Alternatively, use a single op that's called multiple times with different config. Hmm, but in Dagster, each op instance in the job is unique. So maybe using a configurable op that's reused for each brokerage.

Wait, but the pipeline description says "five separate accounts in parallel", so perhaps each fetch is a separate op. But creating five separate ops would be repetitive. Instead, maybe create a single 'fetch_holdings' op that takes a brokerage ID as a config parameter, and then in the job, we have five instances of this op. But Dagster doesn't allow dynamically creating multiple instances of the same op in a job. So perhaps the correct approach is to have one op that processes all five brokerages in parallel, but that might not fit. Alternatively, use a fan-out approach where one op starts and then maps over the brokerages.

Wait, no. The user's pipeline has five parallel fetch tasks. So each fetch is independent. In Dagster, to represent this, perhaps we can have a single op that takes a list of brokerage IDs and processes them in parallel. But that's not how Dagster works. Each op is a single unit of computation. So if we need five parallel tasks, each fetching data from a different brokerage, we need to have five separate ops or use dynamic outputs.

Ah, right! Dynamic outputs allow an op to yield multiple dynamic outputs, which can then be processed downstream. But in this case, the fetch tasks are all independent and need to run in parallel. So maybe using a dynamic approach where one op generates multiple outputs (one per brokerage), and then each is processed by the analysis ops.

Wait, but the description says the fetch tasks are five separate tasks running in parallel. So perhaps the initial approach is to have a single op that handles all five fetches, but that wouldn't be parallel. Alternatively, use a graph with a dynamic fan-out.

Alternatively, since the problem states that the maximum parallel width is 5, perhaps each fetch is a separate op, but how to create them without repeating code? Maybe create a function that generates the ops dynamically, but in Dagster, ops are defined statically. So perhaps the correct approach is to define one op for fetching, which is configured with the brokerage ID, and then in the job, we call this op five times with different configurations. But how to do that in Dagster?

Wait, in Dagster, you can't dynamically create multiple instances of the same op in a job definition. Each op in the job is a specific node. So for five brokerages, you would need five separate ops, unless you use dynamic outputs. Let me think again.

The user's pipeline has five fetch tasks (BROKERAGE_001 to BROKERAGE_005) running in parallel. Each of these is a separate task. So in Dagster, each would be a separate op. But writing five ops would be repetitive. So perhaps create a helper function to generate these ops.

Alternatively, use a single op that takes a list of brokerages and processes them all, but that would not be parallel. So that's not ideal. The correct approach here is to use dynamic outputs. Let me recall: in Dagster, an op can yield multiple DynamicOutputs, which can then be processed in parallel by downstream ops. So the fetch op could yield five dynamic outputs, each representing a brokerage's data. Then, each dynamic output is processed by the analyze op in parallel.

Yes, that makes sense. So the fetch_holdings op would generate five dynamic outputs, each with a brokerage ID. Then, the analyze_portfolio op would be mapped over these dynamic outputs, processing each in parallel. Then, the aggregation op would collect all the results from the analyze steps.

Wait, but the pipeline description says that after fetching, each portfolio is analyzed independently. So each fetch task's output is analyzed by a corresponding analysis task. So the structure is:

fetch_brokerage_001 -> analyze_brokerage_001

fetch_brokerage_002 -> analyze_brokerage_002

and so on, all in parallel. Then, all analyze tasks' outputs go into the aggregation task.

So using dynamic outputs would allow us to map the analyze op over each of the dynamic outputs from the fetch op.

So here's the plan:

1. Create a fetch_holdings op that yields DynamicOutput for each brokerage. This op would loop through the five brokerages and yield each as a dynamic output with a mapping key.

2. Then, the analyze_portfolio op is mapped over each dynamic output from fetch_holdings.

3. The aggregation op then depends on all the outputs from the analyze_portfolio ops.

4. Finally, generate_trade_orders depends on the aggregation.

This way, the fetch is a single op that dynamically generates five outputs, each processed by an analyze op in parallel. Then aggregation collects all analyze results.

But how to implement this in Dagster?

Let's outline the code.

First, the fetch_holdings op:

@op
def fetch_holdings(context):
    brokerages = ['BROKERAGE_001', 'BROKERAGE_002', 'BROKERAGE_003', 'BROKERAGE_004', 'BROKERAGE_005']
    for brokerage_id in brokerages:
        # Simulate fetching data
        holdings_data = [
            {'symbol': 'AAPL', 'quantity': 100, 'price': 150},
            {'symbol': 'GOOGL', 'quantity': 50, 'price': 2500},
        ]
        yield DynamicOutput(holdings_data, mapping_key=brokerage_id)

Here, the fetch_holdings op yields five dynamic outputs, each with a different mapping key (brokerage ID). Then, the analyze_portfolio op is mapped over each dynamic output.

Then, the analyze_portfolio op:

@op
def analyze_portfolio(context, holdings):
    total_value = sum(h['quantity'] * h['price'] for h in holdings)
    allocation = {h['symbol']: (h['quantity'] * h['price']) / total_value for h in holdings}
    risk_score = len(holdings)  # Simplified risk score
    return {
        'total_value': total_value,
        'allocation': allocation,
        'risk_score': risk_score
    }

This op takes the holdings data and returns the analysis results.

Then, the aggregation op needs to collect all the results from the analyze_portfolio ops. So it would take a list of the analyze results.

@op
def aggregate_analyses(context, analyses):
    # analyses is a list of the outputs from analyze_portfolio
    total_combined = sum(analysis['total_value'] for analysis in analyses)
    current_allocations = {}
    for analysis in analyses:
        for symbol, alloc in analysis['allocation'].items():
            current_allocations[symbol] = current_allocations.get(symbol, 0) + (alloc * analysis['total_value'] / total_combined)
    # Compare with target allocations
    target_allocations = {'AAPL': 0.3, 'GOOGL': 0.25, 'MSFT': 0.2, 'CASH': 0.25}
    trades = []
    for symbol, current in current_allocations.items():
        target = target_allocations.get(symbol, 0)
        difference = current - target
        if abs(difference) > 0.02:
            action = 'SELL' if difference > 0 else 'BUY'
            trades.append({
                'symbol': symbol,
                'action': action,
                'amount': abs(difference) * total_combined,
                'current_alloc': current,
                'target_alloc': target
            })
    # Handle CASH if not covered
    cash_current = current_allocations.get('CASH', 0)
    cash_diff = cash_current - target_allocations['CASH']
    if abs(cash_diff) > 0.02:
        action = 'SELL' if cash_diff > 0 else 'BUY'
        trades.append({
            'symbol': 'CASH',
            'action': action,
            'amount': abs(cash_diff) * total_combined,
            'current_alloc': cash_current,
            'target_alloc': target_allocations['CASH']
        })
    return {'total_combined': total_combined, 'trades': trades}

Then, the generate_trade_orders op:

@op
def generate_trade_orders(context, aggregated):
    trades = aggregated['trades']
    # Generate CSV content
    csv_content = "symbol,action,amount,current_alloc,target_alloc\n"
    for trade in trades:
        csv_content += f"{trade['symbol']},{trade['action']},{trade['amount']},{trade['current_alloc']},{trade['target_alloc']}\n"
    context.log.info(f"Generated trade orders:\n{csv_content}")
    # In a real scenario, write to a file or external system
    return csv_content

Now, the job needs to wire these ops together. The fetch_holdings op yields dynamic outputs, which are then mapped to analyze_portfolio. The outputs of analyze_portfolio are collected into a list and passed to aggregate_analyses. Then, generate_trade_orders depends on aggregate_analyses.

So the job definition would look like:

@job
def portfolio_rebalancing_job():
    analyses = []
    fetch_results = fetch_holdings()
    for brokerage_result in fetch_results:
        analysis = analyze_portfolio(brokerage_result)
        analyses.append(analysis)
    aggregated = aggregate_analyses(analyses)
    generate_trade_orders(aggregated)

Wait, but in Dagster, when using dynamic outputs, you have to use the .map() method or collect the dynamic outputs. Let me recall the correct syntax.

When an op yields dynamic outputs, the downstream ops that process them need to be mapped. So the correct way is:

fetch_results = fetch_holdings()
analyze_results = fetch_results.map(analyze_portfolio)
aggregated = aggregate_analyses(analyze_results.collect())

Then, generate_trade_orders(aggregated)

So the job would be structured as:

@job
def portfolio_rebalancing_job():
    fetch_step = fetch_holdings()
    analyze_step = fetch_step.map(analyze_portfolio)
    aggregate_step = aggregate_analyses(analyze_step.collect())
    generate_trade_orders(aggregate_step)

Yes, that's the correct way. The .map() method applies the analyze_portfolio op to each dynamic output from fetch_holdings. Then, .collect() gathers all the results into a list, which is passed to aggregate_analyses.

Putting it all together, the code would be:

from dagster import op, job, DynamicOut, DynamicOutput, Out, graph_asset, graph, In, Nothing
# But wait, the DynamicOut and DynamicOutput require specific handling.

Wait, the fetch_holdings op needs to be configured to produce dynamic outputs. So the op should specify that it has dynamic outputs. So in the @op decorator, we need to set out=DynamicOut().

Wait, no. The way to yield dynamic outputs is by using the context's dynamic_output method or yielding DynamicOutput objects. Let me check the correct syntax.

In Dagster, to create dynamic outputs, the op must be defined with the out=DynamicOut() parameter. Wait, no. The DynamicOut is part of the op's definition if the op is configured to dynamically map downstream ops. Alternatively, when an op yields DynamicOutput objects, it can dynamically create multiple outputs.

Wait, perhaps I need to adjust the fetch_holdings op to have a dynamic output. Let me refer to Dagster's documentation.

In Dagster, an op can emit multiple outputs dynamically by yielding DynamicOutput(value, mapping_key). To do this, the op must be decorated with out=DynamicOut().

Wait, the correct approach is:

@op(out=DynamicOut())
def fetch_holdings(context):
    brokerages = ['BROKERAGE_001', 'BROKERAGE_002', 'BROKERAGE_003', 'BROKERAGE_004', 'BROKERAGE_005']
    for brokerage_id in brokerages:
        holdings_data = [...]  # Simulated data
        yield DynamicOutput(holdings_data, mapping_key=brokerage_id)

Then, downstream ops can be mapped over these dynamic outputs.

So the job would be:

@job
def portfolio_rebalancing_job():
    dynamic_data = fetch_holdings()
    analysis_results = dynamic_data.map(analyze_portfolio)
    aggregated = aggregate_analyses(analysis_results.collect())
    generate_trade_orders(aggregated)

Yes, that's correct.

Now, putting all the code together.

Also, the user mentioned that if resources or config are needed, provide simplified examples as comments. The target allocations are embedded in the aggregation logic, which is already handled in the code.

The user also mentioned including a minimal launch pattern, so adding the if __name__ == '__main__' block.

Testing for PEP 8 compliance, function names are snake_case, which they are.

Now, let's write the code:

from dagster import op, job, DynamicOut, DynamicOutput, In, Out, Nothing, graph, graph_asset
from typing import List, Dict

@op(out=DynamicOut())
def fetch_holdings(context):
    brokerages = ['BROKERAGE_001', 'BROKERAGE_002', 'BROKERAGE_003', 'BROKERAGE_004', 'BROKERAGE_005']
    for brokerage_id in brokerages:
        # Simulated API call to fetch holdings
        holdings_data = [
            {'symbol': 'AAPL', 'quantity': 100, 'price': 150},
            {'symbol': 'GOOGL', 'quantity': 50, 'price': 2500},
            {'symbol': 'CASH', 'quantity': 50000, 'price': 1}  # Adding CASH for example
        ]
        yield DynamicOutput(holdings_data, mapping_key=brokerage_id)

@op
def analyze_portfolio(context, holdings: List[Dict]) -> Dict:
    """Analyzes portfolio holdings to compute metrics."""
    total_value = sum(h['quantity'] * h['price'] for h in holdings)
    allocation = {h['symbol']: (h['quantity'] * h['price']) / total_value for h in holdings}
    risk_score = len(holdings)
    return {
        'total_value': total_value,
        'allocation': allocation,
        'risk_score': risk_score
    }

@op
def aggregate_analyses(context, analyses: List[Dict]) -> Dict:
    """Aggregates analyses and calculates rebalancing trades."""
    total_combined = sum(analysis['total_value'] for analysis in analyses)
    
    # Compute combined allocations
    current_allocations = {}
    for analysis in analyses:
        for symbol, alloc in analysis['allocation'].items():
            current_allocations[symbol] = current_allocations.get(symbol, 0) + (alloc * analysis['total_value'] / total_combined)
    
    # Target allocations
    target_allocations = {'AAPL': 0.3, 'GOOGL': 0.25, 'MSFT': 0.2, 'CASH': 0.25}
    trades = []
    
    # Check each symbol against target
    for symbol in current_allocations:
        current = current_allocations[symbol]
        target = target_allocations.get(symbol, 0)
        difference = current - target
        if abs(difference) > 0.02:
            action = 'SELL' if difference > 0 else 'BUY'
            trades.append({
                'symbol': symbol,
                'action': action,
                'amount': abs(difference) * total_combined,
                'current_alloc': round(current, 4),
                'target_alloc': target
            })
    
    # Ensure CASH is considered even if not in current_allocations
    cash_current = current_allocations.get('CASH', 0)
    cash_diff = cash_current - target_allocations['CASH']
    if abs(cash_diff) > 0.02:
        action = 'SELL' if cash_diff > 0 else 'BUY'
        trades.append({
            'symbol': 'CASH',
            'action': action,
            'amount': abs(cash_diff) * total_combined,
            'current_alloc': round(cash_current, 4),
            'target_alloc': target_allocations['CASH']
        })
    
    return {
        'total_combined': total_combined,
        'trades': trades
    }

@op
def generate_trade_orders(context, aggregated: Dict) -> None:
    """Generates trade orders CSV from aggregated results."""
    trades = aggregated['trades']
    csv_content = "symbol,action,amount,current_alloc,target_alloc\n"
    for trade in trades:
        csv_content += f"{trade['symbol']},{trade['action']},{trade['amount']:.2f},{trade['current_alloc']:.4f},{trade['target_alloc']:.4f}\n"
    # In production, write to file or external system
    context.log.info(f"Trade Orders CSV:\n{csv_content}")

@job
def portfolio_rebalancing_job():
    holdings = fetch_holdings()
    analyses = holdings.map(analyze_portfolio)
    aggregated = aggregate_analyses(analyses.collect())
    generate_trade_orders(aggregated)

if __name__ == '__main__':
    portfolio_rebalancing_job.execute_in_process()

Wait, the generate_trade_orders op returns None, but in the code, it returns csv_content. Let me check. The function returns csv_content, but the op's output is not defined. So perhaps the op should have an output defined. Alternatively, since the function returns the CSV content, but the op doesn't specify an output, Dagster will infer it. However, in the current code, generate_trade_orders doesn't have an output specified. Let's adjust that.

Wait, in the code above, generate_trade_orders is defined as:

@op
def generate_trade_orders(context, aggregated: Dict) -> None:
    ... 
    return csv_content

Wait, no. The code as written has a return statement returning csv_content, but the function is annotated to return None. That's a mistake. So correct the function:

@op
def generate_trade_orders(context, aggregated: Dict):
    """Generates trade orders CSV from aggregated results."""
    trades = aggregated['trades']
    csv_content = "symbol,action,amount,current_alloc,target_alloc\n"
    for trade in trades:
        csv_content += f"{trade['symbol']},{trade['action']},{trade['amount']:.2f},{trade['current_alloc']:.4f},{trade['target_alloc']:.4f}\n"
    # In production, write to file or external system
    context.log.info(f"Trade Orders CSV:\n{csv_content}")
    return csv_content

But since the op doesn't have an explicit output, Dagster will infer the output type. Alternatively, add an Out().

But the user's pipeline says the output is a CSV file. However, the code here logs the CSV content and returns it. Since the user didn't specify where to write it (like a file), perhaps just returning it is sufficient for the example.

Now, checking all the code for syntax errors. The analyze_portfolio op takes holdings as input, which is the output of fetch_holdings. Since fetch_holdings yields DynamicOutputs, each mapped analyze_portfolio will receive one holdings_data list.

The aggregate_analyses op takes a list of the analysis results, which is collected via .collect() on the mapped results.

The generate_trade_orders op takes the aggregated data.

The job is correctly structured with the dynamic mapping and collection.

The launch pattern is included with the execute_in_process() call.

I think this should work. Let me check for any missing parts.

The code includes all necessary imports except perhaps for typing.List and Dict. But in Python 3.9+, it's okay to use list and dict. However, the code uses List and Dict from the typing module, which requires importing them. So adding:

from typing import List, Dict

But in the code above, the functions have type annotations using List[Dict], etc. So yes, the imports are needed.

So the code should be correct.
</think>

from dagster import op, job, DynamicOut, DynamicOutput
from typing import List, Dict

@op(out=DynamicOut())
def fetch_holdings(context):
    brokerages = ['BROKERAGE_001', 'BROKERAGE_002', 'BROKERAGE_003', 'BROKERAGE_004', 'BROKERAGE_005']
    for brokerage_id in brokerages:
        holdings_data = [
            {'symbol': 'AAPL', 'quantity': 100, 'price': 150},
            {'symbol': 'GOOGL', 'quantity': 50, 'price': 2500},
            {'symbol': 'CASH', 'quantity': 50000, 'price': 1}
        ]
        yield DynamicOutput(holdings_data, mapping_key=brokerage_id)

@op
def analyze_portfolio(context, holdings: List[Dict]) -> Dict:
    total_value = sum(h['quantity'] * h['price'] for h in holdings)
    allocation = {h['symbol']: (h['quantity'] * h['price']) / total_value for h in holdings}
    return {
        'total_value': total_value,
        'allocation': allocation,
        'risk_score': len(holdings)
    }

@op
def aggregate_analyses(context, analyses: List[Dict]) -> Dict:
    total_combined = sum(a['total_value'] for a in analyses)
    current_allocations = {}
    
    for analysis in analyses:
        for symbol, alloc in analysis['allocation'].items():
            current_allocations[symbol] = current_allocations.get(symbol, 0) + (alloc * analysis['total_value'] / total_combined)

    target_allocations = {'AAPL': 0.3, 'GOOGL': 0.25, 'MSFT': 0.2, 'CASH': 0.25}
    trades = []
    
    for symbol in current_allocations:
        current = current_allocations[symbol]
        target = target_allocations.get(symbol, 0)
        if abs(current - target) > 0.02:
            action = 'SELL' if current > target else 'BUY'
            trades.append({
                'symbol': symbol,
                'action': action,
                'amount': abs(current - target) * total_combined,
                'current_alloc': round(current, 4),
                'target_alloc': target
            })

    cash_current = current_allocations.get('CASH', 0)
    cash_diff = cash_current - target_allocations['CASH']
    if abs(cash_diff) > 0.02:
        action = 'SELL' if cash_diff > 0 else 'BUY'
        trades.append({
            'symbol': 'CASH',
            'action': action,
            'amount': abs(cash_diff) * total_combined,
            'current_alloc': round(cash_current, 4),
            'target_alloc': target_allocations['CASH']
        })

    return {'total_combined': total_combined, 'trades': trades}

@op
def generate_trade_orders(context, aggregated: Dict):
    csv_content = "symbol,action,amount,current_alloc,target_alloc\n"
    for trade in aggregated['trades']:
        csv_content += f"{trade['symbol']},{trade['action']},{trade['amount']:.2f},{trade['current_alloc']:.4f},{trade['target_alloc']:.4f}\n"
    context.log.info(f"Generated trade orders:\n{csv_content}")
    return csv_content

@job
def portfolio_rebalancing_job():
    analyses = fetch_holdings().map(analyze_portfolio)
    generate_trade_orders(aggregate_analyses(analyses.collect()))

if __name__ == '__main__':
    portfolio_rebalancing_job.execute_in_process()