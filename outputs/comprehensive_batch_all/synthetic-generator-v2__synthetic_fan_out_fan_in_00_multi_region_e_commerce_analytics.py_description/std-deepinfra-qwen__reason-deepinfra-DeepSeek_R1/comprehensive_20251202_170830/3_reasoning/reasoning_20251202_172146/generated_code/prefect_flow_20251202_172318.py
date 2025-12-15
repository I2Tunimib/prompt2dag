<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements and constraints.

First, the pipeline has multiple steps with parallel processing. The user mentioned using .submit() for parallel tasks and normal calls for sequential ones. So, for the ingestion and conversion steps, which are parallel, I'll use task.submit(). The aggregation is sequential, so just call it normally.

The steps are: start, four ingestion tasks in parallel, four conversion tasks in parallel (depending on ingestion), then aggregation, then end. The start and end are marked with EmptyOperators in the description, but in Prefect, tasks can be simple. Maybe create dummy tasks for start and end that just log or pass.

Wait, the user said to use @task for each step. So each of those steps (ingest, convert, aggregate) should be tasks. The start and end are also tasks, even if they don't do much. But in Prefect, every step in the flow is a task or a subflow. So maybe the start and end are tasks that just pass.

Looking at the pipeline steps:

- Start pipeline: EmptyOperator. So maybe a task that does nothing, maybe logs something.
- Then four ingestion tasks in parallel. Each ingest a region's data. These can be four separate tasks, but since they're similar, maybe a parameterized task. But the user wants explicit task functions. So perhaps separate tasks for each region. Wait, but the description says four parallel data ingestion tasks, each for a region. So maybe a single task function that takes a region as a parameter, called four times with different regions. But the user's example might prefer separate functions. Hmm.

Wait, the user's example says "Define @task functions for each step". So each of the four ingestion steps would be separate tasks? That might be redundant code. Alternatively, maybe a single task that handles ingestion for any region, called four times with different parameters. But the problem is that the user's example shows four parallel tasks with different names (US-East, US-West, etc.), so perhaps separate functions. But that's a lot of repeated code. Alternatively, maybe a single task function that's called four times with different parameters, but the user's instructions say to use clear function names derived from the steps. So perhaps separate tasks for each region's ingestion.

Wait, the pipeline steps list:

- Ingest US-East region sales data from CSV source
- Ingest US-West region sales data from CSV source  
- Ingest EU region sales data from CSV source
- Ingest APAC region sales data from CSV source

These are four separate tasks. So perhaps four separate @task functions, each named appropriately. But that's repetitive. Alternatively, maybe a helper function and parameterized tasks. But the user wants explicit dependencies. Maybe better to have separate tasks for each region's ingestion. But that would result in four functions like ingest_us_east, ingest_us_west, etc. But that's a lot of code duplication. Alternatively, create a single task function that takes region as a parameter, and then call it four times with different regions. But how to handle the different regions' specifics, like CSV paths or currency handling?

Wait, the conversion tasks also vary: US-East and US-West don't need conversion, EU converts from EUR, APAC from JPY. So maybe the ingestion tasks return the data along with the currency, and the conversion tasks handle accordingly. But perhaps each region's conversion is a separate task. So again, maybe separate functions for each conversion.

Alternatively, have a generic conversion task that takes the data and the currency, and applies the conversion if needed. But the problem is that the US-East and US-West tasks don't need conversion. So perhaps the conversion tasks for US-East and US-West just pass through, while EU and APAC do actual conversion. But how to model that.

Alternatively, create four conversion tasks, each handling a region's data. For US-East and US-West, they just return the data as is. For EU and APAC, they perform the conversion. So perhaps separate @task functions for each conversion step.

But again, that's code duplication. Alternatively, have a single conversion task that takes the region and data, and applies the correct conversion based on the region. That might be better. Let's think.

So, the ingestion tasks would each return the data for their region. Then, the conversion tasks take that data and the region, and convert if necessary. So perhaps:

- ingest_us_east() -> data
- ingest_us_west() -> data
- ingest_eu() -> data
- ingest_apac() -> data

Then, convert_us_east(data) returns data
convert_us_west(data) returns data
convert_eu(data) converts EUR to USD
convert_apac(data) converts JPY to USD

But again, code duplication here. Alternatively, a single conversion task that checks the region and applies conversion if needed. But how to handle that with Prefect tasks. Since each conversion is a separate task in parallel, perhaps each conversion is a separate task function. But that's four functions. Alternatively, create a helper function inside the conversion task that handles the conversion based on region.

But the user's example says "Define @task functions for each step". So each step in the pipeline description is a task. The steps for conversion are four parallel tasks, so each would be a separate @task function. But that's a lot of code. Let me check the pipeline steps again.

The pipeline steps for conversion:

- Convert US-East data (already USD, no conversion needed)
- Convert US-West data (already USD, no conversion needed)
- Convert EU data from EUR to USD using daily exchange rates
- Convert APAC data from JPY to USD using daily exchange rates

So each of these is a separate step, hence separate tasks. So perhaps four conversion tasks. But writing four functions would be repetitive. Alternatively, create a parameterized task function that handles each region's conversion. For example:

@task
def convert_currency(data, region):
    if region in ['US-East', 'US-West']:
        return data
    elif region == 'EU':
        # convert EUR to USD
    elif region == 'APAC':
        # convert JPY to USD
    ...

But then, how to call this task four times with different regions in parallel. Since Prefect tasks can be called multiple times, each call would create a separate task instance. So perhaps in the flow, after ingesting each region's data, we call convert_currency.submit(data, region) for each region. But then, how to get the data from the ingestion tasks.

Wait, the ingestion tasks are four separate tasks (us_east, us_west, eu, apac). Each returns data. Then, each of those data is passed to a convert_currency task. So:

us_east_data = ingest_us_east.submit()
convert_us_east = convert_currency.submit(us_east_data, 'US-East')

Similarly for other regions. But then, the convert_currency task would handle each region's conversion. That way, we have a single task function for conversion, but called four times with different parameters. That reduces code duplication.

But the user's example says to use clear function names derived from the steps. So maybe the conversion steps should have specific names. But perhaps the task can be named based on parameters. Alternatively, the task name can be dynamic, but Prefect 2.x allows task names with parameters, but perhaps it's better to have separate functions for clarity.

Alternatively, perhaps the user expects four separate conversion tasks, each with their own function. But that's a lot of code. Let me think again.

The pipeline description lists four parallel currency conversion tasks, each for a region. So each is a separate task. Therefore, perhaps four @task functions: convert_us_east, convert_us_west, convert_eu, convert_apac. Each takes the data from the respective ingestion task. For US-East and US-West, they just return the data. For EU and APAC, they perform conversion.

But that would require four functions, which is repetitive. But maybe necessary for clarity as per the user's instructions.

Alternatively, create a helper function for conversion and have each task call it. For example:

@task
def convert_us_east(data):
    return data  # no conversion

@task
def convert_us_west(data):
    return data  # no conversion

@task
def convert_eu(data):
    # convert EUR to USD
    ...

@task
def convert_apac(data):
    # convert JPY to USD
    ...

But this way, each conversion task is a separate function. That's more code, but aligns with the pipeline steps as described.

So, perhaps proceed with separate functions for each ingestion and conversion task.

Now, the flow would orchestrate these tasks. The flow would first call the start task, then the four ingestion tasks in parallel, then the four conversion tasks in parallel (each depending on their respective ingestion task), then the aggregation task, then the end task.

But how to structure the dependencies. For example, the start task runs first, then all ingestion tasks run in parallel. Once all ingestion tasks are done, the conversion tasks run in parallel. Once all conversion tasks are done, the aggregation task runs, then the end task.

So the flow would look like:

@flow
def ecommerce_analytics_pipeline():
    start_task()
    us_east_data = ingest_us_east()
    us_west_data = ingest_us_west()
    eu_data = ingest_eu()
    apac_data = ingest_apac()
    # Wait for all ingestion tasks to complete
    us_east_converted = convert_us_east(us_east_data)
    us_west_converted = convert_us_west(us_west_data)
    eu_converted = convert_eu(eu_data)
    apac_converted = convert_apac(apac_data)
    # Wait for all conversions
    aggregate_task(us_east_converted, us_west_converted, eu_converted, apac_converted)
    end_task()

But since the ingestion and conversion steps are parallel, we need to submit them as asynchronous tasks. So in Prefect 2.x, to run tasks in parallel, we use .submit(), which returns a future-like object. Then, we need to wait for all of them to complete before proceeding.

So the flow would be:

@flow
def ecommerce_analytics_pipeline():
    start_task()
    # Ingest all regions in parallel
    us_east_future = ingest_us_east.submit()
    us_west_future = ingest_us_west.submit()
    eu_future = ingest_eu.submit()
    apac_future = ingest_apac.submit()
    # Convert all regions in parallel, each depending on their ingestion future
    us_east_conv_future = convert_us_east.submit(us_east_future)
    us_west_conv_future = convert_us_west.submit(us_west_future)
    eu_conv_future = convert_eu.submit(eu_future)
    apac_conv_future = convert_apac.submit(apac_future)
    # Wait for all conversions to complete
    aggregated_data = aggregate_task.submit(
        us_east_conv_future, us_west_conv_future, eu_conv_future, apac_conv_future
    )
    end_task()
    return aggregated_data

Wait, but the aggregation task needs to collect all converted data. So each conversion task's result is passed to the aggregation task. Using .submit() would mean that the aggregation task is also run as a background task, but since it's sequential, maybe we call it normally. Wait, no: the aggregation task depends on all conversion tasks, so after submitting all conversion tasks, we need to gather their results and pass to the aggregation task.

But in Prefect, when you submit a task, you get a PrefectFuture. To get the result, you can call .result() on it, which will block until the task is complete. So perhaps:

@flow
def ecommerce_analytics_pipeline():
    start_task()
    # Ingest in parallel
    us_east_data = ingest_us_east.submit().result()
    us_west_data = ingest_us_west.submit().result()
    eu_data = ingest_eu.submit().result()
    apac_data = ingest_apac.submit().result()
    # Convert in parallel
    us_east_conv = convert_us_east.submit(us_east_data).result()
    us_west_conv = convert_us_west.submit(us_west_data).result()
    eu_conv = convert_eu.submit(eu_data).result()
    apac_conv = convert_apac.submit(apac_data).result()
    # Aggregate
    aggregate_task(us_east_conv, us_west_conv, eu_conv, apac_conv)
    end_task()

But this approach would not run the ingestion tasks in parallel because each .submit().result() blocks until completion. So the first ingest_us_east.submit().result() would wait for that task to finish before submitting the next. That's not parallel. So to run them in parallel, we need to submit all four ingestion tasks first, then wait for their results.

So:

@flow
def ecommerce_analytics_pipeline():
    start_task()
    # Submit all ingestion tasks
    us_east_future = ingest_us_east.submit()
    us_west_future = ingest_us_west.submit()
    eu_future = ingest_eu.submit()
    apac_future = ingest_apac.submit()
    # Wait for all to complete
    us_east_data = us_east_future.result()
    us_west_data = us_west_future.result()
    eu_data = eu_future.result()
    apac_data = apac_future.result()
    # Submit all conversion tasks
    us_east_conv_future = convert_us_east.submit(us_east_data)
    us_west_conv_future = convert_us_west.submit(us_west_data)
    eu_conv_future = convert_eu.submit(eu_data)
    apac_conv_future = convert_apac.submit(apac_data)
    # Wait for conversions
    us_east_conv = us_east_conv_future.result()
    us_west_conv = us_west_conv_future.result()
    eu_conv = eu_conv_future.result()
    apac_conv = apac_conv_future.result()
    # Aggregate
    aggregate_task(us_east_conv, us_west_conv, eu_conv, apac_conv)
    end_task()

But this way, the ingestion tasks are submitted in parallel, then we wait for all of them. Then submit conversion tasks in parallel, wait for all, then aggregate. This ensures that all ingestion tasks run in parallel, then conversions in parallel.

But Prefect handles dependencies automatically. If you submit tasks and their dependencies are futures, Prefect will manage the execution. So perhaps a better approach is to submit all ingestion tasks, then submit all conversion tasks (each depending on their respective ingestion future), then aggregate depends on all conversion futures.

So:

@flow
def ecommerce_analytics_pipeline():
    start = start_task.submit()
    # Ingest in parallel
    us_east_ingest = ingest_us_east.submit(wait_for=[start])
    us_west_ingest = ingest_us_west.submit(wait_for=[start])
    eu_ingest = ingest_eu.submit(wait_for=[start])
    apac_ingest = ingest_apac.submit(wait_for=[start])
    # Convert in parallel, each depends on their ingest
    us_east_conv = convert_us_east.submit(us_east_ingest)
    us_west_conv = convert_us_west.submit(us_west_ingest)
    eu_conv = convert_eu.submit(eu_ingest)
    apac_conv = convert_apac.submit(apac_ingest)
    # Aggregate depends on all conversions
    agg = aggregate_task.submit(us_east_conv, us_west_conv, eu_conv, apac_conv)
    end_task.submit(wait_for=[agg])

But Prefect 2.x automatically tracks dependencies based on the arguments passed. So if a task is called with the result of another task's future, Prefect knows to wait for that future. So perhaps:

@flow
def ecommerce_analytics_pipeline():
    start_task()
    us_east_data = ingest_us_east.submit()
    us_west_data = ingest_us_west.submit()
    eu_data = ingest_eu.submit()
    apac_data = ingest_apac.submit()
    us_east_conv = convert_us_east.submit(us_east_data)
    us_west_conv = convert_us_west.submit(us_west_data)
    eu_conv = convert_eu.submit(eu_data)
    apac_conv = convert_apac.submit(apac_data)
    aggregate_task(us_east_conv, us_west_conv, eu_conv, apac_conv)
    end_task()

In this case, the start_task() is called first (as a normal task, not submitted), which would run before the ingestion tasks. Then, the ingestion tasks are submitted in parallel. Each conversion task depends on its respective ingestion task's future. The aggregate task depends on all conversion futures. The end_task runs after aggregate.

But how to ensure that start_task runs first, and end_task runs last. Since in Prefect, tasks are executed when their dependencies are met. So if start_task is a normal task (not submitted), it runs first. Then the ingestion tasks are submitted, which depend on start_task completing? Or does the flow's structure dictate the order?

Wait, if start_task is a normal task (called with start_task()), then it runs sequentially before the next lines. Then the ingestion tasks are submitted, which can run in parallel. Then the conversion tasks are submitted, each depending on their respective ingestion task. Then aggregate_task is called with all conversion futures, which waits for all of them. Then end_task is called normally, which depends on aggregate_task completing.

But the start and end tasks are supposed to be EmptyOperators, which in Prefect can be simple tasks that do nothing but mark the start and end.

So code outline:

from prefect import flow, task

@task
def start_pipeline():
    pass  # or log something

@task
def ingest_us_east():
    # read CSV and return data
    pass

@task
def ingest_us_west():
    pass

@task
def ingest_eu():
    pass

@task
def ingest_apac():
    pass

@task
def convert_us_east(data):
    return data  # no conversion

@task
def convert_us_west(data):
    return data

@task
def convert_eu(data):
    # convert EUR to USD
    pass

@task
def convert_apac(data):
    # convert JPY to USD
    pass

@task
def aggregate_data(us_east, us_west, eu, apac):
    # aggregate and generate report
    pass

@task
def end_pipeline():
    pass

@flow
def ecommerce_analytics_pipeline():
    start_pipeline()
    us_east_future = ingest_us_east.submit()
    us_west_future = ingest_us_west.submit()
    eu_future = ingest_eu.submit()
    apac_future = ingest_apac.submit()
    
    us_east_conv = convert_us_east.submit(us_east_future)
    us_west_conv = convert_us_west.submit(us_west_future)
    eu_conv = convert_eu.submit(eu_future)
    apac_conv = convert_apac.submit(apac_future)
    
    agg_future = aggregate_data.submit(us_east_conv, us_west_conv, eu_conv, apac_conv)
    
    end_pipeline.submit(wait_for=[agg_future])

if __name__ == "__main__":
    ecommerce_analytics_pipeline()

But wait, the start_pipeline task is called normally (not submitted), which means it runs immediately, and the next tasks are submitted. The ingestion tasks are submitted in parallel. Each conversion task is submitted with the respective ingestion future. The aggregate task is submitted with all conversion futures. The end_pipeline is submitted and depends on the aggregate future.

But in Prefect, when you call a task with .submit(), it's added to the flow's task runner and runs asynchronously. The flow will wait for all submitted tasks to complete before finishing.

Now, the currency conversion steps: for US-East and US-West, they just return the data. For EU and APAC, they need to convert using exchange rates. So the convert_eu and convert_apac tasks need to integrate with exchange rate services. The user mentioned "currency exchange rate integration for EUR and JPY conversions". So perhaps those tasks fetch the daily exchange rate and apply it.

But for the code, we can mock that. For example:

@task
def convert_eu(data):
    # Assume data is a DataFrame with 'amount_eur' column
    exchange_rate = get_daily_exchange_rate('EUR')
    data['amount_usd'] = data['amount_eur'] * exchange_rate
    return data

Similarly for JPY. But the actual implementation details are not provided, so we can add placeholders with comments.

Now, the aggregation task collects all converted data, calculates regional revenues, sums them, and generates a CSV. So the aggregate_data task would take the four converted data inputs, process them, and output the report.

Regarding task retries and email notifications: the user mentioned "Task retries with 5-minute delays on failure" and "Email notifications on task failures". In Prefect, this is handled via task decorator parameters or flow settings. For example:

@task(retries=3, retry_delay_seconds=300)
def ingest_us_east():
    ...

But the user's code needs to include these parameters. So each task should have retries and retry_delay_seconds set appropriately.

Also, the scheduling: the user mentioned daily execution schedule with no catch-up. So in Prefect, this would be handled via a deployment configuration. The code should include a comment indicating that deployment/schedule configuration is needed, but the code itself doesn't need to include the schedule. The user's instruction says "if schedules are mentioned, include a brief comment indicating deployment/schedule configuration (optional)". So add a comment in the code about scheduling.

Putting it all together:

- Start and end tasks as simple pass-through.
- Four ingestion tasks with retries and delays.
- Four conversion tasks, two of which do nothing, two convert currencies.
- Aggregate task that processes all data.
- The flow orchestrates them with .submit() for parallel steps.
- Add retry parameters to tasks.
- Include email notification setup? Probably not, since that's part of Prefect configuration (e.g., notifications via Prefect Cloud/Server). The code can't include that directly, but the user mentioned "Email notifications on task failures" as part of the pipeline description. So perhaps add a comment indicating that notifications are configured via Prefect settings.

But the user's code constraints say to output only the code, no comments about usage. However, the pipeline description mentions email notifications, so perhaps include a comment in the task decorator about notifications, but Prefect 2.x handles notifications via event hooks or via the UI. So maybe not necessary to include in code. The user's instruction says to include comments where helpful.

So, code steps:

1. Import necessary modules.
2. Define all tasks with @task decorators, including retries and delays.
3. Define the flow with @flow.
4. In the flow, call start task, then submit ingestion tasks, then submit conversion tasks, then aggregate, then end task.
5. Add if __name__ == '__main__' block.

Now, code:

from prefect import flow, task
# Assuming pandas is used for data processing
import pandas as pd

@task(retries=3, retry_delay_seconds=300)
def start_pipeline():
    """Marks the start of the pipeline."""
    print("Pipeline started.")

@task(retries=3, retry_delay_seconds=300)
def ingest_us_east() -> pd.DataFrame:
    """Ingest US-East sales data from CSV."""
    # Implementation placeholder
    return pd.read_csv("s3://bucket/us-east-sales.csv")

@task(retries=3, retry_delay_seconds=300)
def ingest_us_west() -> pd.DataFrame:
    """Ingest US-West sales data from CSV."""
    return pd.read_csv("s3://bucket/us-west-sales.csv")

@task(retries=3, retry_delay_seconds=300)
def ingest_eu() -> pd.DataFrame:
    """Ingest EU sales data from CSV."""
    return pd.read_csv("s3://bucket/eu-sales.csv")

@task(retries=3, retry_delay_seconds=300)
def ingest_apac() -> pd.DataFrame:
    """Ingest APAC sales data from CSV."""
    return pd.read_csv("s3://bucket/apac-sales.csv")

@task(retries=3, retry_delay_seconds=300)
def convert_us_east(data: pd.DataFrame) -> pd.DataFrame:
    """No conversion needed for US-East (already USD)."""
    return data

@task(retries=3, retry_delay_seconds=300)
def convert_us_west(data: pd.DataFrame) -> pd.DataFrame:
    """No conversion needed for US-West (already USD)."""
    return data

@task(retries=3, retry_delay_seconds=300)
def convert_eu(data: pd.DataFrame) -> pd.DataFrame:
    """Convert EUR amounts to USD using daily exchange rates."""
    exchange_rate = 1.08  # Example rate, should fetch from API
    data['amount_usd'] = data['amount_eur'] * exchange_rate
    return data

@task(retries=3, retry_delay_seconds=300)
def convert_apac(data: pd.DataFrame) -> pd.DataFrame:
    """Convert JPY amounts to USD using daily exchange rates."""
    exchange_rate = 0.0077  # Example rate
    data['amount_usd'] = data['amount_jpy'] * exchange_rate
    return data

@task(retries=3, retry_delay_seconds=300)
def aggregate_data(
    us_east: pd.DataFrame,
    us_west: pd.DataFrame,
    eu: pd.DataFrame,
    apac: pd.DataFrame,
) -> None:
    """Aggregate regional data and generate global report."""
    # Concatenate data and calculate totals
    combined = pd.concat([us_east, us_west, eu, apac])
    total_revenue = combined['amount_usd'].sum()
    # Generate report
    report = f"Global Revenue Report\nTotal USD: {total_revenue}"
    print(report)
    # Save to CSV (example path)
    combined.to_csv("global_revenue_report.csv", index=False)

@task(retries=3, retry_delay_seconds=300)
def end_pipeline():
    """Marks the end of the pipeline."""
    print("Pipeline completed.")

@flow
def ecommerce_analytics_pipeline():
    # Schedule comment: Configure daily execution via deployment
    start_pipeline()
    # Fan-out: Ingest regional data in parallel
    us_east_data = ingest_us_east.submit()
    us_west_data = ingest_us_west.submit()
    eu_data = ingest_eu.submit()
    apac_data = ingest_apac.submit()
    # Fan-out: Convert currencies in parallel
    us_east_conv = convert_us_east.submit(us_east_data)
    us_west_conv = convert_us_west.submit(us_west_data)
    eu_conv = convert_eu.submit(eu_data)
    apac_conv = convert_apac.submit(apac_data)
    # Fan-in: Aggregate all data
    aggregate_data.submit(us_east_conv, us_west_conv, eu_conv, apac_conv)
    end_pipeline()

if __name__ == "__main__":
    ecommerce_analytics_pipeline()

This code includes all tasks with retries and delays. The start and end tasks log messages. The ingestion tasks read CSV files (placeholders with example paths). Conversion tasks handle currency conversion with example exchange rates. The aggregate task combines data and saves a report.

The flow uses .submit() for parallel tasks. The start_pipeline is called normally to ensure it runs first. The end_pipeline is called after aggregation.

The code includes retries and retry delays as specified. Email notifications are not included in the code since that's typically handled via Prefect's notification system, which is configured outside the flow code.

The schedule is mentioned in a comment in the flow function. The user's instruction allows a brief comment about deployment/schedule configuration.
</think>

from prefect import flow, task
import pandas as pd

@task(retries=3, retry_delay_seconds=300)
def start_pipeline():
    """Mark pipeline initiation."""
    print("Pipeline started")

@task(retries=3, retry_delay_seconds=300)
def ingest_region(region: str) -> pd.DataFrame:
    """Base ingestion task for regional data."""
    path = f"s3://ecommerce-data/{region}/sales.csv"
    return pd.read_csv(path)

@task(retries=3, retry_delay_seconds=300)
def convert_currency(data: pd.DataFrame, currency: str) -> pd.DataFrame:
    """Convert regional data to USD."""
    if currency != "USD":
        rate = get_exchange_rate(currency)  # Assume external service integration
        data["amount_usd"] = data["amount"] * rate
    return data

@task(retries=3, retry_delay_seconds=300)
def aggregate_reports(dataframes: list[pd.DataFrame]) -> None:
    """Generate consolidated global revenue report."""
    combined = pd.concat(dataframes)
    total = combined["amount_usd"].sum()
    combined.to_csv("global_revenue_report.csv")
    print(f"Global revenue report generated. Total: ${total:,.2f}")

@task(retries=3, retry_delay_seconds=300)
def end_pipeline():
    """Mark pipeline completion."""
    print("Pipeline completed")

@flow
def ecommerce_analytics_pipeline():
    # Deployment configuration: daily schedule with no catch-up
    start_pipeline()
    
    # Fan-out phase 1: Parallel ingestion
    us_east = ingest_region.submit("us-east")
    us_west = ingest_region.submit("us-west")
    eu = ingest_region.submit("eu")
    apac = ingest_region.submit("apac")
    
    # Fan-out phase 2: Parallel conversions
    us_east_conv = convert_currency.submit(us_east, "USD")
    us_west_conv = convert_currency.submit(us_west, "USD")
    eu_conv = convert_currency.submit(eu, "EUR")
    apac_conv = convert_currency.submit(apac, "JPY")
    
    # Fan-in phase: Aggregation
    aggregate_reports.submit([us_east_conv, us_west_conv, eu_conv, apac_conv])
    
    end_pipeline()

if __name__ == "__main__":
    ecommerce_analytics_pipeline()