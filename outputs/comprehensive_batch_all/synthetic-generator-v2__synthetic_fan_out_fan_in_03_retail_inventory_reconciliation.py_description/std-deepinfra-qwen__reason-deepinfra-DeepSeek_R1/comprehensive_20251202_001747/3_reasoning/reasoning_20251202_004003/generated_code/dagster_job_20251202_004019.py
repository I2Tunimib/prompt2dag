<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has four main steps: fetching CSV files from four warehouses in parallel, normalizing SKU formats concurrently, reconciling discrepancies, and generating a final report. 

First, I'll create ops for each step. The fetch step is parallel for four warehouses, so I'll make four separate ops for fetching north, south, east, and west data. Each fetch op can be an @op. Then normalization also needs to happen in parallel for each warehouse, so four more normalization ops. These normalization ops depend on their respective fetch ops. 

Next, the reconcile step needs all four normalized datasets, so that op will depend on all four normalization ops. Then the generate report op depends on the reconcile step.

Wait, the user mentioned using standard Python branching if needed, but the description says no external sensors or conditional branching. So all dependencies are linear with parallel steps. So the structure should be: fetch ops (parallel) → normalize ops (parallel, each depending on their fetch) → reconcile (depends on all normalize) → generate report (depends on reconcile).

Now, for the job, I'll use @job to define the dependencies. Each fetch op is independent, so they can run in parallel. Each normalize op depends on its fetch. Then reconcile depends on all four normalize ops. Then generate report depends on reconcile.

For resources and config, the user said to provide minimal stubs. Maybe the fetch ops need warehouse names, which can come from config. So each fetch op can take a config parameter for warehouse_name. Alternatively, since there are four separate fetch ops, maybe each is hardcoded to their warehouse. But using config might be more flexible. Wait, but the four fetch ops are for north, south, east, west. So perhaps each fetch op is specific to a warehouse. So separate functions: fetch_north, fetch_south, etc. That way, each can be called without config, but maybe they share common code. Alternatively, have a single fetch op that's called four times with different config. But in Dagster, you can't dynamically generate ops, so better to have four separate ops.

So, four fetch ops, each for a warehouse. Similarly, four normalize ops. Each normalize op takes the output of its respective fetch op. Then reconcile takes all four normalized outputs.

For the functions themselves, the user mentioned mock functions. So each fetch op could return a mock DataFrame or file path. The normalize ops would process that data, applying transformations like uppercase, stripping whitespace, standardizing prefixes. The reconcile op would compare all four datasets, find discrepancies, and generate a report. The generate report op would take the discrepancy data and create a PDF.

Now, dependencies: in the job, after defining all the ops, set the dependencies. For example, normalize_north depends on fetch_north, etc. Then reconcile depends on all four normalize ops. Then generate_report depends on reconcile.

Regarding resources and retries: The user mentioned daily schedule with retries. But the code example needs to include the job with retries configuration. So in the @job decorator, add config for retries. Also, the launch pattern at the end with execute_in_process.

Putting it all together:

- Define four fetch ops (fetch_north, fetch_south, etc.) each returning some data.
- Define four normalize ops (normalize_north, etc.), each taking the respective fetch op's output.
- Define reconcile op taking all four normalized outputs.
- Define generate_report op taking reconcile's output.
- Create the job with these ops and dependencies.

Also, add the necessary imports: from dagster import op, job, RetryPolicy.

Wait, the user mentioned maximum 2 retries with 5-minute delay. So in the job, set a retry policy. So in the @job decorator, include a retry_policy parameter with RetryPolicy(max_retries=2, delay=300). But wait, Dagster's RetryPolicy uses delay in seconds, so 5 minutes is 300 seconds.

For the schedule, the user said to include a daily schedule, but the code example doesn't need the schedule definition unless asked. The question says to provide the module with the job, and the launch pattern. So maybe the schedule is out of scope here since the user didn't ask to define the schedule, just the job.

Now, code structure:

Imports first: from dagster import op, job, RetryPolicy.

Then each op with @op decorator. The fetch ops could simulate fetching CSV, maybe returning a dictionary or a DataFrame. The normalize ops process the data. The reconcile op compares all four and generates discrepancies. The generate_report creates a PDF (mock function).

Finally, the job with dependencies and retry policy.

Testing the code: Each op's function should have a mock implementation. For example, fetch_north could return a sample DataFrame. Normalize ops process it. Reconcile checks for discrepancies. Generate report creates a PDF (maybe just a print statement or a mock file).

So putting it all together, the code would look like this:
</think>

from dagster import op, job, RetryPolicy

@op
def fetch_north():
    # Mock CSV fetch for north warehouse
    return {"sku_data": ["north_123", " north_456 "]}

@op
def fetch_south():
    # Mock CSV fetch for south warehouse
    return {"sku_data": ["south-789", " south-012 "]}

@op
def fetch_east():
    # Mock CSV fetch for east warehouse
    return {"sku_data": ["east_345", " east_678 "]}

@op
def fetch_west():
    # Mock CSV fetch for west warehouse
    return {"sku_data": ["west_901", " west_234 "]}

@op
def normalize_north(context, data):
    skus = data["sku_data"]
    normalized = [sku.strip().upper().replace("NORTH_", "STD-") for sku in skus]
    context.log.info(f"Normalized north: {normalized}")
    return {"normalized": normalized}

@op
def normalize_south(context, data):
    skus = data["sku_data"]
    normalized = [sku.strip().upper().replace("SOUTH-", "STD-") for sku in skus]
    context.log.info(f"Normalized south: {normalized}")
    return {"normalized": normalized}

@op
def normalize_east(context, data):
    skus = data["sku_data"]
    normalized = [sku.strip().upper().replace("EAST_", "STD-") for sku in skus]
    context.log.info(f"Normalized east: {normalized}")
    return {"normalized": normalized}

@op
def normalize_west(context, data):
    skus = data["sku_data"]
    normalized = [sku.strip().upper().replace("WEST_", "STD-") for sku in skus]
    context.log.info(f"Normalized west: {normalized}")
    return {"normalized": normalized}

@op
def reconcile_inventory(context, north, south, east, west):
    all_skus = set()
    all_skus.update(north["normalized"])
    all_skus.update(south["normalized"])
    all_skus.update(east["normalized"])
    all_skus.update(west["normalized"])
    
    discrepancies = []
    for sku in all_skus:
        counts = [
            north["normalized"].count(sku),
            south["normalized"].count(sku),
            east["normalized"].count(sku),
            west["normalized"].count(sku)
        ]
        if len(set(counts)) > 1:
            discrepancies.append({"sku": sku, "counts": counts})
    
    context.log.info(f"Found {len(discrepancies)} discrepancies")
    return {"discrepancies": discrepancies}

@op
def generate_reconciliation_report(context, discrepancies):
    # Mock PDF generation
    report_data = "\n".join(
        f"SKU: {d['sku']}, Counts: {d['counts']}" 
        for d in discrepancies["discrepancies"]
    )
    context.log.info(f"Generated report:\n{report_data}")
    return {"status": "success", "report_path": "/reports/final_report.pdf"}

@job(retry_policy=RetryPolicy(max_retries=2, delay=300))
def inventory_reconciliation_job():
    north_data = fetch_north()
    south_data = fetch_south()
    east_data = fetch_east()
    west_data = fetch_west()
    
    norm_n = normalize_north(north_data)
    norm_s = normalize_south(south_data)
    norm_e = normalize_east(east_data)
    norm_w = normalize_west(west_data)
    
    discrepancies = reconcile_inventory(norm_n, norm_s, norm_e, norm_w)
    generate_reconciliation_report(discrepancies)

if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()