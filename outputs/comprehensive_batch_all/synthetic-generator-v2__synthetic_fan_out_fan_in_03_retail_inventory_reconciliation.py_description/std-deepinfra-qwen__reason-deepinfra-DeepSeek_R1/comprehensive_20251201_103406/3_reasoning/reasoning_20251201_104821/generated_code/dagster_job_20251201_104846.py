<think>
Okay, let's tackle this problem. The user wants to convert a pipeline description into a Dagster job. Let me start by understanding the requirements.

First, the pipeline has four main steps: fetching CSV files from four warehouses, normalizing SKU formats in parallel, reconciling discrepancies, and generating a final report. The key points are the parallel execution of fetch and normalization steps, followed by sequential steps for reconciliation and report generation.

In Dagster, each step is an op, and the job defines the dependencies between them. Since fetching and normalization need to happen in parallel for each warehouse, I'll create separate ops for each warehouse's fetch and normalize steps. But wait, creating four separate ops for each might be repetitive. Alternatively, I can have a single fetch op that handles all warehouses, but the problem states they should be processed in parallel. Hmm, maybe using a configurable approach with a single op that takes a warehouse name as a parameter. But the user's example might expect separate ops for each warehouse. Let me check the original pipeline description: "Fetch warehouse CSV files in parallel from four locations". So perhaps four separate fetch ops, one for each location.

Similarly, the normalization step is done in parallel for each warehouse. So each fetch op's output is passed to a corresponding normalize op. Then, all normalized data must be collected before the reconciliation step.

So the structure would be:

- fetch_north → normalize_north
- fetch_south → normalize_south
- fetch_east → normalize_east
- fetch_west → normalize_west

Then, all four normalize ops feed into the reconcile_inventory op, which then feeds into generate_report.

Now, how to represent this in Dagster. Each fetch op is an @op, same for normalize. The reconcile op needs to take all four normalized outputs as inputs.

Wait, but in Dagster, each op can have multiple inputs. So the reconcile_inventory op would have four parameters, each coming from the respective normalize op.

Then, the generate_report op depends on the reconcile_inventory op.

Now, for the code structure. Let's outline the ops:

1. Four fetch ops: fetch_north, fetch_south, etc. Each returns some data (maybe a DataFrame or a file path).
2. Four normalize ops: normalize_north, etc., each takes the corresponding fetch op's output.
3. The reconcile op takes all four normalized outputs.
4. The generate report op takes the reconcile output.

But wait, creating four separate fetch ops would require writing four functions, which is repetitive. Alternatively, can we parameterize the warehouse name? But the user's example might prefer explicit ops for clarity. Let me check the constraints: "Use Dagster ops (@op) to represent each step". So each step here is per warehouse. So yes, separate ops.

But that's a lot of repeated code. Maybe using a loop to create the ops, but Dagster doesn't support dynamic op creation. So perhaps write each op explicitly.

Alternatively, create a function that generates the ops dynamically, but that's more complex. The user's example might prefer explicit code.

So, for each warehouse direction (north, south, east, west), create a fetch op and a normalize op.

Wait, but the original pipeline says "fetch CSV files in parallel from four locations". So four fetch tasks running in parallel. Similarly, four normalize tasks in parallel. Each fetch task is followed by a normalize task for the same warehouse.

So in Dagster, the dependencies would be:

fetch_north → normalize_north

fetch_south → normalize_south

etc.

Then, all four normalize ops must complete before reconcile_inventory runs.

So the reconcile_inventory op has four inputs, each from the normalize ops.

Then, generate_report depends on reconcile_inventory.

Now, coding this:

First, define the fetch ops. Each returns some data. Let's mock the fetch function.

@op
def fetch_north():
    # Simulate fetching CSV
    return "north_data"

Similarly for south, east, west.

Then the normalize ops:

@op
def normalize_north(context, data):
    # process data
    return f"normalized_{data}"

Same for others.

But this leads to a lot of repeated code. Alternatively, create a helper function to generate these ops. But since the user wants explicit code, perhaps write each op separately.

Alternatively, use a configurable approach where the op is parameterized by warehouse. But in Dagster, each op is a separate function. So perhaps create a function that takes the warehouse as an argument and returns the processed data.

Wait, but how to handle that with @op decorators. Maybe not. So perhaps better to write each op explicitly, even if repetitive.

Alternatively, use a loop to register the ops. But that's more advanced and might complicate the code. Since the user wants executable code, perhaps stick with explicit definitions.

So, proceed with writing four fetch ops and four normalize ops.

Then, the reconcile op takes all four normalized data as inputs.

@op
def reconcile_inventory(context, north, south, east, west):
    # process all four inputs
    discrepancies = ...
    return discrepancies

Then, generate_report depends on reconcile_inventory.

Now, the job would set up the dependencies:

@job
def inventory_reconciliation_job():
    north_data = fetch_north()
    south_data = fetch_south()
    east_data = fetch_east()
    west_data = fetch_west()

    normalized_north = normalize_north(north_data)
    normalized_south = normalize_south(south_data)
    normalized_east = normalize_east(east_data)
    normalized_west = normalize_west(west_data)

    discrepancies = reconcile_inventory(
        normalized_north, normalized_south, normalized_east, normalized_west
    )
    generate_report(discrepancies)

This sets up the dependencies correctly. Each fetch runs in parallel, then each normalize runs in parallel after their respective fetch. Then reconcile runs after all normalizes complete, and generate_report runs after reconcile.

Now, for the functions themselves. The fetch functions can return mock data. The normalize functions process the data. The reconcile function compares all normalized data and generates discrepancies. The report function generates a PDF.

But the user mentioned that the final output is a PDF. So the generate_report op might write a file, but in the code, perhaps just mock it.

Also, the user mentioned resources or config. For example, maybe the CSV paths are configurable. But the problem says to provide simplified resource or config examples as comments or stubs. So perhaps add a config schema for the ops, but mock the actual implementation.

For example, the fetch ops could take a config parameter for the CSV path. But since the user wants minimal code, perhaps just hardcode the paths in comments.

Regarding retries and scheduling: The job should have a daily schedule, max 2 retries, 5-minute delay. But in the code, the job's configuration can include these. However, the code example should include the job definition with retries. The scheduling part would be in a separate ScheduleDefinition, but the user's code example may not include that unless specified. The problem says to include a minimal launch pattern, so maybe the schedule is not part of the code but the retries can be set in the job decorator.

So, the @job decorator can have config:

@job(
    config={
        "retries": {
            "enabled": {
                "max_retries": 2,
                "delay": 300  # 5 minutes in seconds
            }
        }
    }
)
def inventory_reconciliation_job():
    ...

But wait, in Dagster, retry policies are set via the RetryPolicy, not in the job config. So perhaps the correct way is to define a RetryPolicy and attach it to each op. But the problem states that all tasks are implemented with PythonOperator (mocked here as ops), and the orchestration requires max 2 retries with 5-minute delay. So perhaps each op should have a retry policy. But the user's code may not include that unless specified. The problem says "if you need resources/config, provide simplified examples as comments". So maybe add a comment indicating the retry policy.

Alternatively, since the user's code must be executable, perhaps omit the retry policies unless necessary. The problem's constraints mention including minimal config stubs. So perhaps add a comment in the job definition about retries.

But the main code should be executable. So perhaps the retries are not part of the code, but the user's example may include a schedule. However, the problem says to include a minimal launch pattern, which is the if __name__ block.

So putting it all together:

The code will have four fetch ops, four normalize ops, reconcile, generate_report. Each op is defined with @op, the job sets dependencies. The launch block runs the job in-process.

Testing for PEP 8 compliance: function names are lowercase_with_underscores, proper spacing, etc.

Now, writing the code:

Start with imports:

from dagster import op, job

Then define the fetch ops:

@op
def fetch_north():
    # In production: fetch CSV from north warehouse system
    return {"sku_data": "north_skus.csv"}

Similarly for south, east, west.

Then the normalize ops. Each takes the output of the corresponding fetch op.

@op
def normalize_north(context, data):
    # Apply transformation rules
    normalized = data["sku_data"].upper().strip().replace("WH-N-", "STD-")
    return {"normalized": normalized}

Same for the others, adjusting the prefix replacement (e.g., "WH-S-" to "STD-").

Then the reconcile op takes all four normalized data.

@op
def reconcile_inventory(context, north, south, east, west):
    # Compare all normalized datasets
    discrepancies = []
    # Mock comparison logic
    all_skus = set(north["normalized"]) | set(south["normalized"]) | set(east["normalized"]) | set(west["normalized"])
    # ... actual comparison would check quantities
    context.log.info(f"Found {len(discrepancies)} discrepancies")
    return {"discrepancies": discrepancies}

Wait, but the normalized data is strings in the mock. Maybe the actual data would be a list of SKUs with quantities. For the sake of the example, perhaps mock as lists.

But the code needs to be executable. So perhaps the normalized data is a list of SKUs, and the reconcile op checks counts.

But for simplicity, let's adjust the mock data. For example, the fetch ops return a list of SKUs and quantities, and the normalize ops process them into a standard format.

But to keep it simple, perhaps the normalized data is a dictionary with a key, and the reconcile op just counts them.

Alternatively, mock with simple data structures.

But the code must be executable. Let's adjust:

In fetch_north, return a list of SKUs. For example:

@op
def fetch_north():
    # Simulate fetching data - returns list of (SKU, quantity)
    return [("WH-N-123", 10), ("WH-N-456", 5)]

Similarly for other fetch ops, with different prefixes.

Normalize ops would convert the SKUs to standard format:

@op
def normalize_north(context, data):
    normalized = []
    for sku, qty in data:
        # Convert to standard format: remove WH-N- prefix, uppercase, strip whitespace
        std_sku = sku.replace("WH-N-", "STD-").upper().strip()
        normalized.append((std_sku, qty))
    return normalized

Same for others, replacing their respective prefixes.

Then, the reconcile_inventory op takes all four normalized lists and checks for discrepancies.

@op
def reconcile_inventory(context, north, south, east, west):
    from collections import defaultdict

    sku_counts = defaultdict(dict)
    # Populate counts from each warehouse
    for warehouse, data in [("north", north), ("south", south), ("east", east), ("west", west)]:
        for sku, qty in data:
            sku_counts[sku][warehouse] = qty

    discrepancies = []
    for sku, counts in sku_counts.items():
        if len(counts) < 4:
            # Missing in some warehouses
            discrepancies.append(sku)
        else:
            # Check if all quantities are the same
            quantities = list(counts.values())
            if not all(q == quantities[0] for q in quantities):
                discrepancies.append(sku)

    context.log.info(f"Found {len(discrepancies)} SKUs with discrepancies")
    return {"discrepancies": discrepancies}

Then, generate_report op creates a PDF. Mock this by logging.

@op
def generate_report(context, discrepancies):
    # In production: generate PDF report
    context.log.info(f"Generating report with {len(discrepancies['discrepancies'])} discrepancies")
    return "report.pdf"

Now, the job definition:

@job
def inventory_reconciliation_job():
    n = fetch_north()
    s = fetch_south()
    e = fetch_east()
    w = fetch_west()

    nn = normalize_north(n)
    ns = normalize_south(s)
    ne = normalize_east(e)
    nw = normalize_west(w)

    recon = reconcile_inventory(nn, ns, ne, nw)
    generate_report(recon)

Finally, the launch block:

if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()

This should be a complete, executable code. Testing for PEP 8: lines not too long, proper spacing, etc.

Now, check if all ops are connected correctly. Each fetch is followed by normalize, then all normalized data goes into reconcile, then generate_report.

Yes, this should work.
</think>

from dagster import op, job

@op
def fetch_north():
    # Simulate fetching CSV from north warehouse
    return [("WH-N-101", 15), ("WH-N-202", 20)]

@op
def fetch_south():
    # Simulate fetching CSV from south warehouse
    return [("WH-S-101", 10), ("WH-S-303", 8)]

@op
def fetch_east():
    # Simulate fetching CSV from east warehouse
    return [("WH-E-101", 12), ("WH-E-404", 7)]

@op
def fetch_west():
    # Simulate fetching CSV from west warehouse
    return [("WH-W-101", 18), ("WH-W-505", 6)]

@op
def normalize_north(_, data):
    # Convert north SKUs to standard format
    return [(sku.replace("WH-N-", "STD-").upper().strip(), qty) for sku, qty in data]

@op
def normalize_south(_, data):
    # Convert south SKUs to standard format
    return [(sku.replace("WH-S-", "STD-").upper().strip(), qty) for sku, qty in data]

@op
def normalize_east(_, data):
    # Convert east SKUs to standard format
    return [(sku.replace("WH-E-", "STD-").upper().strip(), qty) for sku, qty in data]

@op
def normalize_west(_, data):
    # Convert west SKUs to standard format
    return [(sku.replace("WH-W-", "STD-").upper().strip(), qty) for sku, qty in data]

@op
def reconcile_inventory(_, north, south, east, west):
    # Collect all SKUs and compare quantities
    all_skus = {}
    for sku, qty in north + south + east + west:
        if sku in all_skus:
            all_skus[sku].append(qty)
        else:
            all_skus[sku] = [qty]

    discrepancies = {
        sku: quantities
        for sku, quantities in all_skus.items()
        if len(set(quantities)) > 1 or len(quantities) < 4
    }
    return {"discrepancies": discrepancies, "count": len(discrepancies)}

@op
def generate_report(_, data):
    # Simulate PDF report generation
    print(f"Generated reconciliation report with {data['count']} discrepancies")
    return f"report_{len(data['discrepancies'])}_discrepancies.pdf"

@job
def inventory_reconciliation_job():
    n_data = fetch_north()
    s_data = fetch_south()
    e_data = fetch_east()
    w_data = fetch_west()

    n_norm = normalize_north(n_data)
    s_norm = normalize_south(s_data)
    e_norm = normalize_east(e_data)
    w_norm = normalize_west(w_data)

    recon_data = reconcile_inventory(n_norm, s_norm, e_norm, w_norm)
    generate_report(recon_data)

if __name__ == "__main__":
    inventory_reconciliation_job.execute_in_process()