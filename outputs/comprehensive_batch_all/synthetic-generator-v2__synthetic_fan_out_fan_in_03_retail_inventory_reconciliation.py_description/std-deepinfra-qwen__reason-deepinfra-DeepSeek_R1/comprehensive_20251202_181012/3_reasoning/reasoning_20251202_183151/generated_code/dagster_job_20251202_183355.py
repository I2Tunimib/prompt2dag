<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has four main steps: fetching CSV files from four warehouses, normalizing SKU formats in parallel, reconciling discrepancies, and generating a final report. 

First, Dagster uses ops and jobs. Each step in the pipeline should be an op. The fetch step is four parallel ops, one for each warehouse. Then normalization is also four parallel ops. These need to depend on their respective fetch ops. Then the reconcile step depends on all normalization ops, and the final report depends on the reconcile step.

So, I'll start by defining the fetch ops. Since there are four warehouses, I can create a function that generates these ops. Maybe using a loop for north, south, east, west. Each fetch op will return a file path or data. But the description says they fetch CSV files, so maybe each returns a DataFrame or a file path. For simplicity, perhaps they return a dictionary with some mock data.

Next, the normalization ops. Each normalization op takes the output from its corresponding fetch op. So north_fetch goes to north_normalize, etc. The normalization steps involve converting SKU formats: uppercase, strip whitespace, standardize prefixes. So in the normalize op, I'll process the data accordingly. Since the actual data processing isn't specified, I'll mock it by taking the input data and applying the transformations.

Then the reconcile op needs all four normalized datasets. So this op will have inputs from all four normalization ops. The reconcile step compares SKU counts, identifies discrepancies, and generates a report. The output could be a discrepancy report, maybe a dictionary with discrepancies found.

Finally, the generate_report op takes the discrepancy report and creates a PDF. Again, since it's a mock, maybe just print a message or create a dummy file.

Now, structuring the job. The dependencies should be: fetch ops run in parallel, each normalize op depends on its fetch, then reconcile depends on all normalize ops, and report depends on reconcile.

In Dagster, each op's inputs are specified as parameters, and the job defines the dependencies. So for the fetch ops, since they're separate, each will be an individual op. Then the normalization ops each take one fetch output. The reconcile op takes all four normalized outputs.

I need to create the ops using @op decorator. For the fetch ops, maybe a function that creates them dynamically using a loop. Wait, but in Dagster, each op needs to be defined explicitly. Alternatively, I can write a function that returns the four fetch ops. Or perhaps use a factory pattern. But maybe it's easier to write four separate fetch ops, but that's repetitive. Alternatively, create a helper function to generate them.

Wait, the user mentioned "Use Dagster ops (@op) to represent each step". So each step in the pipeline is an op. The fetch step has four parallel tasks, so four ops. Similarly for normalization. So perhaps I'll write four fetch ops: fetch_north, fetch_south, etc. Similarly for normalization.

But that's a lot of repeated code. To avoid repetition, maybe create a function that generates these ops using a configurable parameter. But in Dagster, each op must be a unique function. So perhaps using a loop to create the ops dynamically. Hmm, but in Python, functions created in a loop might not have unique names. Alternatively, use a decorator with parameters. Wait, Dagster allows using configurable ops with configured parameters. Alternatively, use a single op that takes a warehouse parameter, but the problem is that each fetch is a separate parallel task. So maybe using a single op that's called four times with different config. But in Dagster, each op instance in the job is a separate invocation. Wait, no. Each op is a node in the job graph. So if you have four different ops (fetch_north, fetch_south, etc.), each can be run in parallel.

Alternatively, create a generic fetch_warehouse op that takes a warehouse name as a config parameter. Then, in the job, have four instances of this op with different configs. But Dagster doesn't allow multiple instances of the same op in a job unless they're aliased. So perhaps using a configurable op with multiple invocations. But how to represent that in code?

Wait, maybe the best approach is to create a single fetch op that handles all warehouses, but that would not be parallel. The user's pipeline requires parallel execution for fetch tasks. So each fetch must be a separate op. Therefore, I need to define four separate fetch ops, each for a warehouse. Similarly for normalization.

But writing four similar functions is tedious. To avoid code duplication, perhaps use a loop to generate the ops. For example:

warehouses = ['north', 'south', 'east', 'west']

for warehouse in warehouses:
    @op(name=f"fetch_{warehouse}")
    def fetch_warehouse_op(context):
        # fetch logic for the warehouse
        return {"data": f"mock_{warehouse}_data"}

But in Python, this would create functions in a loop, which might not work as expected because the variable 'warehouse' is late-binding. All the functions would end up using the last value of 'warehouse'. To fix this, we can capture the current value of 'warehouse' in a default argument.

So:

warehouses = ['north', 'south', 'east', 'west']

fetch_ops = []
normalize_ops = []

for wh in warehouses:
    @op(name=f"fetch_{wh}")
    def fetch_op(context, warehouse=wh):
        context.log.info(f"Fetching {warehouse} data")
        return {"raw_data": f"mock_{warehouse}_data"}
    fetch_ops.append(fetch_op)

    @op(name=f"normalize_{wh}")
    def normalize_op(context, raw_data, warehouse=wh):
        context.log.info(f"Normalizing {warehouse} SKUs")
        # Apply transformations
        normalized = raw_data['raw_data'].upper().strip().replace(" ", "")
        # Example prefix standardization: add 'WH_' prefix
        standardized = f"WH_{normalized}"
        return {"normalized_data": standardized}
    normalize_ops.append(normalize_op)

But wait, each iteration of the loop defines a new function. However, in Python, defining functions in a loop like this can lead to all functions referencing the same variable 'wh', which would have the last value after the loop. To avoid this, we can bind the current value of 'wh' in each iteration by using a default parameter, which is evaluated at function definition time. So by setting warehouse=wh in the function definition, each function captures the current value of 'wh' in that iteration.

This should work. Then, the fetch_ops list contains the four fetch ops, and normalize_ops contains the four normalize ops.

Then, the reconcile op depends on all four normalize ops. So in the job definition, after creating all normalize ops, the reconcile op takes their outputs as inputs.

The reconcile op would collect all four normalized data and process them. Similarly, the generate_report op depends on the reconcile op.

Putting this together, the job would look like:

@job
def inventory_reconciliation_job():
    normalized_data = []
    for fetch_op, normalize_op in zip(fetch_ops, normalize_ops):
        raw_data = fetch_op()
        norm_data = normalize_op(raw_data)
        normalized_data.append(norm_data)
    
    recon_result = reconcile_inventory_discrepancies(normalized_data)
    generate_final_report(recon_result)

Wait, but in Dagster, each op's output is passed to the next op. So for each pair of fetch and normalize ops, the normalize op depends on the fetch op. Then, the reconcile op depends on all four normalize ops. So in the job function, we need to collect all the outputs from the normalize ops and pass them to the reconcile op.

But in the code above, when we loop through fetch_ops and normalize_ops, each normalize_op is called with the corresponding fetch_op's output. Then, collect all the normalize_op outputs into a list and pass them to the reconcile op.

But in Dagster, each op's output is a SolidOutputHandle, and when you call an op in the job function, you get the output. So perhaps:

@job
def inventory_reconciliation_job():
    north_raw = fetch_north()
    north_norm = normalize_north(north_raw)
    
    south_raw = fetch_south()
    south_norm = normalize_south(south_raw)
    
    # and so on for east and west
    
    recon_inputs = [north_norm, south_norm, east_norm, west_norm]
    recon_result = reconcile_inventory_discrepancies(recon_inputs)
    generate_final_report(recon_result)

But since we generated the ops dynamically, perhaps using a list comprehension to collect the normalized outputs.

Alternatively, in the job definition, loop through the warehouses and collect the normalized data.

Wait, perhaps:

@job
def inventory_reconciliation_job():
    normalized_outputs = []
    for wh in warehouses:
        fetch_op = globals()[f"fetch_{wh}"]
        normalize_op = globals()[f"normalize_{wh}"]
        raw_data = fetch_op()
        normalized_data = normalize_op(raw_data)
        normalized_outputs.append(normalized_data)
    
    recon_result = reconcile_inventory_discrepancies(normalized_outputs)
    generate_final_report(recon_result)

But this approach requires that the ops are in the global scope, which might not be the case if they are created dynamically in a loop. Alternatively, collect the normalized outputs by iterating over the ops created earlier.

But perhaps a better way is to structure the code with the dynamically created ops and then in the job, connect them accordingly.

Alternatively, since the user's example uses a loop to create the ops, perhaps the job can be structured as follows:

After creating the fetch_ops and normalize_ops lists, in the job function, for each pair, execute fetch_op then normalize_op, collect all normalized outputs, then pass to reconcile.

But in Dagster, each op is a node in the graph. So for each warehouse, fetch_op -> normalize_op, then all normalize_ops -> reconcile_op -> generate_report_op.

So in code:

@job
def inventory_reconciliation_job():
    # Collect normalized data from all warehouses
    normalized_data = []
    for fetch_op, normalize_op in zip(fetch_ops, normalize_ops):
        raw = fetch_op()
        norm = normalize_op(raw)
        normalized_data.append(norm)
    
    # Pass all normalized data to reconcile
    recon = reconcile_inventory_discrepancies(normalized_data)
    generate_final_report(recon)

But in Dagster, when you call an op in the job function, it's added to the graph. So for each fetch_op and normalize_op in the loop, their execution is added to the graph. Then, the reconcile op depends on all the normalize ops' outputs.

Wait, but in the code above, normalized_data is a list of the outputs from each normalize_op. Then, when reconcile_inventory_discrepancies is called with normalized_data, it's expecting a list of inputs. But in Dagster, each input to an op must be a single SolidOutputHandle. So if the reconcile op is expecting a list of four inputs, how is that handled?

Ah, right. The reconcile op's function needs to accept four inputs, one from each normalize op. So perhaps the reconcile op should have four parameters, each corresponding to a normalized data output.

Wait, the pipeline description says that the reconcile step processes all four normalized files. So the reconcile op needs to take all four normalized datasets as inputs.

So the reconcile_inventory_discrepancies op should have four inputs: north_norm, south_norm, east_norm, west_norm.

But in the current setup, the normalized_data list contains the four outputs. So how to pass them as separate arguments?

Alternatively, the reconcile op can accept a list of normalized data. But in Dagster, each input must be mapped to an upstream op's output. So if the reconcile op is designed to take a list, perhaps using a fan-in approach where each element of the list is an input from a different op.

Wait, perhaps the reconcile op has four inputs, each from a different normalize op. Then, in the job, those four inputs are connected to the four normalize ops.

So, the reconcile op would be defined as:

@op
def reconcile_inventory_discrepancies(context, north_norm, south_norm, east_norm, west_norm):
    # process all four normalized data
    all_data = [north_norm, south_norm, east_norm, west_norm]
    # ... reconciliation logic ...
    discrepancies = len(all_data)  # mock logic
    return {"discrepancies": discrepancies}

Then, in the job, after getting all four normalized data, pass each to the reconcile op's parameters.

But in the dynamically created ops, how to collect the four normalized outputs and pass them to reconcile_inventory_discrepancies?

In the job function:

north_norm = normalize_north(fetch_north())
south_norm = normalize_south(fetch_south())
east_norm = normalize_east(fetch_east())
west_norm = normalize_west(fetch_west())

recon_result = reconcile_inventory_discrepancies(north_norm, south_norm, east_norm, west_norm)

But if the ops are created dynamically in a loop, how to reference them? Maybe the initial approach of creating separate ops for each warehouse is better, even if it's repetitive.

Alternatively, perhaps the code should explicitly define each fetch and normalize op for the four warehouses, then the reconcile op takes all four as inputs.

But that would require writing a lot of repetitive code. For example:

@op
def fetch_north(context):
    # fetch data
    return data

@op
def fetch_south(context):
    # fetch data
    return data

... and so on for east and west.

Similarly for normalize ops.

Then, the reconcile op would have four inputs:

@op
def reconcile_inventory_discrepancies(context, north_norm, south_norm, east_norm, west_norm):
    # process all four

But this approach leads to a lot of code duplication. However, given the constraints of the problem, maybe this is acceptable for the sake of clarity and simplicity, especially since the user wants explicit dependencies.

Alternatively, use a config parameter in the fetch and normalize ops to specify the warehouse, allowing a single op to handle all four warehouses with different configs. But then, how to run them in parallel with different configs?

Wait, in Dagster, you can use a config schema for ops. So perhaps:

from dagster import op, job, Field

@op(config_schema={"warehouse": str})
def fetch_warehouse(context):
    warehouse = context.op_config["warehouse"]
    # fetch data for the specified warehouse
    return {"raw_data": f"mock_{warehouse}_data"}

Similarly for normalize_warehouse:

@op(config_schema={"warehouse": str})
def normalize_warehouse(context, raw_data):
    warehouse = context.op_config["warehouse"]
    # normalize data
    return {"normalized_data": ...}

But then, in the job, how to create four instances of fetch_warehouse with different configs? Because in Dagster, each op in the job graph is a separate node. So you can use the same op multiple times with different configs by using .configured().

For example:

fetch_north = fetch_warehouse.configured({"warehouse": "north"}, name="fetch_north")
fetch_south = fetch_warehouse.configured({"warehouse": "south"}, name="fetch_south")
# etc.

Similarly for normalize ops.

Then, the job would look like:

@job
def inventory_reconciliation_job():
    north_raw = fetch_north()
    south_raw = fetch_south()
    east_raw = fetch_east()
    west_raw = fetch_west()

    north_norm = normalize_north(north_raw)
    south_norm = normalize_south(south_raw)
    east_norm = normalize_east(east_raw)
    west_norm = normalize_west(west_raw)

    recon_result = reconcile_inventory_discrepancies(
        north_norm, south_norm, east_norm, west_norm
    )
    generate_final_report(recon_result)

This approach reduces code duplication by reusing the same op with different configurations. This seems efficient and follows best practices.

So, putting it all together:

Define a fetch_warehouse op with config for the warehouse name. Similarly for normalize_warehouse. Then configure them for each warehouse. Then define the reconcile and report ops.

Now, let's draft the code.

First, the fetch_warehouse op:

@op(config_schema={"warehouse": str})
def fetch_warehouse(context):
    warehouse = context.op_config["warehouse"]
    context.log.info(f"Fetching {warehouse} warehouse data")
    # Mock data: in real scenario, fetch CSV
    return {"raw_data": f"mock_{warehouse}_data.csv"}

Then, configure four instances:

fetch_north = fetch_warehouse.configured({"warehouse": "north"}, name="fetch_north")
fetch_south = fetch_warehouse.configured({"warehouse": "south"}, name="fetch_south")
fetch_east = fetch_warehouse.configured({"warehouse": "east"}, name="fetch_east")
fetch_west = fetch_warehouse.configured({"warehouse": "west"}, name="fetch_west")

Similarly for normalize_warehouse:

@op(config_schema={"warehouse": str})
def normalize_warehouse(context, raw_data):
    warehouse = context.op_config["warehouse"]
    context.log.info(f"Normalizing SKUs for {warehouse}")
    # Mock transformations
    skus = raw_data["raw_data"]
    standardized = skus.upper().strip().replace(" ", "")
    # Add warehouse prefix (example standardization)
    standardized = f"{warehouse[:2].upper()}_{standardized}"
    return {"normalized_data": standardized}

normalize_north = normalize_warehouse.configured({"warehouse": "north"}, name="normalize_north")
normalize_south = normalize_warehouse.configured({"warehouse": "south"}, name="normalize_south")
normalize_east = normalize_warehouse.configured({"warehouse": "east"}, name="normalize_east")
normalize_west = normalize_warehouse.configured({"warehouse": "west"}, name="normalize_west")

Then, the reconcile op:

@op
def reconcile_inventory_discrepancies(context, north_norm, south_norm, east_norm, west_norm):
    context.log.info("Reconciling inventory discrepancies")
    # Mock comparison logic
    all_data = [north_norm, south_norm, east_norm, west_norm]
    discrepancies = []
    # Example: check if all normalized data have the same SKU count (mock)
    sku_counts = {d["normalized_data"]: len(d["normalized_data"]) for d in all_data}
    # For mock, assume discrepancies found
    discrepancy_report = {
        "discrepancies": 23,
        "details": "23 SKUs with quantity mismatches"
    }
    return discrepancy_report

Then, the generate report op:

@op
def generate_final_report(context, discrepancy_report):
    context.log.info("Generating final reconciliation report")
    # Mock PDF generation
    report_path = f"/reports/reconciliation_{context.run_id}.pdf"
    # For demo, return mock path
    return {"report_path": report_path, **discrepancy_report}

Finally, the job:

@job
def inventory_reconciliation_job():
    north_raw = fetch_north()
    south_raw = fetch_south()
    east_raw = fetch_east()
    west_raw = fetch_west()

    north_norm = normalize_north(north_raw)
    south_norm = normalize_south(south_raw)
    east_norm = normalize_east(east_raw)
    west_norm = normalize_west(west_raw)

    recon_result = reconcile_inventory_discrepancies(
        north_norm["normalized_data"],
        south_norm["normalized_data"],
        east_norm["normalized_data"],
        west_norm["normalized_data"]
    )
    generate_final_report(recon_result)

Wait, but the inputs to reconcile_inventory_discrepancies are the outputs from the normalize ops. Each normalize op returns a dictionary with "normalized_data". So when passing north_norm to reconcile_inventory_discrepancies, we need to extract the "normalized_data" value.

But in Dagster, when an op returns a dictionary, the downstream op can access the keys as parameters. Alternatively, the downstream op can take the entire dictionary and access the key.

Wait, no. In Dagster, each op's output is a single value. If an op returns a dictionary, the downstream op can either take the entire dictionary as input or use a specific key via .get_output("key") when wiring dependencies.

Alternatively, when defining the reconcile op's inputs, each input is mapped to a specific output from the upstream op.

Wait, perhaps I need to adjust the code. Let's see:

Each normalize op returns a dictionary {"normalized_data": ...}. So, when connecting to the reconcile op, each input (like north_norm) is the entire output of the normalize op. Therefore, the reconcile op's parameters should expect dictionaries and extract the "normalized_data" key.

Alternatively, the reconcile op can take each normalized data as a parameter, expecting the "normalized_data" value.

Wait, in Dagster, if an op returns a dictionary, the downstream op can specify an input with the same name as the key. For example:

If op1 returns {"key": "value"}, then op2 can have an input named "key" which will receive "value".

But in this case, the normalize ops return {"normalized_data": ...}, so the reconcile op's inputs should be named normalized_data_north, normalized_data_south, etc., and each mapped to the "normalized_data" output of the respective normalize op.

Alternatively, the reconcile op can take four inputs, each of which is the "normalized_data" output from each normalize op.

So, modifying the reconcile op:

@op
def reconcile_inventory_discrepancies(
    context,
    north_normalized: str,
    south_normalized: str,
    east_normalized: str,
    west_normalized: str
):
    # process all four normalized strings
    all_skus = [north_normalized, south_normalized, east_normalized, west_normalized]
    # mock discrepancy detection
    discrepancies = 23
    return {"discrepancies": discrepancies, "details": "23 SKUs with mismatches"}

Then, in the job, when connecting the normalize ops to the reconcile op, we need to map the "normalized_data" output of each normalize op to the respective input of the reconcile op.

So, in the job definition:

north_norm = normalize_north(north_raw)
south_norm = normalize_south(south_raw)
east_norm = normalize_east(east_raw)
west_norm = normalize_west(west_raw)

recon_result = reconcile_inventory_discrepancies(
    north_normalized=north_norm["normalized_data"],
    south_normalized=south_norm["normalized_data"],
    east_normalized=east_norm["normalized_data"],
    west_normalized=west_norm["normalized_data"]
)

But in Dagster, when you call an op in the job function, you can specify which output to use if the op has multiple outputs. However, in this case, each normalize op returns a single output (a dictionary), but the key is "normalized_data". So, to pass the value of "normalized_data" to the reconcile op's input, we need to reference it correctly.

Wait, perhaps the normalize ops should return just the normalized data string instead of a dictionary. That would simplify things. Let me adjust:

Modify the normalize_warehouse op to return the string directly:

@op(config_schema={"warehouse": str})
def normalize_warehouse(context, raw_data):
    warehouse = context.op_config["warehouse"]
    context.log.info(f"Normalizing SKUs for {warehouse}")
    # Mock transformations
    skus = raw_data["raw_data"]
    standardized = skus.upper().strip().replace(" ", "")
    # Add warehouse prefix (example standardization)
    standardized = f"{warehouse[:2].upper()}_{standardized}"
    return standardized  # instead of a dict

Then, the reconcile op can take four string inputs:

@op
def reconcile_inventory_discrepancies(
    context,
    north_normalized: str,
    south_normalized: str,
    east_normalized: str,
    west_normalized: str
):
    # process all four normalized strings
    all_skus = [north_normalized, south_normalized, east_normalized, west_normalized]
    # mock discrepancy detection
    discrepancies = 23
    return {"discrepancies": discrepancies, "details": "23 SKUs with mismatches"}

Then, in the job:

north_raw = fetch_north()
south_raw = fetch_south()
east_raw = fetch_east()
west_raw = fetch_west()

north_norm = normalize_north(north_raw)
south_norm = normalize_south(south_raw)
east_norm = normalize_east(east_raw)
west_norm = normalize_west(west_raw)

recon_result = reconcile_inventory_discrepancies(
    north_normalized=north_norm,
    south_normalized=south_norm,
    east_normalized=east_norm,
    west_normalized=west_norm
)

generate_final_report(recon_result)

This simplifies the code because each normalize op returns a single string output, which is directly passed to the reconcile op's parameters.

Now, the fetch_warehouse op returns a dictionary {"raw_data": ...}, so the normalize_warehouse op's input is that dictionary. Wait, no. The normalize_warehouse op's input is the output of fetch_warehouse. If fetch_warehouse returns a dictionary, then the normalize_warehouse op's input parameter should expect that dictionary.

Wait, no. If fetch_warehouse returns a dictionary, then the normalize_warehouse op's input parameter should be named raw_data and expect the entire dictionary. But in the code above, the normalize_warehouse op's input is raw_data, which is the output of fetch_warehouse. So, for example, fetch_north returns {"raw_data": "mock_north_data.csv"}, which is passed to normalize_north's raw_data parameter. Then, normalize_north processes raw_data["raw_data"].

Yes, that makes sense. So the normalize_warehouse op's input is the entire output of fetch_warehouse, which is a dictionary. Then, inside normalize_warehouse, we extract raw_data["raw_data"].

So, adjusting the code:

@op(config_schema={"warehouse": str})
def normalize_warehouse(context, raw_data):
    warehouse = context.op_config["warehouse"]
    context.log.info(f"Normalizing SKUs for {warehouse}")
    # Mock transformations
    skus = raw_data["raw_data"]  # raw_data is the output from fetch_warehouse
    standardized = skus.upper().strip().replace(" ", "")
    # Add warehouse prefix (example standardization)
    standardized = f"{warehouse[:2].upper()}_{standardized}"
    return standardized

This way, the normalize_warehouse op takes the output from fetch_warehouse (a dict), processes it, and returns the standardized string.

Then, the reconcile_inventory_discrepancies op takes four string inputs, each from a normalize_warehouse op.

This should work.

Putting all together:

The code would have:

- Four fetch ops configured from fetch_warehouse with different warehouse names.
- Four normalize ops configured from normalize_warehouse with different warehouse names.
- The reconcile op that takes four inputs.
- The generate report op that takes the reconcile output.

Now, handling the launch pattern:

The user wants a minimal launch pattern, so adding:

if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()

Now, considering resources and config:

The user mentioned that if resources/config are needed, provide simplified examples as comments or stubs. The current code uses op config for warehouse names, which is handled via configured(). No other resources are mentioned, so perhaps no need for additional resource definitions.

Testing for PEP 8 compliance: function names are lowercase with underscores, etc.

Now, putting all the code together.
</think>

from dagster import op, job, Field

# Fetch Operations
@op(config_schema={"warehouse": str})
def fetch_warehouse(context):
    warehouse = context.op_config["warehouse"]
    context.log.info(f"Fetching {warehouse} warehouse data")
    return {"raw_data": f"mock_{warehouse}_data.csv"}

fetch_north = fetch_warehouse.configured({"warehouse": "north"}, name="fetch_north")
fetch_south = fetch_warehouse.configured({"warehouse": "south"}, name="fetch_south")
fetch_east = fetch_warehouse.configured({"warehouse": "east"}, name="fetch_east")
fetch_west = fetch_warehouse.configured({"warehouse": "west"}, name="fetch_west")

# Normalization Operations
@op(config_schema={"warehouse": str})
def normalize_warehouse(context, raw_data):
    warehouse = context.op_config["warehouse"]
    context.log.info(f"Normalizing SKUs for {warehouse}")
    sku = raw_data["raw_data"].split("_")[1]  # Extract mock SKU
    standardized = sku.upper().strip().replace(" ", "")
    standardized = f"{warehouse[:2].upper()}_{standardized}"
    return standardized

normalize_north = normalize_warehouse.configured({"warehouse": "north"}, name="normalize_north")
normalize_south = normalize_warehouse.configured({"warehouse": "south"}, name="normalize_south")
normalize_east = normalize_warehouse.configured({"warehouse": "east"}, name="normalize_east")
normalize_west = normalize_warehouse.configured({"warehouse": "west"}, name="normalize_west")

# Reconciliation Operations
@op
def reconcile_inventory_discrepancies(
    context,
    north_normalized: str,
    south_normalized: str,
    east_normalized: str,
    west_normalized: str
):
    context.log.info("Reconciling inventory discrepancies")
    all_skus = {north_normalized, south_normalized, east_normalized, west_normalized}
    discrepancy_count = 23  # Mock detection logic
    return {
        "discrepancy_count": discrepancy_count,
        "report_data": f"Found {discrepancy_count} SKU mismatches across warehouses"
    }

# Report Generation
@op
def generate_final_report(context, reconciliation_result):
    context.log.info("Generating final reconciliation report")
    return {
        "status": "success",
        "report_path": f"/reports/recon_{context.run_id}.pdf",
        "details": reconciliation_result["report_data"]
    }

# Job Definition
@job
def inventory_reconciliation_job():
    north_data = fetch_north()
    south_data = fetch_south()
    east_data = fetch_east()
    west_data = fetch_west()

    north_norm = normalize_north(north_data)
    south_norm = normalize_south(south_data)
    east_norm = normalize_east(east_data)
    west_norm = normalize_west(west_data)

    recon_result = reconcile_inventory_discrepancies(
        north_norm, south_norm, east_norm, west_norm
    )
    generate_final_report(recon_result)

# Execution
if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()