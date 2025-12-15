<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let me start by understanding the steps involved. The pipeline has three steps: Load & Modify Data, Entity Reconciliation, and Save Final Data. Each step uses a different Docker image and communicates via API services on specific ports. 

First, I'll represent each step as a Dagster op. Each op will correspond to one of the pipeline steps. The dependencies are sequential: Load -> Reconciliation -> Save. So the job will set these ops in order.

The user mentioned using resources or config. Since each op requires parameters like DATASET_ID and others, I should create a configuration schema for each op. Using Dagster's Config classes makes sense here. Each op will have its own config with the necessary parameters.

For the input and output paths, since they all use DATA_DIR from the environment, I'll need to pass that as a resource. So I'll create a resource that provides the data directory path. The resource can be loaded from environment variables, maybe with a default.

Each op probably needs to call an external service. Since the actual implementation details aren't provided, I'll mock the service calls with placeholders, like print statements or simple file operations, just to make the code executable. But I'll structure the functions as if they're making API calls.

Error handling: The user mentioned retries. In Dagster, I can add retry policies to the job or individual ops. Since the default is 1 retry, I'll add a default_retry_policy to the job.

Docker and infrastructure details like volumes and networks are part of the deployment setup, not the Dagster code itself. So I don't need to handle that here. The code just needs to focus on the pipeline logic.

Now, structuring the code:

1. Import necessary modules: dagster's op, job, resource, RetryPolicy, etc.
2. Define the resource for DATA_DIR.
3. Create config classes for each op with their parameters.
4. Define each op with the required config and resources.
5. Link the ops in a job with dependencies.
6. Add retry policy to the job.
7. Include a main block to execute the job in process for testing.

Wait, the user mentioned using if __name__ == "__main__" to execute the job. So I'll add that at the end.

Let me draft each op. For example, the first op, load_and_modify_data, takes the config (DATASET_ID, TABLE_NAME_PREFIX) and the data_dir resource. It reads suppliers.csv, processes it, and writes to table_data_2.json. But since the actual service call is on port 3003, maybe the op would make an HTTP request. But without the service code, I'll simulate that by reading and writing files.

Similarly, the reconciliation op takes its parameters and processes the JSON file, adding Wikidata info. The save op converts the JSON to CSV.

I need to make sure the inputs and outputs are passed correctly between ops. The first op's output is table_data_2.json, which the second op uses as input. Then the second op's output is reconciled_table_2.json, which the third op uses.

But in Dagster, ops pass data via return values. So each op should return the output path or data for the next op. However, since the steps are file-based, maybe each op returns the filename, and the next op reads it. Alternatively, pass the data in memory. But given that the actual services might be reading/writing files, perhaps the ops just need to pass the filenames as strings.

So, the first op returns "table_data_2.json", the second takes that as input, processes it, returns "reconciled_table_2.json", and the third uses that.

Wait, but in the pipeline description, each step's output is a file in DATA_DIR. So the ops are generating files in a directory, and the next op reads from that directory. So maybe each op doesn't need to pass the data directly but relies on the filesystem. However, in Dagster, it's better to pass data between ops through parameters. So perhaps each op should return the data, and the next op processes it. But if the actual services are writing to files, maybe the data_dir is a shared volume, and the ops are just triggering the services which handle the file I/O. In that case, the Dagster ops might not handle the data directly but just execute the service calls, ensuring the files are in place.

This complicates things because the actual data passing isn't handled by Dagster. But for the purpose of creating an executable example, I can simulate the file operations within the ops.

So, for the first op, I'll read suppliers.csv from data_dir, process it (maybe convert to JSON), and write to table_data_2.json. Then return the path to the next op. The next op reads that JSON, adds Wikidata info, writes to reconciled_table_2.json, and passes that path to the save op, which converts it to CSV.

But how to simulate the API calls? Since the services are running in Docker, perhaps the ops are making HTTP requests to localhost:3003. But without knowing the exact API endpoints, I can't implement that. So, for the sake of creating a runnable example, I'll mock the API calls by performing file operations.

So, each op will read the input file, process it in a simple way (like adding a field), and write the output file. This way, the code is executable without external services.

Now, let's outline the code:

- Define the data_dir resource as a string.
- Config classes for each op with their parameters.
- Ops that take config and data_dir, read input files, process, write output.
- Job with the three ops in sequence.

Also, include retry policy. Dagster's RetryPolicy can be added to the job with max_retries=1.

Testing the code: when run, it should process the files in order. So in the main block, execute the job in process.

Potential issues: file paths. The data_dir resource should be used to construct the full path. For example, input_path = os.path.join(context.resources.data_dir, "suppliers.csv").

But the user might not have the suppliers.csv file, so the code might raise an error. To make it runnable, perhaps create dummy files or handle missing files by creating them on the fly.

Alternatively, in the code, when reading a file, if it doesn't exist, create a sample CSV for suppliers.csv. That way, the code can run without external files.

But the user might not want that. However, the problem states to avoid placeholders and make the code executable. So perhaps in the load_and_modify_data op, if the input file isn't found, create a sample DataFrame and write to the CSV, then process it. That way, the code can run end-to-end.

But the user's pipeline starts with suppliers.csv as input. So maybe in the example code, we can generate a sample suppliers.csv in the data_dir for testing purposes.

Alternatively, have the load op generate the sample data if the file isn't present. That would make the code self-contained.

So, in the first op's code:

Check if suppliers.csv exists in data_dir. If not, create a sample CSV with some supplier data. Then proceed to process it into JSON.

Similarly, the other ops can process the data accordingly.

This approach ensures the code runs without external dependencies.

Putting it all together:

- Import os and pandas for file handling and data processing.
- Use the data_dir resource to build file paths.
- Each op reads the input file, processes, writes output.
- Handle cases where input files might not exist by creating sample data.

Now, writing the code step by step.

First, the resources:

@resource(config_schema={"data_dir": str})
def data_dir_resource(init_context):
    return init_context.resource_config["data_dir"]

But maybe the data_dir is provided via environment variable. The user's note says DATA_DIR is via environment variable. So perhaps the resource is configured to read from an environment variable, with a default.

But in Dagster, resources can be configured in the configuration. Alternatively, set the data_dir in the resource config.

But for simplicity, perhaps the resource is a simple string. The user can configure it when running the job.

But in the example, to make it runnable, maybe set a default data_dir, like "./data".

Alternatively, in the resource, use os.environ.get("DATA_DIR", "./data").

But resources in Dagster are passed via configuration. So perhaps the resource is configured with a data_dir parameter that defaults to "./data" if not set.

Wait, perhaps the resource is defined as:

@resource(config_schema={"data_dir": Field(str, default_value="./data")})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

Then, when the job is executed, if the resource isn't provided, it uses "./data".

This way, the code can run without external configuration.

Now, the ops:

Each op will have a config with the necessary parameters. For example, the first op's config includes dataset_id and table_name_prefix.

Using Dagster's Config class (from dagster import Config) would be better for type hints and structured config.

Wait, in Dagster, you can define structured config using Python classes with dagster.Config. So:

class LoadModifyConfig(Config):
    dataset_id: int = 2
    table_name_prefix: str = "JOT_"

Then, in the op, access the config via context.op_config.

Similarly for the other ops.

So, each op's config is a structured config class.

Now, writing the first op:

@op
def load_and_modify_data(context, config: LoadModifyConfig, data_dir: str):
    input_path = os.path.join(data_dir, "suppliers.csv")
    output_path = os.path.join(data_dir, f"table_data_{config.dataset_id}.json")

    # Simulate API call to load-and-modify service
    context.log.info(f"Loading and modifying data from {input_path}")
    
    # If input file doesn't exist, create sample data
    if not os.path.exists(input_path):
        sample_data = pd.DataFrame({
            "supplier_name": ["Supplier A", "Supplier B"],
            "location": ["Location A", "Location B"],
            "contact_info": ["contact@a.com", "contact@b.com"]
        })
        sample_data.to_csv(input_path, index=False)
    
    # Read CSV and convert to JSON
    df = pd.read_csv(input_path)
    df.to_json(output_path, orient="records")
    
    context.log.info(f"Saved modified data to {output_path}")
    return output_path

Wait, but the op is supposed to return the output path so the next op can use it. So the next op takes this path as input.

Similarly, the reconciliation op:

@op
def entity_reconciliation(context, config: ReconciliationConfig, data_dir: str, input_path: str):
    output_path = os.path.join(data_dir, f"reconciled_table_{config.dataset_id}.json")
    
    # Simulate API call to reconciliation service
    context.log.info(f"Reconciling entities in {input_path}")
    
    # Read input JSON
    df = pd.read_json(input_path)
    
    # Add Wikidata ID/link (simulated)
    df["wikidata_id"] = ["Q123", "Q456"]
    df["wikidata_link"] = [f"https://www.wikidata.org/wiki/Q123", f"https://www.wikidata.org/wiki/Q456"]
    
    df.to_json(output_path, orient="records")
    context.log.info(f"Saved reconciled data to {output_path}")
    return output_path

And the save op:

@op
def save_final_data(context, config: SaveConfig, data_dir: str, input_path: str):
    output_path = os.path.join(data_dir, f"enriched_data_{config.dataset_id}.csv")
    
    # Simulate API call to save service
    context.log.info(f"Saving final data from {input_path}")
    
    # Read input JSON and save as CSV
    df = pd.read_json(input_path)
    df.to_csv(output_path, index=False)
    
    context.log.info(f"Saved final CSV to {output_path}")
    return output_path

Then, the job ties them together:

@job(
    resource_defs={"data_dir": data_dir_resource},
    config={
        "ops": {
            "load_and_modify_data": {"config": {"dataset_id": 2, "table_name_prefix": "JOT_"}},
            "entity_reconciliation": {"config": {"dataset_id": 2, "primary_column": "supplier_name", "reconciliator_id": "wikidataEntity"}},
            "save_final_data": {"config": {"dataset_id": 2}}
        }
    },
    tags={"retry_policy": RetryPolicy(max_retries=1)}
)
def procurement_supplier_validation_job():
    loaded_data = load_and_modify_data()
    reconciled_data = entity_reconciliation(loaded_data)
    save_final_data(reconciled_data)

Finally, the main block:

if __name__ == "__main__":
    result = procurement_supplier_validation_job.execute_in_process(
        run_config={
            "resources": {
                "data_dir": {"config": {"data_dir": "./data"}}
            }
        }
    )

This way, when run, the job creates the data directory if it doesn't exist, generates sample CSV if needed, processes through the steps, and produces the output files.

I need to make sure all imports are included: import os, pandas as pd, and the necessary Dagster modules.

Putting it all together, the code should be PEP 8 compliant, with proper indentation, docstrings where helpful, and executable.
</think>

import os
import pandas as pd
from dagster import op, job, resource, RetryPolicy

# --- Resources ---
@resource(config_schema={"data_dir": str})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

# --- Configurations ---
class LoadModifyConfig:
    def __init__(self, dataset_id=2, table_name_prefix="JOT_"):
        self.dataset_id = dataset_id
        self.table_name_prefix = table_name_prefix

class ReconciliationConfig:
    def __init__(self, dataset_id=2, primary_column="supplier_name", reconciliator_id="wikidataEntity"):
        self.dataset_id = dataset_id
        self.primary_column = primary_column
        self.reconciliator_id = reconciliator_id

class SaveConfig:
    def __init__(self, dataset_id=2):
        self.dataset_id = dataset_id

# --- Operations ---
@op
def load_and_modify_data(context, data_dir):
    config = LoadModifyConfig()
    input_path = os.path.join(data_dir, "suppliers.csv")
    output_path = os.path.join(data_dir, f"table_data_{config.dataset_id}.json")

    # Simulate API call to load-and-modify service
    context.log.info(f"Loading from {input_path}")
    
    if not os.path.exists(input_path):
        pd.DataFrame({
            "supplier_name": ["Supplier A", "Supplier B"],
            "location": ["Country X", "Country Y"],
            "contact_info": ["contact@a.com", "contact@b.com"]
        }).to_csv(input_path, index=False)

    df = pd.read_csv(input_path)
    df.to_json(output_path, orient="records")
    return output_path

@op
def entity_reconciliation(context, data_dir, input_path):
    config = ReconciliationConfig()
    output_path = os.path.join(data_dir, f"reconciled_table_{config.dataset_id}.json")

    # Simulate Wikidata reconciliation
    context.log.info(f"Reconciling {input_path}")
    df = pd.read_json(input_path)
    
    # Mock reconciliation results
    df["wikidata_id"] = ["Q1234", "Q5678"]
    df["wikidata_link"] = [
        f"https://www.wikidata.org/wiki/Q1234",
        f"https://www.wikidata.org/wiki/Q5678"
    ]
    
    df.to_json(output_path, orient="records")
    return output_path

@op
def save_final_data(context, data_dir, input_path):
    config = SaveConfig()
    output_path = os.path.join(data_dir, f"enriched_data_{config.dataset_id}.csv")

    # Simulate save service call
    context.log.info(f"Saving to {output_path}")
    pd.read_json(input_path).to_csv(output_path, index=False)
    return output_path

# --- Job Definition ---
@job(
    resource_defs={"data_dir": data_dir_resource},
    tags={"retry_policy": RetryPolicy(max_retries=1)}
)
def procurement_supplier_validation_job():
    processed_data = load_and_modify_data()
    reconciled_data = entity_reconciliation(processed_data)
    save_final_data(reconciled_data)

# --- Execution ---
if __name__ == "__main__":
    result = procurement_supplier_validation_job.execute_in_process(
        run_config={
            "resources": {
                "data_dir": {"config": {"data_dir": "data"}}
            }
        }
    )