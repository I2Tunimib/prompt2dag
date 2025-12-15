<think>
Okay, I need to convert this medical facility accessibility pipeline into a Dagster job. Let's start by understanding the steps involved. The pipeline has five steps: Load & Modify Data, Reconciliation (Geocoding), Distance Calculation for Public Transport, Distance Calculation for Residential Areas, and Save Final Data. Each step uses a different Docker image and has specific parameters.

First, I'll represent each step as a Dagster op. Each op will correspond to one of the pipeline steps. The dependencies are sequential except maybe the two distance calculations, but looking at the inputs, the Residential Areas step depends on the Public Transport step's output. So it's a linear flow: Load -> Geocode -> PT Distance -> Residential Distance -> Save.

Each op needs to call some service, probably via a CLI or API. Since the original setup uses Docker images, maybe the ops are shell commands that run those containers. But since Dagster ops are Python functions, perhaps I'll simulate the service calls with placeholders, or use the Dagster's shell_command_op if necessary. However, the user wants explicit dependencies and executable code, so maybe using subprocess calls within the ops.

Wait, but the user's constraints mention using @op and @job, and if resources/config are needed, provide simplified examples. Also, the code must be executable. So perhaps each op is a Python function that, for example, reads the input file, processes it, and writes the output. But given that the original steps involve external services, maybe the actual processing is done via those services, but for the sake of a minimal example, the code can simulate the steps.

Alternatively, maybe the ops are stubs that just log the parameters and simulate the data flow. The key is to model the dependencies correctly.

Let me outline the ops:

1. load_and_modify_data: Takes facilities.csv, processes it, outputs table_data_2.json.
2. reconciliation_geocoding: Takes table_data_2.json, adds lat/lon, outputs reconciled_table_2.json.
3. calculate_pt_distance: Takes reconciled_table_2.json, adds distance_to_pt, outputs distance_pt_2.json.
4. calculate_residential_distance: Takes distance_pt_2.json, adds distance_to_residential, outputs column_extended_2.json.
5. save_final_data: Takes column_extended_2.json, outputs enriched_data_2.csv.

Each op depends on the previous one's output. So the job will chain them in order.

Now, considering parameters: Each step has key parameters like DATASET_ID, API_TOKEN, etc. These can be passed via config. So each op will have a config schema that includes these parameters. For example, the reconciliation_geocoding op needs API_TOKEN, PRIMARY_COLUMN, etc.

But since the user wants minimal resources/config examples, perhaps I'll define the config for each op and use them in the functions. Also, the DATA_DIR is an environment variable, so maybe we can use a resource to handle that.

Wait, the infrastructure mentions a shared volume mounted via DATA_DIR. So maybe the ops read/write files from a directory specified by DATA_DIR. So each op's function will read the input file from DATA_DIR and write the output there. So DATA_DIR can be a resource that provides the path.

So first, create a resource for DATA_DIR. Then, each op uses this resource to get the directory path.

But how to handle the parameters like DATASET_ID=2? Those are per-op configurations. So each op can have a config schema that includes these parameters.

Let me structure each op with a config and the resource.

For example, the load_and_modify_data op would have config parameters like dataset_id, table_name_prefix. The op function would use these along with the data_dir resource to construct the input and output file paths.

Similarly, the reconciliation_geocoding op would have config parameters like primary_column, reconciliator_id, api_token, dataset_id.

But in Dagster, each op's config is specified when executing the job. So in the job definition, we can set the default configuration, but in the code, each op needs to have a config schema defined.

So, for each op, define the required config fields. Then, in the job, we can set the default config.

But the user wants minimal code, so perhaps the config is handled via the op's context. For example:

@op(config_schema={"dataset_id": int, "table_name_prefix": str})
def load_and_modify_data(context):
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_{dataset_id}.json"
    # Simulate processing
    context.log.info(f"Loading {input_path}, outputting to {output_path}")
    # Here, perhaps read CSV, process, write JSON
    # For the example, just touch the files or simulate
    open(input_path, 'a').close()  # Ensure file exists for example
    open(output_path, 'w').close()

But since the actual processing is done via external services (Docker containers), maybe the ops are just stubs that log the parameters and simulate the data flow. The key is to model the dependencies and parameters correctly.

So, each op will have a config with the necessary parameters, use the data_dir resource to get the directory, and then log the actions. The actual file operations are simulated for the sake of creating executable code.

Now, the resources: The DATA_DIR is a resource. So define a resource that provides the data directory path. Maybe using a simple resource:

from dagster import resource

@resource(config_schema={"data_dir": str})
def data_dir_resource(init_context):
    return init_context.resource_config["data_dir"]

Then, in the job, the resource is configured with the data_dir.

For the API_TOKEN in the geocoding step, that's part of the op's config.

Putting it all together:

Define the ops with their config schemas, use the data_dir resource, and set up the job with the dependencies.

Also, the main block to execute the job in-process with the necessary configuration.

So, the code would look something like:

from dagster import op, job, ResourceDefinition, resource

@resource(config_schema={"data_dir": str})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

@op(required_resource_keys={"data_dir"})
def load_and_modify_data(context):
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    # Process input and output paths
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_{dataset_id}.json"
    # Simulate calling the service
    context.log.info(f"Processing {input_path} to {output_path}")
    # For the example, create the output file
    with open(output_path, 'w') as f:
        f.write("{}")

@op(required_resource_keys={"data_dir"})
def reconciliation_geocoding(context):
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    input_path = f"{data_dir}/table_data_{dataset_id}.json"
    output_path = f"{data_dir}/reconciled_table_{dataset_id}.json"
    # Simulate geocoding
    context.log.info(f"Geocoding {input_path} to {output_path}")
    with open(output_path, 'w') as f:
        f.write('{"latitude": 1.0, "longitude": 2.0}')

@op(required_resource_keys={"data_dir"})
def calculate_pt_distance(context):
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    input_path = f"{data_dir}/reconciled_table_{dataset_id}.json"
    output_path = f"{data_dir}/distance_pt_{dataset_id}.json"
    # Simulate distance calculation
    context.log.info(f"Calculating PT distance for {input_path}")
    with open(output_path, 'w') as f:
        f.write('{"distance_to_pt": 500}')

@op(required_resource_keys={"data_dir"})
def calculate_residential_distance(context):
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    input_path = f"{data_dir}/distance_pt_{dataset_id}.json"
    output_path = f"{data_dir}/column_extended_{dataset_id}.json"
    # Simulate residential distance calculation
    context.log.info(f"Calculating residential distance for {input_path}")
    with open(output_path, 'w') as f:
        f.write('{"distance_to_residential": 1000}')

@op(required_resource_keys={"data_dir"})
def save_final_data(context):
    data_dir = context.resources.data_dir
    dataset_id = context.op_config["dataset_id"]
    input_path = f"{data_dir}/column_extended_{dataset_id}.json"
    output_path = f"{data_dir}/enriched_data_{dataset_id}.csv"
    # Simulate saving to CSV
    context.log.info(f"Saving final data to {output_path}")
    with open(output_path, 'w') as f:
        f.write("facility,distance_to_pt,distance_to_residential")

@job(resource_defs={"data_dir": data_dir_resource})
def medical_facility_accessibility_job():
    save_final_data(
        calculate_residential_distance(
            calculate_pt_distance(
                reconciliation_geocoding(
                    load_and_modify_data()
                )
            )
        )
    )

if __name__ == "__main__":
    run_config = {
        "resources": {
            "data_dir": {"config": {"data_dir": "data"}}
        },
        "ops": {
            "load_and_modify_data": {"config": {"dataset_id": 2, "table_name_prefix": "JOT_"}},
            "reconciliation_geocoding": {"config": {"dataset_id": 2, "primary_column": "address", "reconciliator_id": "geocodingHere", "api_token": "HERE_API_TOKEN"}},
            "calculate_pt_distance": {"config": {"dataset_id": 2, "extender_id": "spatialDistanceCalculator", "lat_column": "latitude", "lon_column": "longitude", "target_layer": "public_transport", "target_data_source": "/app/data/transport_stops.geojson", "output_column": "distance_to_pt"}},
            "calculate_residential_distance": {"config": {"dataset_id": 2, "extender_id": "spatialDistanceCalculator", "lat_column": "latitude", "lon_column": "longitude", "target_layer": "residential_areas", "target_data_source": "/app/data/residential_areas.geojson", "output_column": "distance_to_residential"}},
            "save_final_data": {"config": {"dataset_id": 2}}
        }
    }
    result = medical_facility_accessibility_job.execute_in_process(run_config=run_config)

Wait, but each op's config is defined in the run_config under "ops" -> op_name -> "config". So the op config schemas need to match the provided config.

But in the current code, the ops like load_and_modify_data don't have a config_schema defined. So I need to add config_schema to each op.

For example, load_and_modify_data should have a config_schema that includes dataset_id and table_name_prefix.

So updating the op definitions:

@op(config_schema={"dataset_id": int, "table_name_prefix": str}, required_resource_keys={"data_dir"})
def load_and_modify_data(context):
    dataset_id = context.op_config["dataset_id"]
    table_prefix = context.op_config["table_name_prefix"]
    # Use these as needed, perhaps in the service call parameters
    # For simulation, just log them
    context.log.info(f"Dataset ID: {dataset_id}, Table prefix: {table_prefix}")
    # Rest of the code...

Similarly, reconciliation_geocoding would have config_schema including primary_column, reconciliator_id, api_token, dataset_id.

But in the current code, the op_config for reconciliation_geocoding includes dataset_id, primary_column, etc. So the config_schema should reflect that.

So:

@op(config_schema={
    "dataset_id": int,
    "primary_column": str,
    "reconciliator_id": str,
    "api_token": str
}, required_resource_keys={"data_dir"})
def reconciliation_geocoding(context):
    dataset_id = context.op_config["dataset_id"]
    primary_col = context.op_config["primary_column"]
    api_token = context.op_config["api_token"]
    # Log these parameters
    context.log.info(f"Reconciling with {primary_col}, API token: {api_token}")
    # Rest of the code...

Similarly for the other ops.

But in the original pipeline steps, some parameters are fixed (like DATASET_ID=2 for all steps). So in the run_config, these are set per op.

This way, each op's config is properly defined, and the job's run_config provides the necessary parameters.

Putting it all together, the code would have each op with their respective config schemas, the job defines the dependencies, and the main block provides the run_config with all the parameters.

Testing this code would require creating a data directory with facilities.csv, but for simulation purposes, the code creates the necessary files. However, in a real scenario, the services would process the files, but here we just simulate by writing empty files.

This should meet the user's requirements: executable code, uses @op and @job, explicit dependencies, minimal resources/config examples, and a main block to execute the job.
</think>

from dagster import op, job, resource, ResourceDefinition

@resource(config_schema={"data_dir": str})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

@op(
    config_schema={
        "dataset_id": int,
        "table_name_prefix": str
    },
    required_resource_keys={"data_dir"}
)
def load_and_modify_data(context):
    data_dir = context.resources.data_dir
    cfg = context.op_config
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_{cfg['dataset_id']}.json"
    
    context.log.info(f"Processing {input_path} -> {output_path}")
    with open(input_path) as f_in, open(output_path, "w") as f_out:
        f_out.write(f_in.read().upper())  # Simplified transformation

@op(
    config_schema={
        "dataset_id": int,
        "primary_column": str,
        "reconciliator_id": str,
        "api_token": str
    },
    required_resource_keys={"data_dir"}
)
def reconciliation_geocoding(context):
    data_dir = context.resources.data_dir
    cfg = context.op_config
    input_path = f"{data_dir}/table_data_{cfg['dataset_id']}.json"
    output_path = f"{data_dir}/reconciled_table_{cfg['dataset_id']}.json"

    context.log.info(f"Geocoding with {cfg['reconciliator_id']}")
    with open(output_path, "w") as f:
        f.write('{"coordinates": [1.23, 4.56]}')  # Simulated geocoding

@op(
    config_schema={
        "dataset_id": int,
        "extender_id": str,
        "lat_column": str,
        "lon_column": str,
        "target_layer": str,
        "target_data_source": str,
        "output_column": str
    },
    required_resource_keys={"data_dir"}
)
def calculate_pt_distance(context):
    data_dir = context.resources.data_dir
    cfg = context.op_config
    input_path = f"{data_dir}/reconciled_table_{cfg['dataset_id']}.json"
    output_path = f"{data_dir}/distance_pt_{cfg['dataset_id']}.json"

    context.log.info(f"Calculating PT distances using {cfg['target_layer']}")
    with open(output_path, "w") as f:
        f.write('{"distance_to_pt": 250}')  # Simulated distance

@op(
    config_schema={
        "dataset_id": int,
        "extender_id": str,
        "lat_column": str,
        "lon_column": str,
        "target_layer": str,
        "target_data_source": str,
        "output_column": str
    },
    required_resource_keys={"data_dir"}
)
def calculate_residential_distance(context):
    data_dir = context.resources.data_dir
    cfg = context.op_config
    input_path = f"{data_dir}/distance_pt_{cfg['dataset_id']}.json"
    output_path = f"{data_dir}/column_extended_{cfg['dataset_id']}.json"

    context.log.info(f"Calculating residential distances using {cfg['target_layer']}")
    with open(output_path, "w") as f:
        f.write('{"distance_to_residential": 750}')  # Simulated distance

@op(
    config_schema={"dataset_id": int},
    required_resource_keys={"data_dir"}
)
def save_final_data(context):
    data_dir = context.resources.data_dir
    cfg = context.op_config
    input_path = f"{data_dir}/column_extended_{cfg['dataset_id']}.json"
    output_path = f"{data_dir}/enriched_data_{cfg['dataset_id']}.csv"

    context.log.info(f"Saving final CSV to {output_path}")
    with open(output_path, "w") as f:
        f.write("facility_id,distance_to_pt,distance_to_residential")

@job(resource_defs={"data_dir": data_dir_resource})
def medical_facility_accessibility_job():
    pt_dist = calculate_pt_distance(
        reconciliation_geocoding(
            load_and_modify_data()
        )
    )
    residential_dist = calculate_residential_distance(pt_dist)
    save_final_data(residential_dist)

if __name__ == "__main__":
    run_config = {
        "resources": {"data_dir": {"config": {"data_dir": "./data"}}},
        "ops": {
            "load_and_modify_data": {
                "config": {"dataset_id": 2, "table_name_prefix": "JOT_"}
            },
            "reconciliation_geocoding": {
                "config": {
                    "dataset_id": 2,
                    "primary_column": "address",
                    "reconciliator_id": "geocodingHere",
                    "api_token": "HERE_API_TOKEN"
                }
            },
            "calculate_pt_distance": {
                "config": {
                    "dataset_id": 2,
                    "extender_id": "spatialDistanceCalculator",
                    "lat_column": "latitude",
                    "lon_column": "longitude",
                    "target_layer": "public_transport",
                    "target_data_source": "/app/data/transport_stops.geojson",
                    "output_column": "distance_to_pt"
                }
            },
            "calculate_residential_distance": {
                "config": {
                    "dataset_id": 2,
                    "extender_id": "spatialDistanceCalculator",
                    "lat_column": "latitude",
                    "lon_column": "longitude",
                    "target_layer": "residential_areas",
                    "target_data_source": "/app/data/residential_areas.geojson",
                    "output_column": "distance_to_residential"
                }
            },
            "save_final_data": {"config": {"dataset_id": 2}}
        }
    }
    result = medical_facility_accessibility_job.execute_in_process(run_config=run_config)