<think>
Okay, I need to convert this pipeline description into a Dagster job. Let's start by understanding each step and how they connect.

First, the pipeline has five steps: Load & Modify Data, Reconciliation (Geocoding), Distance Calculation for Public Transport, Distance Calculation for Residential Areas, and Save Final Data. Each step is a separate Docker container with specific parameters. The dependencies are sequential except for steps 3 and 4, which both depend on step 2 but can run in parallel? Wait, no, looking at the inputs: step 3 takes the output of step 2, step 4 takes the output of step 3. So it's a linear flow: 1 → 2 → 3 → 4 → 5.

Each step is an op in Dagster. So I'll create five ops. Each op will execute a Docker container, probably using Dagster's Docker executor or something. But the user didn't specify execution details, just to model the dependencies. So maybe the actual execution is abstracted, and the ops are just placeholders that represent each step.

Wait, the constraints mention using @op and @job. So each step is an op, connected via dependencies. The actual processing might be done via API calls to services running in Docker, but the code here just needs to model the steps and their dependencies.

Each op takes some input and produces output. The inputs and outputs are files in DATA_DIR. So maybe each op reads from a file, processes, writes to another file. But since the user wants explicit dependencies, perhaps each op returns the filename or a data structure that the next op uses.

But in Dagster, ops pass Python objects to each other. So maybe each op returns the path to the generated file, which the next op uses as input. For example, the first op returns 'table_data_2.json', which is passed to the reconciliation op, and so on.

So the ops would be structured such that each produces the filename needed for the next step. The parameters for each op (like DATASET_ID, API_TOKEN, etc.) can be configured using Dagster's config system.

I need to define each op with the necessary config parameters. For example, the Load & Modify Data op needs DATASET_ID and TABLE_NAME_PREFIX. These can be specified in the config_schema for the op.

Also, the user mentioned using resources or config stubs. So maybe the DATA_DIR is a resource that provides the base path. Or perhaps it's part of the config.

Let me outline each op:

1. load_and_modify_data: Takes facilities.csv, processes it, outputs table_data_2.json. Config parameters: DATASET_ID=2, TABLE_NAME_PREFIX=JOT_.

2. reconciliation_geocoding: Uses HERE API. Input is table_data_2.json, outputs reconciled_table_2.json. Config: PRIMARY_COLUMN=address, RECONCILIATOR_ID=geocodingHere, API_TOKEN, DATASET_ID=2.

3. distance_calculation_pt: Input is reconciled_table_2.json, outputs distance_pt_2.json. Config: EXTENDER_ID, LAT_COLUMN, LON_COLUMN, TARGET_LAYER, TARGET_DATA_SOURCE, OUTPUT_COLUMN, DATASET_ID=2.

4. distance_calculation_residential: Input is distance_pt_2.json, outputs column_extended_2.json. Similar config to step 3 but different TARGET_LAYER and OUTPUT_COLUMN.

5. save_final_data: Input is column_extended_2.json, outputs enriched_data_2.csv. Config: DATASET_ID=2.

Now, how to model these in Dagster. Each op will have a function that perhaps simulates calling the service (since we can't actually run Docker here). But the user wants executable code, so maybe the functions just handle the file names, and the actual processing is mocked. For example, each op reads the input file, does some processing (like appending data), writes the output file.

But the user wants the code to be executable. So perhaps the code uses placeholders for the actual processing, but handles the file paths correctly.

Wait, the user said: "If you need resources/config, provide simplified resource or config examples as comments or minimal stubs." So maybe the config is handled via Dagster's config system, and resources are stubbed.

For example, the DATA_DIR could be a resource. So define a resource that provides the data directory path.

Also, the API_TOKEN for HERE needs to be provided via config. So the reconciliation op's config would include that.

But since the code must be executable, perhaps the config is provided via the run_config when executing the job.

But for the code to be standalone, maybe the config is defined with default values where possible, and the necessary ones (like API_TOKEN) are marked as required.

Wait, but in the example, the user might not want to hardcode the API_TOKEN. So perhaps in the config schema, it's specified as a required field, and when running the job, it's passed in.

But the code must be executable. So maybe in the __main__ block, the job is executed with the necessary config.

But the user's constraint says: "If you need resources/config, provide simplified resource or config examples as comments or minimal stubs." So perhaps the config is included as comments or minimal stubs.

Alternatively, the code can have placeholders for the config, but with comments indicating where to fill in.

But the code must be executable. So perhaps the config is provided with default values where possible, and required ones are set to a placeholder, but the code can run.

Wait, but the API_TOKEN is required for the reconciliation step. If it's not provided, the code would fail. So maybe in the example, the config includes a dummy token, but in practice, the user would replace it.

But the user said to avoid placeholders like <TODO>, so perhaps the code uses a default value in the config schema but with a comment indicating to replace it.

Alternatively, the code can have the config schema with the required fields, and when executing in the __main__ block, pass the necessary config.

So putting it all together:

- Define a resource for DATA_DIR, defaulting to some path.
- Each op has a config schema with the required parameters.
- The ops are connected in sequence: load → reconcile → distance_pt → distance_residential → save.

Now, writing the code:

First, import necessary modules: from dagster import op, job, ResourceParam, get_dagster_logger.

The DATA_DIR can be a resource. So define it as a resource parameter.

Each op will take the data_dir as a resource, and any necessary config parameters.

For example, the load_and_modify_data op:

@op(config_schema={"dataset_id": int, "table_name_prefix": str})
def load_and_modify_data(context, data_dir: ResourceParam[str]):
    # uses data_dir to read facilities.csv and write table_data_2.json
    # calls the service via API (mocked here)
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_2.json"
    # simulate processing
    context.log.info(f"Processing {input_path} into {output_path}")
    return output_path

Similarly for the other ops. Each op takes the previous output path as an input.

The reconciliation op:

@op(config_schema={
    "primary_column": str,
    "reconcilator_id": str,
    "api_token": str,
    "dataset_id": int
})
def reconciliation_geocoding(context, data_dir: ResourceParam[str], input_path: str):
    # input_path is from previous op
    output_path = f"{data_dir}/reconciled_table_2.json"
    # simulate API call
    context.log.info(f"Geocoding {input_path} to {output_path}")
    return output_path

Wait, but the config parameters for each op are specific. For example, the reconciliation step has PRIMARY_COLUMN=address, RECONCILIATOR_ID=geocodingHere, etc. So the config_schema should include those fields, and in the job configuration, those values are set.

But in the code, the op's config_schema would have the required fields, and the defaults can be set in the job's config.

Alternatively, the job's config can provide the necessary parameters for each op.

So when defining the job, each op's config is specified in the run_config.

But for the code to be executable, perhaps the example includes a sample run_config in the __main__ block.

Putting it all together:

The job is defined with the five ops in order. Each op depends on the previous one's output.

The resources are defined, like data_dir.

The config for each op is provided via the run_config when executing the job.

So the final code would look something like:

from dagster import op, job, ResourceParam, get_dagster_logger

logger = get_dagster_logger()

@op(config_schema={
    "dataset_id": int,
    "table_name_prefix": str
})
def load_and_modify_data(context, data_dir: ResourceParam[str]):
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_{context.op_config['dataset_id']}.json"
    logger.info(f"Loading and modifying data from {input_path} to {output_path}")
    # Simulate API call to load-and-modify service
    return output_path

@op(config_schema={
    "primary_column": str,
    "reconcilator_id": str,
    "api_token": str,
    "dataset_id": int
})
def reconciliation_geocoding(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/reconciled_table_{context.op_config['dataset_id']}.json"
    logger.info(f"Geocoding facilities using HERE API, output to {output_path}")
    # Simulate API call to reconciliation service
    return output_path

@op(config_schema={
    "extender_id": str,
    "lat_column": str,
    "lon_column": str,
    "target_layer": str,
    "target_data_source": str,
    "output_column": str,
    "dataset_id": int
})
def distance_calculation_pt(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/distance_pt_{context.op_config['dataset_id']}.json"
    logger.info(f"Calculating PT distances, output to {output_path}")
    # Simulate API call to column extension service
    return output_path

@op(config_schema={
    "extender_id": str,
    "lat_column": str,
    "lon_column": str,
    "target_layer": str,
    "target_data_source": str,
    "output_column": str,
    "dataset_id": int
})
def distance_calculation_residential(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/column_extended_{context.op_config['dataset_id']}.json"
    logger.info(f"Calculating residential distances, output to {output_path}")
    # Simulate API call to column extension service
    return output_path

@op(config_schema={"dataset_id": int})
def save_final_data(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/enriched_data_{context.op_config['dataset_id']}.csv"
    logger.info(f"Saving final data to {output_path}")
    # Simulate API call to save service
    return output_path

@job(resource_defs={"data_dir": ResourceParam.configured("/path/to/data")})
def medical_facility_accessibility_job():
    data_dir = "data_dir"
    step1 = load_and_modify_data()
    step2 = reconciliation_geocoding(step1)
    step3 = distance_calculation_pt(step2)
    step4 = distance_calculation_residential(step3)
    save_final_data(step4)

if __name__ == "__main__":
    run_config = {
        "ops": {
            "load_and_modify_data": {
                "config": {
                    "dataset_id": 2,
                    "table_name_prefix": "JOT_"
                }
            },
            "reconciliation_geocoding": {
                "config": {
                    "primary_column": "address",
                    "reconcilator_id": "geocodingHere",
                    "api_token": "dummy_token",  # Replace with actual HERE API token
                    "dataset_id": 2
                }
            },
            "distance_calculation_pt": {
                "config": {
                    "extender_id": "spatialDistanceCalculator",
                    "lat_column": "latitude",
                    "lon_column": "longitude",
                    "target_layer": "public_transport",
                    "target_data_source": "/app/data/transport_stops.geojson",
                    "output_column": "distance_to_pt",
                    "dataset_id": 2
                }
            },
            "distance_calculation_residential": {
                "config": {
                    "extender_id": "spatialDistanceCalculator",
                    "lat_column": "latitude",
                    "lon_column": "longitude",
                    "target_layer": "residential_areas",
                    "target_data_source": "/app/data/residential_areas.geojson",
                    "output_column": "distance_to_residential",
                    "dataset_id": 2
                }
            },
            "save_final_data": {
                "config": {
                    "dataset_id": 2
                }
            }
        }
    }
    result = medical_facility_accessibility_job.execute_in_process(run_config=run_config)

This code defines each op with the required config parameters, sets up the dependencies, and includes a sample run_config that provides all necessary parameters. The data_dir resource is configured with a placeholder path. The actual processing steps are simulated with logging statements. The __main__ block executes the job with the sample configuration.

I need to check if the dependencies are correctly set. The job's function calls each op in sequence: step2 depends on step1, step3 on step2, etc. Yes, that's correct.

Each op's config is pulled from the run_config. The resource_defs for the job sets the data_dir to "/path/to/data", which the user would need to adjust.

The code uses ResourceParam for data_dir, which is injected into each op. Each op uses data_dir to construct file paths.

The config_schema for each op includes the parameters specified in the pipeline description. For example, the reconciliation_geocoding op has primary_column, reconcilator_id, api_token, dataset_id.

In the run_config, each op's config section provides the necessary values. The API_TOKEN is set to "dummy_token" here, which the user would replace.

This code should be executable, assuming Dagster is installed. It doesn't actually perform the API calls or file operations but simulates them with logging. The dependencies are correctly modeled, and the configuration is provided via run_config.
</think>

from dagster import op, job, ResourceParam, get_dagster_logger

logger = get_dagster_logger()

@op(config_schema={
    "dataset_id": int,
    "table_name_prefix": str
})
def load_and_modify_data(context, data_dir: ResourceParam[str]):
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_{context.op_config['dataset_id']}.json"
    logger.info(f"Loading and modifying data from {input_path} to {output_path}")
    return output_path

@op(config_schema={
    "primary_column": str,
    "reconcilator_id": str,
    "api_token": str,
    "dataset_id": int
})
def reconciliation_geocoding(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/reconciled_table_{context.op_config['dataset_id']}.json"
    logger.info(f"Geocoding facilities using HERE API, output to {output_path}")
    return output_path

@op(config_schema={
    "extender_id": str,
    "lat_column": str,
    "lon_column": str,
    "target_layer": str,
    "target_data_source": str,
    "output_column": str,
    "dataset_id": int
})
def distance_calculation_pt(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/distance_pt_{context.op_config['dataset_id']}.json"
    logger.info(f"Calculating PT distances, output to {output_path}")
    return output_path

@op(config_schema={
    "extender_id": str,
    "lat_column": str,
    "lon_column": str,
    "target_layer": str,
    "target_data_source": str,
    "output_column": str,
    "dataset_id": int
})
def distance_calculation_residential(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/column_extended_{context.op_config['dataset_id']}.json"
    logger.info(f"Calculating residential distances, output to {output_path}")
    return output_path

@op(config_schema={"dataset_id": int})
def save_final_data(context, data_dir: ResourceParam[str], input_path: str):
    output_path = f"{data_dir}/enriched_data_{context.op_config['dataset_id']}.csv"
    logger.info(f"Saving final data to {output_path}")
    return output_path

@job(resource_defs={"data_dir": ResourceParam.configured("/path/to/data")})
def medical_facility_accessibility_job():
    step1 = load_and_modify_data()
    step2 = reconciliation_geocoding(step1)
    step3 = distance_calculation_pt(step2)
    step4 = distance_calculation_residential(step3)
    save_final_data(step4)

if __name__ == "__main__":
    run_config = {
        "ops": {
            "load_and_modify_data": {
                "config": {
                    "dataset_id": 2,
                    "table_name_prefix": "JOT_"
                }
            },
            "reconciliation_geocoding": {
                "config": {
                    "primary_column": "address",
                    "reconcilator_id": "geocodingHere",
                    "api_token": "dummy_token",
                    "dataset_id": 2
                }
            },
            "distance_calculation_pt": {
                "config": {
                    "extender_id": "spatialDistanceCalculator",
                    "lat_column": "latitude",
                    "lon_column": "longitude",
                    "target_layer": "public_transport",
                    "target_data_source": "/app/data/transport_stops.geojson",
                    "output_column": "distance_to_pt",
                    "dataset_id": 2
                }
            },
            "distance_calculation_residential": {
                "config": {
                    "extender_id": "spatialDistanceCalculator",
                    "lat_column": "latitude",
                    "lon_column": "longitude",
                    "target_layer": "residential_areas",
                    "target_data_source": "/app/data/residential_areas.geojson",
                    "output_column": "distance_to_residential",
                    "dataset_id": 2
                }
            },
            "save_final_data": {
                "config": {
                    "dataset_id": 2
                }
            }
        }
    }
    result = medical_facility_accessibility_job.execute_in_process(run_config=run_config)