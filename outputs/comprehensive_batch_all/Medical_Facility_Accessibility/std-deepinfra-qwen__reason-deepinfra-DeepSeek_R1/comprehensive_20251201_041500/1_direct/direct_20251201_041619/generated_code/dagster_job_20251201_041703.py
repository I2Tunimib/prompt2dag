from dagster import op, job, resource, RetryPolicy, Failure, Field, String

# Define resources
@resource(config_schema={"data_dir": Field(String, default_value="/data")})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

@resource(config_schema={"api_token": Field(String, is_required=True)})
def here_api_token_resource(context):
    return context.resource_config["api_token"]

# Define ops
@op(required_resource_keys={"data_dir"})
def load_and_modify(context):
    data_dir = context.resources.data_dir
    input_path = f"{data_dir}/facilities.csv"
    output_path = f"{data_dir}/table_data_2.json"
    # Call load-and-modify service
    # Example: subprocess.run(["docker", "run", "--rm", "-v", f"{data_dir}:/data", "i2t-backendwithintertwino6-load-and-modify:latest", "load-and-modify", "--dataset-id", "2", "--table-name-prefix", "JOT_", "--input", input_path, "--output", output_path])
    context.log.info(f"Loaded and modified data from {input_path} to {output_path}")
    return output_path

@op(required_resource_keys={"data_dir", "here_api_token"})
def reconcile_geocoding(context, input_path):
    data_dir = context.resources.data_dir
    api_token = context.resources.here_api_token
    output_path = f"{data_dir}/reconciled_table_2.json"
    # Call reconciliation service
    # Example: subprocess.run(["docker", "run", "--rm", "-v", f"{data_dir}:/data", "i2t-backendwithintertwino6-reconciliation:latest", "reconcile", "--primary-column", "address", "--reconciliator-id", "geocodingHere", "--api-token", api_token, "--dataset-id", "2", "--input", input_path, "--output", output_path])
    context.log.info(f"Reconciled geocoding data from {input_path} to {output_path}")
    return output_path

@op(required_resource_keys={"data_dir"})
def calculate_distance_to_public_transport(context, input_path):
    data_dir = context.resources.data_dir
    output_path = f"{data_dir}/distance_pt_2.json"
    # Call column extension service
    # Example: subprocess.run(["docker", "run", "--rm", "-v", f"{data_dir}:/data", "i2t-backendwithintertwino6-column-extension:latest", "extend", "--extender-id", "spatialDistanceCalculator", "--lat-column", "latitude", "--lon-column", "longitude", "--target-layer", "public_transport", "--target-data-source", "/app/data/transport_stops.geojson", "--output-column", "distance_to_pt", "--dataset-id", "2", "--input", input_path, "--output", output_path])
    context.log.info(f"Calculated distance to public transport from {input_path} to {output_path}")
    return output_path

@op(required_resource_keys={"data_dir"})
def calculate_distance_to_residential_areas(context, input_path):
    data_dir = context.resources.data_dir
    output_path = f"{data_dir}/column_extended_2.json"
    # Call column extension service
    # Example: subprocess.run(["docker", "run", "--rm", "-v", f"{data_dir}:/data", "i2t-backendwithintertwino6-column-extension:latest", "extend", "--extender-id", "spatialDistanceCalculator", "--lat-column", "latitude", "--lon-column", "longitude", "--target-layer", "residential_areas", "--target-data-source", "/app/data/residential_areas.geojson", "--output-column", "distance_to_residential", "--dataset-id", "2", "--input", input_path, "--output", output_path])
    context.log.info(f"Calculated distance to residential areas from {input_path} to {output_path}")
    return output_path

@op(required_resource_keys={"data_dir"})
def save_final_data(context, input_path):
    data_dir = context.resources.data_dir
    output_path = f"{data_dir}/enriched_data_2.csv"
    # Call save service
    # Example: subprocess.run(["docker", "run", "--rm", "-v", f"{data_dir}:/data", "i2t-backendwithintertwino6-save:latest", "save", "--dataset-id", "2", "--input", input_path, "--output", output_path])
    context.log.info(f"Saved final data from {input_path} to {output_path}")

# Define job
@job(
    resource_defs={
        "data_dir": data_dir_resource,
        "here_api_token": here_api_token_resource,
    },
    retry_policy=RetryPolicy(max_retries=1),
)
def medical_facility_accessibility_job():
    input_path = load_and_modify()
    reconciled_path = reconcile_geocoding(input_path)
    distance_pt_path = calculate_distance_to_public_transport(reconciled_path)
    column_extended_path = calculate_distance_to_residential_areas(distance_pt_path)
    save_final_data(column_extended_path)

# Launch pattern
if __name__ == "__main__":
    result = medical_facility_accessibility_job.execute_in_process(
        run_config={
            "resources": {
                "data_dir": {"config": {"data_dir": "/path/to/data"}},
                "here_api_token": {"config": {"api_token": "your_here_api_token"}},
            }
        }
    )