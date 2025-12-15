from dagster import op, job, resource, RetryPolicy, Field, String, Int

# Resources
@resource(config_schema={"data_dir": Field(String, default_value="/data")})
def data_dir_resource(context):
    return context.resource_config["data_dir"]

@resource(config_schema={"api_token": Field(String, is_required=True)})
def here_api_resource(context):
    return context.resource_config["api_token"]

@resource(config_schema={"api_key": Field(String, is_required=True)})
def geoapify_api_resource(context):
    return context.resource_config["api_key"]

@resource(config_schema={"api_key": Field(String, is_required=True)})
def worldpop_api_resource(context):
    return context.resource_config["api_key"]

@resource(config_schema={"risk_calculation_params": Field(String, is_required=True)})
def risk_calculation_resource(context):
    return context.resource_config["risk_calculation_params"]

# Ops
@op(required_resource_keys={"data_dir"})
def load_and_modify(context):
    data_dir = context.resources.data_dir
    input_file = f"{data_dir}/stations.csv"
    output_file = f"{data_dir}/table_data_2.json"
    # Call load-and-modify service
    # Example: response = requests.post("http://load-and-modify:3003/modify", json={"DATASET_ID": 2, "DATE_COLUMN": "installation_date", "TABLE_NAME_PREFIX": "JOT_"})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("{}")
    return output_file

@op(required_resource_keys={"data_dir", "here_api"})
def reconcile_geocoding(context, input_file):
    data_dir = context.resources.data_dir
    api_token = context.resources.here_api
    output_file = f"{data_dir}/reconciled_table_2.json"
    # Call reconciliation service
    # Example: response = requests.post("http://reconciliation:3003/reconcile", json={"PRIMARY_COLUMN": "location", "RECONCILIATOR_ID": "geocodingHere", "API_TOKEN": api_token, "DATASET_ID": 2})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("{}")
    return output_file

@op(required_resource_keys={"data_dir"})
def open_meteo_extension(context, input_file):
    data_dir = context.resources.data_dir
    output_file = f"{data_dir}/open_meteo_2.json"
    # Call OpenMeteo service
    # Example: response = requests.post("http://openmeteo:3003/extend", json={"LAT_COLUMN": "latitude", "LON_COLUMN": "longitude", "DATE_COLUMN": "installation_date", "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours", "DATE_SEPARATOR_FORMAT": "YYYYMMDD"})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("{}")
    return output_file

@op(required_resource_keys={"data_dir", "geoapify_api"})
def land_use_extension(context, input_file):
    data_dir = context.resources.data_dir
    api_key = context.resources.geoapify_api
    output_file = f"{data_dir}/land_use_2.json"
    # Call GIS Land Use API
    # Example: response = requests.post("http://geoapify:3003/land_use", json={"LAT_COLUMN": "latitude", "LON_COLUMN": "longitude", "OUTPUT_COLUMN": "land_use_type", "API_KEY": api_key})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("{}")
    return output_file

@op(required_resource_keys={"data_dir", "worldpop_api"})
def population_density_extension(context, input_file):
    data_dir = context.resources.data_dir
    api_key = context.resources.worldpop_api
    output_file = f"{data_dir}/pop_density_2.json"
    # Call Demographic Data Service
    # Example: response = requests.post("http://worldpop:3003/density", json={"LAT_COLUMN": "latitude", "LON_COLUMN": "longitude", "OUTPUT_COLUMN": "population_density", "RADIUS": 5000, "API_KEY": api_key})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("{}")
    return output_file

@op(required_resource_keys={"data_dir", "risk_calculation"})
def environmental_calculation(context, input_file):
    data_dir = context.resources.data_dir
    risk_calculation_params = context.resources.risk_calculation
    output_file = f"{data_dir}/column_extended_2.json"
    # Call column extension service
    # Example: response = requests.post("http://column-extension:3003/extend", json={"EXTENDER_ID": "environmentalRiskCalculator", "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type", "OUTPUT_COLUMN": "risk_score", "CALCULATION_FORMULA": risk_calculation_params})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("{}")
    return output_file

@op(required_resource_keys={"data_dir"})
def save_final_data(context, input_file):
    data_dir = context.resources.data_dir
    output_file = f"{data_dir}/enriched_data_2.csv"
    # Call save service
    # Example: response = requests.post("http://save:3003/save", json={"DATASET_ID": 2})
    # Save response to output_file
    with open(output_file, "w") as f:
        f.write("")

# Job
@job(
    resource_defs={
        "data_dir": data_dir_resource,
        "here_api": here_api_resource,
        "geoapify_api": geoapify_api_resource,
        "worldpop_api": worldpop_api_resource,
        "risk_calculation": risk_calculation_resource,
    },
    retry_policy=RetryPolicy(max_retries=1)
)
def environmental_monitoring_pipeline():
    table_data = load_and_modify()
    reconciled_table = reconcile_geocoding(table_data)
    open_meteo_data = open_meteo_extension(reconciled_table)
    land_use_data = land_use_extension(open_meteo_data)
    pop_density_data = population_density_extension(land_use_data)
    column_extended_data = environmental_calculation(pop_density_data)
    save_final_data(column_extended_data)

# Launch pattern
if __name__ == "__main__":
    result = environmental_monitoring_pipeline.execute_in_process()