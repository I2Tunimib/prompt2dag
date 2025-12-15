from dagster import op, job, get_dagster_logger, RetryPolicy

# Define the ops for each step in the pipeline

@op
def load_and_modify_data(context):
    """
    Load and modify the station data.
    """
    import requests
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    input_file = f"{data_dir}/stations.csv"
    output_file = f"{data_dir}/table_data_2.json"

    # Call the load-and-modify service
    response = requests.post(
        "http://localhost:3003/load-and-modify",
        json={
            "DATASET_ID": 2,
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_",
            "INPUT_FILE": input_file,
            "OUTPUT_FILE": output_file
        }
    )

    if response.status_code != 200:
        raise Exception(f"Failed to load and modify data: {response.text}")

    with open(output_file, "w") as f:
        json.dump(response.json(), f)

    return output_file


@op
def reconcile_geocoding(context, input_file):
    """
    Reconcile station locations using geocoding.
    """
    import requests
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/reconciled_table_2.json"

    # Call the reconciliation service
    response = requests.post(
        "http://localhost:3003/reconciliation",
        json={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": context.op_config.get("HERE_API_TOKEN"),
            "DATASET_ID": 2,
            "INPUT_FILE": input_file,
            "OUTPUT_FILE": output_file
        }
    )

    if response.status_code != 200:
        raise Exception(f"Failed to reconcile geocoding: {response.text}")

    with open(output_file, "w") as f:
        json.dump(response.json(), f)

    return output_file


@op
def extend_with_openmeteo_data(context, input_file):
    """
    Add historical weather data based on geocoded location and installation_date.
    """
    import requests
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/open_meteo_2.json"

    # Call the OpenMeteo service
    response = requests.post(
        "http://localhost:3003/openmeteo-extension",
        json={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "DATE_COLUMN": "installation_date",
            "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
            "DATE_SEPARATOR_FORMAT": "YYYYMMDD",
            "INPUT_FILE": input_file,
            "OUTPUT_FILE": output_file
        }
    )

    if response.status_code != 200:
        raise Exception(f"Failed to extend with OpenMeteo data: {response.text}")

    with open(output_file, "w") as f:
        json.dump(response.json(), f)

    return output_file


@op
def extend_with_land_use_data(context, input_file):
    """
    Add land use classification based on location using a GIS API.
    """
    import requests
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/land_use_2.json"

    # Call the GIS Land Use API
    response = requests.post(
        "http://localhost:3003/land-use-extension",
        json={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "land_use_type",
            "API_KEY": context.op_config.get("GEOAPIFY_API_KEY"),
            "INPUT_FILE": input_file,
            "OUTPUT_FILE": output_file
        }
    )

    if response.status_code != 200:
        raise Exception(f"Failed to extend with land use data: {response.text}")

    with open(output_file, "w") as f:
        json.dump(response.json(), f)

    return output_file


@op
def extend_with_population_density_data(context, input_file):
    """
    Add population density data for the area surrounding the station location.
    """
    import requests
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/pop_density_2.json"

    # Call the Demographic Data Service
    response = requests.post(
        "http://localhost:3003/population-density-extension",
        json={
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "OUTPUT_COLUMN": "population_density",
            "RADIUS": 5000,
            "INPUT_FILE": input_file,
            "OUTPUT_FILE": output_file
        }
    )

    if response.status_code != 200:
        raise Exception(f"Failed to extend with population density data: {response.text}")

    with open(output_file, "w") as f:
        json.dump(response.json(), f)

    return output_file


@op
def extend_with_environmental_risk_data(context, input_file):
    """
    Compute custom environmental risk factors based on combined data.
    """
    import requests
    import json

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/column_extended_2.json"

    # Call the column extension service
    response = requests.post(
        "http://localhost:3003/column-extension",
        json={
            "EXTENDER_ID": "environmentalRiskCalculator",
            "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
            "OUTPUT_COLUMN": "risk_score",
            "CALCULATION_FORMULA": context.op_config.get("RISK_CALCULATION_PARAMETERS"),
            "INPUT_FILE": input_file,
            "OUTPUT_FILE": output_file
        }
    )

    if response.status_code != 200:
        raise Exception(f"Failed to extend with environmental risk data: {response.text}")

    with open(output_file, "w") as f:
        json.dump(response.json(), f)

    return output_file


@op
def save_final_data(context, input_file):
    """
    Export the comprehensive environmental dataset to CSV.
    """
    import pandas as pd

    data_dir = context.op_config.get("DATA_DIR", "./data")
    output_file = f"{data_dir}/enriched_data_2.csv"

    # Load the final JSON data
    with open(input_file, "r") as f:
        data = json.load(f)

    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

    return output_file


# Define the job
@job(
    resource_defs={
        "io_manager": dagster_file_manager.configured({"base_dir": "./data"})
    },
    retry_policy=RetryPolicy(max_retries=1)
)
def environmental_monitoring_pipeline():
    """
    Environmental Monitoring Network Pipeline
    """
    table_data_file = load_and_modify_data()
    reconciled_table_file = reconcile_geocoding(table_data_file)
    open_meteo_file = extend_with_openmeteo_data(reconciled_table_file)
    land_use_file = extend_with_land_use_data(open_meteo_file)
    pop_density_file = extend_with_population_density_data(land_use_file)
    column_extended_file = extend_with_environmental_risk_data(pop_density_file)
    save_final_data(column_extended_file)


# Minimal launch pattern
if __name__ == "__main__":
    result = environmental_monitoring_pipeline.execute_in_process()