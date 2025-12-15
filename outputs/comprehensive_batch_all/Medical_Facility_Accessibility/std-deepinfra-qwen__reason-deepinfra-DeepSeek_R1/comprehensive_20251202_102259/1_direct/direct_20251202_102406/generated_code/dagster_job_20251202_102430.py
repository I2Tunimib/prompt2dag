from dagster import op, job, resource, RetryPolicy, get_dagster_logger
import os
import requests
import json

logger = get_dagster_logger()

# Resources
@resource(config_schema={"data_dir": str, "here_api_token": str})
def data_resources(context):
    return {
        "data_dir": context.resource_config["data_dir"],
        "here_api_token": context.resource_config["here_api_token"],
    }

# Ops
@op(required_resource_keys={"data_resources"})
def load_and_modify(context):
    data_dir = context.resources.data_resources["data_dir"]
    input_file = os.path.join(data_dir, "facilities.csv")
    output_file = os.path.join(data_dir, "table_data_2.json")

    # Call the load-and-modify service
    response = requests.post(
        "http://localhost:3003/load-and-modify",
        json={
            "DATASET_ID": 2,
            "TABLE_NAME_PREFIX": "JOT_",
            "input_file": input_file,
            "output_file": output_file,
        },
    )
    response.raise_for_status()

    logger.info(f"Loaded and modified data saved to {output_file}")
    return output_file

@op(required_resource_keys={"data_resources"})
def reconcile_geocoding(context, input_file):
    data_dir = context.resources.data_resources["data_dir"]
    here_api_token = context.resources.data_resources["here_api_token"]
    output_file = os.path.join(data_dir, "reconciled_table_2.json")

    # Call the reconciliation service
    response = requests.post(
        "http://localhost:3003/reconciliation",
        json={
            "PRIMARY_COLUMN": "address",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": here_api_token,
            "DATASET_ID": 2,
            "input_file": input_file,
            "output_file": output_file,
        },
    )
    response.raise_for_status()

    logger.info(f"Geocoded data saved to {output_file}")
    return output_file

@op(required_resource_keys={"data_resources"})
def calculate_distance_to_public_transport(context, input_file):
    data_dir = context.resources.data_resources["data_dir"]
    output_file = os.path.join(data_dir, "distance_pt_2.json")

    # Call the column extension service for public transport
    response = requests.post(
        "http://localhost:3003/column-extension",
        json={
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "public_transport",
            "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
            "OUTPUT_COLUMN": "distance_to_pt",
            "DATASET_ID": 2,
            "input_file": input_file,
            "output_file": output_file,
        },
    )
    response.raise_for_status()

    logger.info(f"Distance to public transport calculated and saved to {output_file}")
    return output_file

@op(required_resource_keys={"data_resources"})
def calculate_distance_to_residential_areas(context, input_file):
    data_dir = context.resources.data_resources["data_dir"]
    output_file = os.path.join(data_dir, "column_extended_2.json")

    # Call the column extension service for residential areas
    response = requests.post(
        "http://localhost:3003/column-extension",
        json={
            "EXTENDER_ID": "spatialDistanceCalculator",
            "LAT_COLUMN": "latitude",
            "LON_COLUMN": "longitude",
            "TARGET_LAYER": "residential_areas",
            "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
            "OUTPUT_COLUMN": "distance_to_residential",
            "DATASET_ID": 2,
            "input_file": input_file,
            "output_file": output_file,
        },
    )
    response.raise_for_status()

    logger.info(f"Distance to residential areas calculated and saved to {output_file}")
    return output_file

@op(required_resource_keys={"data_resources"})
def save_final_data(context, input_file):
    data_dir = context.resources.data_resources["data_dir"]
    output_file = os.path.join(data_dir, "enriched_data_2.csv")

    # Call the save service
    response = requests.post(
        "http://localhost:3003/save",
        json={
            "DATASET_ID": 2,
            "input_file": input_file,
            "output_file": output_file,
        },
    )
    response.raise_for_status()

    logger.info(f"Final data saved to {output_file}")

# Job
@job(
    resource_defs={"data_resources": data_resources},
    retry_policy=RetryPolicy(max_retries=1),
)
def medical_facility_accessibility_job():
    input_file = load_and_modify()
    geocoded_file = reconcile_geocoding(input_file)
    distance_pt_file = calculate_distance_to_public_transport(geocoded_file)
    distance_residential_file = calculate_distance_to_residential_areas(distance_pt_file)
    save_final_data(distance_residential_file)

if __name__ == "__main__":
    result = medical_facility_accessibility_job.execute_in_process(
        run_config={
            "resources": {
                "data_resources": {
                    "config": {
                        "data_dir": "/path/to/data",
                        "here_api_token": "your_here_api_token",
                    }
                }
            }
        }
    )