from prefect import flow, task, get_run_logger
import os
import requests

DATA_DIR = os.getenv("DATA_DIR", "./data")

@task
def load_and_modify_data():
    logger = get_run_logger()
    url = "http://localhost:3003/load-and-modify"
    params = {
        "DATASET_ID": 2,
        "TABLE_NAME_PREFIX": "JOT_",
        "INPUT_FILE": f"{DATA_DIR}/facilities.csv",
        "OUTPUT_FILE": f"{DATA_DIR}/table_data_2.json"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    logger.info("Data loaded and modified successfully.")
    return params["OUTPUT_FILE"]

@task
def reconcile_geocoding(input_file):
    logger = get_run_logger()
    url = "http://localhost:3003/reconciliation"
    params = {
        "DATASET_ID": 2,
        "PRIMARY_COLUMN": "address",
        "RECONCILIATOR_ID": "geocodingHere",
        "API_TOKEN": os.getenv("HERE_API_TOKEN"),
        "INPUT_FILE": input_file,
        "OUTPUT_FILE": f"{DATA_DIR}/reconciled_table_2.json"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    logger.info("Geocoding completed successfully.")
    return params["OUTPUT_FILE"]

@task
def calculate_distance_to_public_transport(input_file):
    logger = get_run_logger()
    url = "http://localhost:3003/column-extension"
    params = {
        "DATASET_ID": 2,
        "EXTENDER_ID": "spatialDistanceCalculator",
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "TARGET_LAYER": "public_transport",
        "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
        "OUTPUT_COLUMN": "distance_to_pt",
        "INPUT_FILE": input_file,
        "OUTPUT_FILE": f"{DATA_DIR}/distance_pt_2.json"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    logger.info("Distance to public transport calculated successfully.")
    return params["OUTPUT_FILE"]

@task
def calculate_distance_to_residential_areas(input_file):
    logger = get_run_logger()
    url = "http://localhost:3003/column-extension"
    params = {
        "DATASET_ID": 2,
        "EXTENDER_ID": "spatialDistanceCalculator",
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "TARGET_LAYER": "residential_areas",
        "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
        "OUTPUT_COLUMN": "distance_to_residential",
        "INPUT_FILE": input_file,
        "OUTPUT_FILE": f"{DATA_DIR}/column_extended_2.json"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    logger.info("Distance to residential areas calculated successfully.")
    return params["OUTPUT_FILE"]

@task
def save_final_data(input_file):
    logger = get_run_logger()
    url = "http://localhost:3003/save"
    params = {
        "DATASET_ID": 2,
        "INPUT_FILE": input_file,
        "OUTPUT_FILE": f"{DATA_DIR}/enriched_data_2.csv"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    logger.info("Final data saved successfully.")
    return params["OUTPUT_FILE"]

@flow
def medical_facility_accessibility_pipeline():
    logger = get_run_logger()
    logger.info("Starting Medical Facility Accessibility Pipeline.")
    
    modified_data_file = load_and_modify_data()
    reconciled_data_file = reconcile_geocoding(modified_data_file)
    distance_to_pt_file = calculate_distance_to_public_transport(reconciled_data_file)
    distance_to_residential_file = calculate_distance_to_residential_areas(distance_to_pt_file)
    final_data_file = save_final_data(distance_to_residential_file)
    
    logger.info(f"Pipeline completed. Final data saved to {final_data_file}.")

if __name__ == "__main__":
    medical_facility_accessibility_pipeline()