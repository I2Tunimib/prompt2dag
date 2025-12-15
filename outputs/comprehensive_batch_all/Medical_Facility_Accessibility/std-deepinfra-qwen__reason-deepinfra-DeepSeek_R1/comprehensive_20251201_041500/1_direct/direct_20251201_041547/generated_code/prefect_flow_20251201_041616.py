from prefect import flow, task
import requests
import os
import json

DATA_DIR = os.getenv("DATA_DIR", "./data")

@task
def load_and_modify_data():
    """Load and modify the facility CSV data."""
    url = "http://localhost:3003/load-and-modify"
    params = {
        "DATASET_ID": 2,
        "TABLE_NAME_PREFIX": "JOT_"
    }
    files = {"file": open(os.path.join(DATA_DIR, "facilities.csv"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "table_data_2.json"), "w") as f:
        json.dump(response.json(), f)

@task
def reconcile_geocoding():
    """Geocode facility locations using the HERE API."""
    url = "http://localhost:3003/reconciliation"
    params = {
        "PRIMARY_COLUMN": "address",
        "RECONCILIATOR_ID": "geocodingHere",
        "API_TOKEN": os.getenv("HERE_API_TOKEN"),
        "DATASET_ID": 2
    }
    files = {"file": open(os.path.join(DATA_DIR, "table_data_2.json"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "reconciled_table_2.json"), "w") as f:
        json.dump(response.json(), f)

@task
def calculate_distance_to_public_transport():
    """Calculate the distance from each facility to the nearest public transport stop/hub."""
    url = "http://localhost:3003/column-extension"
    params = {
        "EXTENDER_ID": "spatialDistanceCalculator",
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "TARGET_LAYER": "public_transport",
        "TARGET_DATA_SOURCE": "/app/data/transport_stops.geojson",
        "OUTPUT_COLUMN": "distance_to_pt",
        "DATASET_ID": 2
    }
    files = {"file": open(os.path.join(DATA_DIR, "reconciled_table_2.json"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "distance_pt_2.json"), "w") as f:
        json.dump(response.json(), f)

@task
def calculate_distance_to_residential_areas():
    """Calculate the distance from each facility to the nearest residential area center/boundary."""
    url = "http://localhost:3003/column-extension"
    params = {
        "EXTENDER_ID": "spatialDistanceCalculator",
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "TARGET_LAYER": "residential_areas",
        "TARGET_DATA_SOURCE": "/app/data/residential_areas.geojson",
        "OUTPUT_COLUMN": "distance_to_residential",
        "DATASET_ID": 2
    }
    files = {"file": open(os.path.join(DATA_DIR, "distance_pt_2.json"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "column_extended_2.json"), "w") as f:
        json.dump(response.json(), f)

@task
def save_final_data():
    """Export the facility accessibility data to CSV."""
    url = "http://localhost:3003/save"
    params = {
        "DATASET_ID": 2
    }
    files = {"file": open(os.path.join(DATA_DIR, "column_extended_2.json"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "enriched_data_2.csv"), "w") as f:
        f.write(response.text)

@flow
def medical_facility_accessibility_pipeline():
    """Orchestrates the medical facility accessibility pipeline."""
    load_and_modify_data()
    reconcile_geocoding()
    calculate_distance_to_public_transport()
    calculate_distance_to_residential_areas()
    save_final_data()

if __name__ == "__main__":
    medical_facility_accessibility_pipeline()