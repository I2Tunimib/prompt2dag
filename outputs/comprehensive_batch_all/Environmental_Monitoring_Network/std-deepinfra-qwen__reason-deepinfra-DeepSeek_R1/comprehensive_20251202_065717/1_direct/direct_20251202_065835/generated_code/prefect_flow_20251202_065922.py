from prefect import flow, task
import requests
import json
import os

DATA_DIR = os.getenv("DATA_DIR", "./data")

@task
def load_and_modify_data():
    """Ingest station CSV, parse installation_date, standardize location names, convert to JSON."""
    url = "http://localhost:3003/load-and-modify"
    params = {
        "DATASET_ID": 2,
        "DATE_COLUMN": "installation_date",
        "TABLE_NAME_PREFIX": "JOT_"
    }
    files = {"file": open(f"{DATA_DIR}/stations.csv", "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/table_data_2.json", "w") as f:
        json.dump(response.json(), f)
    return f"{DATA_DIR}/table_data_2.json"

@task
def reconcile_geocoding(input_file):
    """Geocode station locations using the location field."""
    url = "http://localhost:3003/reconciliation"
    params = {
        "PRIMARY_COLUMN": "location",
        "RECONCILIATOR_ID": "geocodingHere",
        "API_TOKEN": os.getenv("HERE_API_TOKEN"),
        "DATASET_ID": 2
    }
    files = {"file": open(input_file, "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/reconciled_table_2.json", "w") as f:
        json.dump(response.json(), f)
    return f"{DATA_DIR}/reconciled_table_2.json"

@task
def extend_open_meteo_data(input_file):
    """Add historical weather data based on geocoded location and installation_date."""
    url = "http://localhost:3003/openmeteo-extension"
    params = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "DATE_COLUMN": "installation_date",
        "WEATHER_VARIABLES": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
        "DATE_SEPARATOR_FORMAT": "YYYYMMDD"
    }
    files = {"file": open(input_file, "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/open_meteo_2.json", "w") as f:
        json.dump(response.json(), f)
    return f"{DATA_DIR}/open_meteo_2.json"

@task
def extend_land_use_data(input_file):
    """Add land use classification based on location using a GIS API."""
    url = "http://localhost:3003/land-use-extension"
    params = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "OUTPUT_COLUMN": "land_use_type",
        "API_KEY": os.getenv("GEOAPIFY_API_KEY")
    }
    files = {"file": open(input_file, "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/land_use_2.json", "w") as f:
        json.dump(response.json(), f)
    return f"{DATA_DIR}/land_use_2.json"

@task
def extend_population_density_data(input_file):
    """Add population density data for the area surrounding the station location."""
    url = "http://localhost:3003/population-density-extension"
    params = {
        "LAT_COLUMN": "latitude",
        "LON_COLUMN": "longitude",
        "OUTPUT_COLUMN": "population_density",
        "RADIUS": 5000
    }
    files = {"file": open(input_file, "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/pop_density_2.json", "w") as f:
        json.dump(response.json(), f)
    return f"{DATA_DIR}/pop_density_2.json"

@task
def extend_environmental_risk_data(input_file):
    """Compute custom environmental risk factors based on combined data."""
    url = "http://localhost:3003/column-extension"
    params = {
        "EXTENDER_ID": "environmentalRiskCalculator",
        "INPUT_COLUMNS": "precipitation_sum,population_density,land_use_type",
        "OUTPUT_COLUMN": "risk_score",
        "CALCULATION_FORMULA": os.getenv("RISK_CALCULATION_PARAMETERS")
    }
    files = {"file": open(input_file, "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/column_extended_2.json", "w") as f:
        json.dump(response.json(), f)
    return f"{DATA_DIR}/column_extended_2.json"

@task
def save_final_data(input_file):
    """Export the comprehensive environmental dataset to CSV."""
    url = "http://localhost:3003/save"
    params = {
        "DATASET_ID": 2
    }
    files = {"file": open(input_file, "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(f"{DATA_DIR}/enriched_data_2.csv", "w") as f:
        f.write(response.text)
    return f"{DATA_DIR}/enriched_data_2.csv"

@flow
def environmental_monitoring_pipeline():
    """Orchestrates the environmental monitoring data pipeline."""
    table_data = load_and_modify_data()
    reconciled_table = reconcile_geocoding(table_data)
    open_meteo_data = extend_open_meteo_data(reconciled_table)
    land_use_data = extend_land_use_data(open_meteo_data)
    pop_density_data = extend_population_density_data(land_use_data)
    column_extended_data = extend_environmental_risk_data(pop_density_data)
    final_data = save_final_data(column_extended_data)
    return final_data

if __name__ == "__main__":
    environmental_monitoring_pipeline()