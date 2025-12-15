import os
import requests
from typing import Any, Dict

from dagster import op, job, RetryPolicy, OpExecutionContext

DATA_DIR = os.environ.get("DATA_DIR", "./data")
BASE_URL = "http://localhost:3003"


def _make_request(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to make POST requests to services."""
    try:
        response = requests.post(f"{BASE_URL}{endpoint}", json=payload, timeout=300)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"API request failed: {e}")


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Load station CSV, parse dates, standardize names, convert to JSON"
)
def load_and_modify_data(context: OpExecutionContext) -> str:
    """Step 1: Load & Modify Data"""
    input_file = os.path.join(DATA_DIR, "stations.csv")
    output_file = os.path.join(DATA_DIR, "table_data_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": 2,
        "date_column": "installation_date",
        "table_name_prefix": "JOT_"
    }
    
    context.log.info(f"Processing {input_file} -> {output_file}")
    _make_request("/load-and-modify", payload)
    return output_file


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Geocode station locations using location field"
)
def reconciliation_geocoding(context: OpExecutionContext, input_file: str) -> str:
    """Step 2: Reconciliation (Geocoding)"""
    output_file = os.path.join(DATA_DIR, "reconciled_table_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "primary_column": "location",
        "reconciliator_id": "geocodingHere",
        "api_token": os.environ.get("HERE_API_TOKEN", "your_here_api_token"),
        "dataset_id": 2
    }
    
    context.log.info(f"Geocoding {input_file} -> {output_file}")
    _make_request("/reconciliation", payload)
    return output_file


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Add historical weather data based on geocoded location"
)
def open_meteo_extension(context: OpExecutionContext, input_file: str) -> str:
    """Step 3: OpenMeteo Data Extension"""
    output_file = os.path.join(DATA_DIR, "open_meteo_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "lat_column": "latitude",
        "lon_column": "longitude",
        "date_column": "installation_date",
        "weather_variables": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
        "date_separator_format": "YYYYMMDD"
    }
    
    context.log.info(f"Fetching weather data {input_file} -> {output_file}")
    _make_request("/openmeteo-extension", payload)
    return output_file


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Add land use classification based on location"
)
def land_use_extension(context: OpExecutionContext, input_file: str) -> str:
    """Step 4: Land Use Extension"""
    output_file = os.path.join(DATA_DIR, "land_use_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "lat_column": "latitude",
        "lon_column": "longitude",
        "output_column": "land_use_type",
        "api_key": os.environ.get("GEOAPIFY_API_KEY", "your_geoapify_api_key")
    }
    
    context.log.info(f"Fetching land use data {input_file} -> {output_file}")
    _make_request("/land-use-extension", payload)
    return output_file


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Add population density data for station locations"
)
def population_density_extension(context: OpExecutionContext, input_file: str) -> str:
    """Step 5: Population Density Extension"""
    output_file = os.path.join(DATA_DIR, "pop_density_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "lat_column": "latitude",
        "lon_column": "longitude",
        "output_column": "population_density",
        "radius": 5000
    }
    
    context.log.info(f"Fetching population density {input_file} -> {output_file}")
    _make_request("/population-density-extension", payload)
    return output_file


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Compute custom environmental risk factors"
)
def environmental_calculation(context: OpExecutionContext, input_file: str) -> str:
    """Step 6: Environmental Calculation (Column Extension)"""
    output_file = os.path.join(DATA_DIR, "column_extended_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "extender_id": "environmentalRiskCalculator",
        "input_columns": "precipitation_sum,population_density,land_use_type",
        "output_column": "risk_score",
        "calculation_formula": os.environ.get("RISK_CALCULATION_FORMULA", "default_risk_calculation")
    }
    
    context.log.info(f"Calculating risk scores {input_file} -> {output_file}")
    _make_request("/column-extension", payload)
    return output_file


@op(
    retry_policy=RetryPolicy(max_retries=1),
    description="Export final dataset to CSV"
)
def save_final_data(context: OpExecutionContext, input_file: str) -> str:
    """Step 7: Save Final Data"""
    output_file = os.path.join(DATA_DIR, "enriched_data_2.csv")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": 2
    }
    
    context.log.info(f"Saving final data {input_file} -> {output_file}")
    _make_request("/save", payload)
    return output_file


@job(
    description="Environmental Monitoring Network Pipeline - Creates comprehensive dataset for environmental risk analysis"
)
def environmental_monitoring_pipeline():
    """Dagster job that orchestrates the environmental monitoring data pipeline."""
    save_final_data(
        environmental_calculation(
            population_density_extension(
                land_use_extension(
                    open_meteo_extension(
                        reconciliation_geocoding(
                            load_and_modify_data()
                        )
                    )
                )
            )
        )
    )


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    result = environmental_monitoring_pipeline.execute_in_process()
    
    if result.success:
        print("Pipeline executed successfully!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")