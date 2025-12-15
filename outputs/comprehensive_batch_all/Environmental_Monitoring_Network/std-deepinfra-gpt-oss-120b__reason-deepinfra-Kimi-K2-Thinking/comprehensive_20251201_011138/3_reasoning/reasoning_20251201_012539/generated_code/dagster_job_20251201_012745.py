import os
import json
import requests
from dagster import op, job, Config, In, Out, Nothing, get_dagster_logger
from typing import Dict, Any

logger = get_dagster_logger()

# Configuration class for pipeline parameters
class EnvironmentalPipelineConfig(Config):
    """Configuration for the environmental monitoring pipeline."""
    data_dir: str = "/data"
    dataset_id: int = 2
    here_api_token: str = "YOUR_HERE_API_TOKEN"
    geoapify_api_key: str = "YOUR_GEOAPIFY_API_KEY"
    openmeteo_base_url: str = "http://openmeteo-service:8080"
    # Service URLs
    load_modify_service_url: str = "http://localhost:3003"
    reconciliation_service_url: str = "http://localhost:3003"
    column_extension_service_url: str = "http://localhost:3003"
    save_service_url: str = "http://localhost:3003"
    land_use_service_url: str = "http://geoapify-land-use:8080"
    pop_density_service_url: str = "http://worldpop-density:8080"

# Op 1: Load & Modify Data
@op(
    description="Ingest station CSV, parse installation_date, standardize location names, convert to JSON"
)
def load_and_modify_data(context, config: EnvironmentalPipelineConfig) -> str:
    """Calls the load-and-modify service to process stations.csv"""
    input_file = os.path.join(config.data_dir, "stations.csv")
    output_file = os.path.join(config.data_dir, "table_data_2.json")
    
    # Check if input exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Prepare payload
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config.dataset_id,
        "date_column": "installation_date",
        "table_name_prefix": "JOT_"
    }
    
    try:
        response = requests.post(
            f"{config.load_modify_service_url}/load-and-modify",
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Load & Modify completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Load & Modify service failed: {e}")
        raise

# Op 2: Reconciliation (Geocoding)
@op(
    description="Geocode station locations using the location field"
)
def reconcile_geocoding(context, input_file: str, config: EnvironmentalPipelineConfig) -> str:
    """Calls the reconciliation service to geocode locations"""
    output_file = os.path.join(config.data_dir, "reconciled_table_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "primary_column": "location",
        "reconciliator_id": "geocodingHere",
        "api_token": config.here_api_token,
        "dataset_id": config.dataset_id
    }
    
    try:
        response = requests.post(
            f"{config.reconciliation_service_url}/reconciliation",
            json=payload,
            timeout=600
        )
        response.raise_for_status()
        logger.info(f"Reconciliation completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Reconciliation service failed: {e}")
        raise

# Op 3: OpenMeteo Data Extension
@op(
    description="Add historical weather data based on geocoded location and installation_date"
)
def extend_openmeteo_data(context, input_file: str, config: EnvironmentalPipelineConfig) -> str:
    """Calls OpenMeteo service to add weather data"""
    output_file = os.path.join(config.data_dir, "open_meteo_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "lat_column": "latitude",
        "lon_column": "longitude",
        "date_column": "installation_date",
        "weather_variables": "apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours",
        "date_separator_format": "YYYYMMDD"
    }
    
    try:
        response = requests.post(
            f"{config.openmeteo_base_url}/extend-weather",
            json=payload,
            timeout=600
        )
        response.raise_for_status()
        logger.info(f"OpenMeteo extension completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"OpenMeteo service failed: {e}")
        raise

# Op 4: Land Use Extension
@op(
    description="Add land use classification based on location using a GIS API"
)
def extend_land_use(context, input_file: str, config: EnvironmentalPipelineConfig) -> str:
    """Calls Geoapify land use service"""
    output_file = os.path.join(config.data_dir, "land_use_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "lat_column": "latitude",
        "lon_column": "longitude",
        "output_column": "land_use_type",
        "api_key": config.geoapify_api_key
    }
    
    try:
        response = requests.post(
            f"{config.land_use_service_url}/land-use",
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Land use extension completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Land use service failed: {e}")
        raise

# Op 5: Population Density Extension
@op(
    description="Add population density data for the area surrounding the station location"
)
def extend_population_density(context, input_file: str, config: EnvironmentalPipelineConfig) -> str:
    """Calls WorldPop density service"""
    output_file = os.path.join(config.data_dir, "pop_density_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "lat_column": "latitude",
        "lon_column": "longitude",
        "output_column": "population_density",
        "radius": 5000
    }
    
    try:
        response = requests.post(
            f"{config.pop_density_service_url}/population-density",
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Population density extension completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Population density service failed: {e}")
        raise

# Op 6: Environmental Calculation (Column Extension)
@op(
    description="Compute custom environmental risk factors based on combined data"
)
def calculate_environmental_risk(context, input_file: str, config: EnvironmentalPipelineConfig) -> str:
    """Calls column extension service for risk calculation"""
    output_file = os.path.join(config.data_dir, "column_extended_2.json")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "extender_id": "environmentalRiskCalculator",
        "input_columns": "precipitation_sum,population_density,land_use_type",
        "output_column": "risk_score",
        "calculation_formula": "standard_risk_calculation"  # Placeholder
    }
    
    try:
        response = requests.post(
            f"{config.column_extension_service_url}/column-extension",
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Environmental risk calculation completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Column extension service failed: {e}")
        raise

# Op 7: Save Final Data
@op(
    description="Export the comprehensive environmental dataset to CSV"
)
def save_final_data(context, input_file: str, config: EnvironmentalPipelineConfig) -> str:
    """Calls save service to export to CSV"""
    output_file = os.path.join(config.data_dir, "enriched_data_2.csv")
    
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config.dataset_id
    }
    
    try:
        response = requests.post(
            f"{config.save_service_url}/save",
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        logger.info(f"Save final data completed: {output_file}")
        return output_file
    except requests.exceptions.RequestException as e:
        logger.error(f"Save service failed: {e}")
        raise

# Define the job
@job(
    description="Environmental Monitoring Network Pipeline - Creates comprehensive dataset for environmental risk analysis"
)
def environmental_monitoring_pipeline():
    """Defines the pipeline dependency graph"""
    config = EnvironmentalPipelineConfig()
    
    # Chain the ops together
    step1_output = load_and_modify_data(config)
    step2_output = reconcile_geocoding(step1_output, config)
    step3_output = extend_openmeteo_data(step2_output, config)
    step4_output = extend_land_use(step3_output, config)
    step5_output = extend_population_density(step4_output, config)
    step6_output = calculate_environmental_risk(step5_output, config)
    save_final_data(step6_output, config)

# For local execution
if __name__ == '__main__':
    # Set up environment variables or default config
    os.environ.setdefault("DATA_DIR", "/tmp/environmental_data")
    os.environ.setdefault("HERE_API_TOKEN", "your_here_token")
    os.environ.setdefault("GEOAPIFY_API_KEY", "your_geoapify_key")
    
    # Create data directory if it doesn't exist
    data_dir = os.environ.get("DATA_DIR", "/tmp/environmental_data")
    os.makedirs(data_dir, exist_ok=True)
    
    # Execute the pipeline
    result = environmental_monitoring_pipeline.execute_in_process(
        run_config={
            "ops": {
                "load_and_modify_data": {
                    "config": {
                        "data_dir": data_dir,
                        "dataset_id": 2,
                        "here_api_token": os.environ.get("HERE_API_TOKEN"),
                        "geoapify_api_key": os.environ.get("GEOAPIFY_API_KEY")
                    }
                }
            }
        }
    )
    
    if result.success:
        logger.info("Pipeline executed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step failed: {event.step_key}")