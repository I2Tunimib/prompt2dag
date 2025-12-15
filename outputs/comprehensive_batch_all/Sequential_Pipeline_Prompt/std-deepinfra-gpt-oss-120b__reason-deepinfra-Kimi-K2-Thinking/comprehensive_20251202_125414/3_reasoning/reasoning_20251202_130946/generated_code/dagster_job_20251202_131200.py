from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    RetryPolicy,
    resource,
    make_values_resource,
    In,
    Out,
    String,
    Int,
    Noneable,
)
from typing import Any, Dict
import os
import glob
import json
import logging

# Simulated Docker API resource
class DockerApiResource:
    def __init__(self, base_url: str = "http://localhost:3003"):
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)

    def call_endpoint(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate calling a Docker service endpoint."""
        self.logger.info(f"Calling {self.base_url}{endpoint} with data: {data}")
        # In real implementation, this would make HTTP requests to the Docker service
        return {"status": "success", "message": f"Processed by {endpoint}"}

# Resource definition
@resource(config_schema={"base_url": String})
def docker_api_resource(context):
    return DockerApiResource(base_url=context.resource_config["base_url"])

# Shared configuration resource
shared_config = make_values_resource(
    data_dir=str,
    api_token=str,
    dataset_id=Int,
    date_column=str,
    table_prefix=str,
    primary_column=str,
    optional_columns=list,
    reconciliator_id=str,
    weather_attributes=list,
    date_separator=str,
    extender_id=str,
    final_output_dir=str,
)

# Op Configs
class LoadAndModifyConfig(Config):
    dataset_id: int = 2
    date_column: str = "Fecha_id"
    table_prefix: str = "JOT_{}"

class ReconciliationConfig(Config):
    primary_column: str = "City"
    optional_columns: list[str] = ["County", "Country"]
    reconciliator_id: str = "geocodingHere"

class OpenMeteoConfig(Config):
    weather_attributes: list[str] = ["apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "precipitation_hours"]
    date_separator: str = "-"

class ColumnExtensionConfig(Config):
    extender_id: str = "reconciledColumnExt"

class SaveFinalDataConfig(Config):
    output_filename: str = "enriched_data_{}.csv"

# Op 1: Load and Modify Data
@op(
    required_resource_keys={"docker_api", "shared_config"},
    config_schema=LoadAndModifyConfig,
    out=Out(String),
    retry_policy=RetryPolicy(max_retries=1),
    tags={"docker_image": "i2t-backendwithintertwino6-load-and-modify:latest"},
)
def load_and_modify_data(context: OpExecutionContext, config: LoadAndModifyConfig) -> str:
    """
    Ingest CSV files from DATA_DIR and convert them to JSON format.
    Calls the load-and-modify service (port 3003).
    """
    data_dir = context.resources.shared_config["data_dir"]
    dataset_id = config.dataset_id or context.resources.shared_config.get("dataset_id", 2)
    date_column = config.date_column or context.resources.shared_config.get("date_column", "Fecha_id")
    table_prefix = config.table_prefix or context.resources.shared_config.get("table_prefix", "JOT_{}")
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    # Simulate API call
    payload = {
        "data_dir": data_dir,
        "dataset_id": dataset_id,
        "date_column": date_column,
        "table_prefix": table_prefix,
        "input_files": csv_files,
    }
    
    result = context.resources.docker_api.call_endpoint("/load-and-modify", payload)
    
    # Generate output file path (simulating the service would create this)
    output_file = os.path.join(data_dir, f"table_data_{dataset_id}.json")
    
    # In real implementation, the service would create this file
    # For simulation, we'll create a placeholder
    with open(output_file, 'w') as f:
        json.dump({"step": "load_and_modify", "input_files": csv_files, "status": "processed"}, f)
    
    context.log.info(f"Load and modify completed. Output: {output_file}")
    return output_file

# Op 2: Data Reconciliation
@op(
    required_resource_keys={"docker_api", "shared_config"},
    config_schema=ReconciliationConfig,
    ins={"input_file": In(String)},
    out=Out(String),
    retry_policy=RetryPolicy(max_retries=1),
    tags={"docker_image": "i2t-backendwithintertwino6-reconciliation:latest"},
)
def reconcile_data(context: OpExecutionContext, input_file: str, config: ReconciliationConfig) -> str:
    """
    Standardize and reconcile city names using HERE geocoding service.
    """
    data_dir = context.resources.shared_config["data_dir"]
    api_token = context.resources.shared_config["api_token"]
    primary_column = config.primary_column or context.resources.shared_config.get("primary_column", "City")
    optional_columns = config.optional_columns or context.resources.shared_config.get("optional_columns", ["County", "Country"])
    reconciliator_id = config.reconciliator_id or context.resources.shared_config.get("reconciliator_id", "geocodingHere")
    
    # Extract dataset_id from input filename
    dataset_id = os.path.basename(input_file).split('_')[-1].replace('.json', '')
    
    payload = {
        "input_file": input_file,
        "api_token": api_token,
        "primary_column": primary_column,
        "optional_columns": optional_columns,
        "reconciliator_id": reconciliator_id,
    }
    
    result = context.resources.docker_api.call_endpoint("/reconciliation", payload)
    
    output_file = os.path.join(data_dir, f"reconciled_table_{dataset_id}.json")
    
    # Simulate file creation
    with open(output_file, 'w') as f:
        json.dump({"step": "reconciliation", "input_file": input_file, "status": "reconciled"}, f)
    
    context.log.info(f"Reconciliation completed. Output: {output_file}")
    return output_file

# Op 3: OpenMeteo Data Extension
@op(
    required_resource_keys={"docker_api", "shared_config"},
    config_schema=OpenMeteoConfig,
    ins={"input_file": In(String)},
    out=Out(String),
    retry_policy=RetryPolicy(max_retries=1),
    tags={"docker_image": "i2t-backendwithintertwino6-openmeteo-extension:latest"},
)
def extend_openmeteo_data(context: OpExecutionContext, input_file: str, config: OpenMeteoConfig) -> str:
    """
    Enrich dataset with weather information from OpenMeteo.
    """
    data_dir = context.resources.shared_config["data_dir"]
    weather_attributes = config.weather_attributes or context.resources.shared_config.get("weather_attributes", 
        ["apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "precipitation_hours"])
    date_separator = config.date_separator or context.resources.shared_config.get("date_separator", "-")
    
    dataset_id = os.path.basename(input_file).split('_')[-1].replace('.json', '')
    
    payload = {
        "input_file": input_file,
        "weather_attributes": weather_attributes,
        "date_separator": date_separator,
    }
    
    result = context.resources.docker_api.call_endpoint("/openmeteo-extension", payload)
    
    output_file = os.path.join(data_dir, f"open_meteo_{dataset_id}.json")
    
    # Simulate file creation
    with open(output_file, 'w') as f:
        json.dump({"step": "openmeteo", "input_file": input_file, "status": "extended"}, f)
    
    context.log.info(f"OpenMeteo extension completed. Output: {output_file}")
    return output_file

# Op 4: Column Extension
@op(
    required_resource_keys={"docker_api", "shared_config"},
    config_schema=ColumnExtensionConfig,
    ins={"input_file": In(String)},
    out=Out(String),
    retry_policy=RetryPolicy(max_retries=1),
    tags={"docker_image": "i2t-backendwithintertwino6-column-extension:latest"},
)
def extend_columns(context: OpExecutionContext, input_file: str, config: ColumnExtensionConfig) -> str:
    """
    Append additional data properties to the dataset.
    """
    data_dir = context.resources.shared_config["data_dir"]
    extender_id = config.extender_id or context.resources.shared_config.get("extender_id", "reconciledColumnExt")
    
    dataset_id = os.path.basename(input_file).split('_')[-1].replace('.json', '')
    
    payload = {
        "input_file": input_file,
        "extender_id": extender_id,
    }
    
    result = context.resources.docker_api.call_endpoint("/column-extension", payload)
    
    output_file = os.path.join(data_dir, f"column_extended_{dataset_id}.json")
    
    # Simulate file creation
    with open(output_file, 'w') as f:
        json.dump({"step": "column_extension", "input_file": input_file, "status": "extended"}, f)
    
    context.log.info(f"Column extension completed. Output: {output_file}")
    return output_file

# Op 5: Save Final Data
@op(
    required_resource_keys={"docker_api", "shared_config"},
    config_schema=SaveFinalDataConfig,
    ins={"input_file": In(String)},
    out=Out(String),
    retry_policy=RetryPolicy(max_retries=1),
    tags={"docker_image": "i2t-backendwithintertwino6-save:latest"},
)
def save_final_data(context: OpExecutionContext, input_file: str, config: SaveFinalDataConfig) -> str:
    """
    Consolidate and export the fully enriched dataset as CSV.
    """
    data_dir = context.resources.shared_config["data_dir"]
    final_output_dir = context.resources.shared_config.get("final_output_dir", data_dir)
    output_filename = config.output_filename or "enriched_data_{}.csv"
    
    dataset_id = os.path.basename(input_file).split('_')[-1].replace('.json', '')
    
    payload = {
        "input_file": input_file,
        "output_dir": final_output_dir,
        "output_filename": output_filename.format(dataset_id),
    }
    
    result = context.resources.docker_api.call_endpoint("/save", payload)
    
    output_file = os.path.join(final_output_dir, output_filename.format(dataset_id))
    
    # Simulate file creation
    with open(output_file, 'w') as f:
        f.write("id,name,city,weather_data\n")  # Minimal CSV header
        f.write("1,Test,SampleCity,25.5\n")
    
    context.log.info(f"Final data saved. Output: {output_file}")
    return output_file

# Job Definition
@job(
    resource_defs={
        "docker_api": docker_api_resource,
        "shared_config": shared_config,
    },
    description="Sequential data processing pipeline that transforms raw CSV data through enrichment steps.",
)
def data_enrichment_pipeline():
    """
    End-to-end data enrichment pipeline.
    
    Steps:
    1. Load and modify CSV data
    2. Reconcile city names with geocoding
    3. Extend with OpenMeteo weather data
    4. Append additional columns
    5. Save final enriched CSV
    """
    # Chain the operations sequentially
    reconciled = reconcile_data(load_and_modify_data())
    openmeteo_extended = extend_openmeteo_data(reconciled)
    column_extended = extend_columns(openmeteo_extended)
    save_final_data(column_extended)

# Launch pattern
if __name__ == '__main__':
    # Example configuration for local execution
    run_config = {
        "resources": {
            "docker_api": {
                "config": {
                    "base_url": "http://localhost:3003"
                }
            },
            "shared_config": {
                "config": {
                    "data_dir": "/tmp/data",
                    "api_token": "your_here_api_token",
                    "dataset_id": 2,
                    "date_column": "Fecha_id",
                    "table_prefix": "JOT_{}",
                    "primary_column": "City",
                    "optional_columns": ["County", "Country"],
                    "reconciliator_id": "geocodingHere",
                    "weather_attributes": [
                        "apparent_temperature_max",
                        "apparent_temperature_min",
                        "precipitation_sum",
                        "precipitation_hours"
                    ],
                    "date_separator": "-",
                    "extender_id": "reconciledColumnExt",
                    "final_output_dir": "/tmp/data",
                }
            }
        },
        "ops": {
            "load_and_modify_data": {
                "config": {
                    "dataset_id": 2,
                    "date_column": "Fecha_id",
                    "table_prefix": "JOT_{}"
                }
            },
            "reconcile_data": {
                "config": {
                    "primary_column": "City",
                    "optional_columns": ["County", "Country"],
                    "reconciliator_id": "geocodingHere"
                }
            },
            "extend_openmeteo_data": {
                "config": {
                    "weather_attributes": [
                        "apparent_temperature_max",
                        "apparent_temperature_min",
                        "precipitation_sum",
                        "precipitation_hours"
                    ],
                    "date_separator": "-"
                }
            },
            "extend_columns": {
                "config": {
                    "extender_id": "reconciledColumnExt"
                }
            },
            "save_final_data": {
                "config": {
                    "output_filename": "enriched_data_{}.csv"
                }
            }
        }
    }
    
    # Ensure data directory exists
    os.makedirs("/tmp/data", exist_ok=True)
    
    # Create a sample CSV for testing
    sample_csv = "/tmp/data/sample.csv"
    with open(sample_csv, 'w') as f:
        f.write("Fecha_id,City,County,Country,value\n")
        f.write("2023-01-01,New York,New York,USA,100\n")
        f.write("2023-01-02,Los Angeles,Los Angeles,USA,200\n")
    
    # Execute the pipeline
    result = data_enrichment_pipeline.execute_in_process(run_config=run_config)
    
    if result.success:
        print("Pipeline executed successfully!")
        print(f"Output files created in /tmp/data")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")