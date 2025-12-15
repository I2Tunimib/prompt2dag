import os
import json
from typing import Any, Dict
from dagster import (
    op,
    job,
    resource,
    RetryPolicy,
    OpExecutionContext,
    get_dagster_logger,
)


@resource
def pipeline_config(init_context) -> Dict[str, Any]:
    """Resource providing pipeline configuration parameters."""
    return {
        "data_dir": os.getenv("DATA_DIR", "/app/data"),
        "dataset_id": 2,
        "table_name_prefix": "JOT_",
        "primary_column": "address",
        "reconciliator_id": "geocodingHere",
        "extender_id": "spatialDistanceCalculator",
        "lat_column": "latitude",
        "lon_column": "longitude",
        "api_token": os.getenv("HERE_API_TOKEN", "dummy-token"),
        "services_base_url": os.getenv("SERVICES_BASE_URL", "http://localhost:3003"),
    }


def _simulate_service_call(
    endpoint: str, payload: Dict[str, Any], context: OpExecutionContext
) -> Dict[str, Any]:
    """Simulate Docker service API call by creating dummy output files."""
    logger = get_dagster_logger()
    config = context.resources.pipeline_config
    output_file = payload.get("output_file")

    if output_file:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(
                {
                    "simulated": True,
                    "endpoint": endpoint,
                    "dataset_id": config["dataset_id"],
                    "processed_rows": 100,
                },
                f,
            )
        logger.info(f"Simulated service call to {endpoint}, created {output_file}")

    return {"status": "success", "output_file": output_file}


@op(
    retry_policy=RetryPolicy(max_retries=1),
    required_resource_keys={"pipeline_config"},
)
def load_and_modify_data(context: OpExecutionContext) -> str:
    """Load facility CSV, clean addresses, and convert to JSON."""
    config = context.resources.pipeline_config
    logger = get_dagster_logger()

    input_file = os.path.join(config["data_dir"], "facilities.csv")
    output_file = os.path.join(config["data_dir"], "table_data_2.json")

    if not os.path.exists(input_file):
        logger.warning(f"Input file {input_file} not found, creating dummy")
        os.makedirs(config["data_dir"], exist_ok=True)
        with open(input_file, "w") as f:
            f.write("id,name,address\n1,Test Facility,123 Test St")

    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config["dataset_id"],
        "table_name_prefix": config["table_name_prefix"],
    }

    result = _simulate_service_call("/load-and-modify", payload, context)
    return result["output_file"]


@op(
    retry_policy=RetryPolicy(max_retries=1),
    required_resource_keys={"pipeline_config"},
)
def reconcile_geocoding(context: OpExecutionContext, input_file: str) -> str:
    """Geocode facility locations using HERE API."""
    config = context.resources.pipeline_config
    logger = get_dagster_logger()

    output_file = os.path.join(config["data_dir"], "reconciled_table_2.json")
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "primary_column": config["primary_column"],
        "reconciliator_id": config["reconciliator_id"],
        "api_token": config["api_token"],
        "dataset_id": config["dataset_id"],
    }

    result = _simulate_service_call("/reconciliation", payload, context)
    return result["output_file"]


@op(
    retry_policy=RetryPolicy(max_retries=1),
    required_resource_keys={"pipeline_config"},
)
def calculate_distance_public_transport(
    context: OpExecutionContext, input_file: str
) -> str:
    """Calculate distance to nearest public transport."""
    config = context.resources.pipeline_config
    logger = get_dagster_logger()

    output_file = os.path.join(config["data_dir"], "distance_pt_2.json")
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "extender_id": config["extender_id"],
        "lat_column": config["lat_column"],
        "lon_column": config["lon_column"],
        "target_layer": "public_transport",
        "target_data_source": "/app/data/transport_stops.geojson",
        "output_column": "distance_to_pt",
        "dataset_id": config["dataset_id"],
    }

    result = _simulate_service_call("/column-extension", payload, context)
    return result["output_file"]


@op(
    retry_policy=RetryPolicy(max_retries=1),
    required_resource_keys={"pipeline_config"},
)
def calculate_distance_residential(
    context: OpExecutionContext, input_file: str
) -> str:
    """Calculate distance to nearest residential area."""
    config = context.resources.pipeline_config
    logger = get_dagster_logger()

    output_file = os.path.join(config["data_dir"], "column_extended_2.json")
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "extender_id": config["extender_id"],
        "lat_column": config["lat_column"],
        "lon_column": config["lon_column"],
        "target_layer": "residential_areas",
        "target_data_source": "/app/data/residential_areas.geojson",
        "output_column": "distance_to_residential",
        "dataset_id": config["dataset_id"],
    }

    result = _simulate_service_call("/column-extension", payload, context)
    return result["output_file"]


@op(
    retry_policy=RetryPolicy(max_retries=1),
    required_resource_keys={"pipeline_config"},
)
def save_final_data(context: OpExecutionContext, input_file: str) -> str:
    """Export final accessibility data to CSV."""
    config = context.resources.pipeline_config
    logger = get_dagster_logger()

    output_file = os.path.join(config["data_dir"], "enriched_data_2.csv")
    payload = {
        "input_file": input_file,
        "output_file": output_file,
        "dataset_id": config["dataset_id"],
    }

    result = _simulate_service_call("/save", payload, context)
    return result["output_file"]


@job(
    resource_defs={"pipeline_config": pipeline_config},
    description="Medical facility accessibility assessment pipeline",
)
def medical_facility_accessibility_pipeline():
    """Define the pipeline dependency graph."""
    step1_output = load_and_modify_data()
    step2_output = reconcile_geocoding(step1_output)
    step3_output = calculate_distance_public_transport(step2_output)
    step4_output = calculate_distance_residential(step3_output)
    save_final_data(step4_output)


if __name__ == "__main__":
    result = medical_facility_accessibility_pipeline.execute_in_process()
    if result.success:
        print("✅ Pipeline execution successful!")
        print(f"Final output: {result.output_for_node('save_final_data')}")
    else:
        print("❌ Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")