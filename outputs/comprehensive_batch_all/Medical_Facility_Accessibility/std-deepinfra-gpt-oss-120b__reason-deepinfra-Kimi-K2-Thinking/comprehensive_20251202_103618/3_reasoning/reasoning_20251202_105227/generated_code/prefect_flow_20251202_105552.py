import os
import httpx
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

# Constants from pipeline description
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
API_BASE_URL = "http://localhost:3003"  # Service port 3003
DATASET_ID = 2
TABLE_NAME_PREFIX = "JOT_"
PRIMARY_COLUMN = "address"
RECONCILIATOR_ID = "geocodingHere"
EXTENDER_ID = "spatialDistanceCalculator"
LAT_COLUMN = "latitude"
LON_COLUMN = "longitude"
TARGET_LAYER_PT = "public_transport"
TARGET_LAYER_RES = "residential_areas"
OUTPUT_COLUMN_PT = "distance_to_pt"
OUTPUT_COLUMN_RES = "distance_to_residential"

# Task 1: Load & Modify Data
@task(
    name="load_and_modify_data",
    retries=1,
    retry_delay_seconds=30,
    task_run_name="load_and_modify_data_{dataset_id}"
)
async def load_and_modify_data(dataset_id: int, data_dir: str, table_name_prefix: str):
    """Load facility CSV, clean addresses, convert to JSON"""
    # This would call the load-and-modify service
    # For now, simulate with a placeholder API call
    pass

# Task 2: Reconciliation (Geocoding)
@task(
    name="reconcile_geocoding",
    retries=1,
    retry_delay_seconds=30,
    task_run_name="reconcile_geocoding_{dataset_id}"
)
async def reconcile_geocoding(dataset_id: int, data_dir: str, api_token: str):
    """Geocode facility locations using HERE API"""
    pass

# Task 3: Distance Calculation (Public Transport)
@task(
    name="calculate_distance_pt",
    retries=1,
    retry_delay_seconds=30,
    task_run_name="calculate_distance_pt_{dataset_id}"
)
async def calculate_distance_pt(dataset_id: int, data_dir: str):
    """Calculate distance to nearest public transport"""
    pass

# Task 4: Distance Calculation (Residential Areas)
@task(
    name="calculate_distance_residential",
    retries=1,
    retry_delay_seconds=30,
    task_run_name="calculate_distance_residential_{dataset_id}"
)
async def calculate_distance_residential(dataset_id: int, data_dir: str):
    """Calculate distance to nearest residential area"""
    pass

# Task 5: Save Final Data
@task(
    name="save_final_data",
    retries=1,
    retry_delay_seconds=30,
    task_run_name="save_final_data_{dataset_id}"
)
async def save_final_data(dataset_id: int, data_dir: str):
    """Export final data to CSV"""
    pass

# Main Flow
@flow(
    name="medical-facility-accessibility-pipeline",
    description="Assesses medical facility accessibility by geocoding and calculating distances",
    task_runner=ConcurrentTaskRunner()
)
async def medical_facility_accessibility_pipeline(
    dataset_id: int = DATASET_ID,
    data_dir: str = DATA_DIR,
    table_name_prefix: str = TABLE_NAME_PREFIX,
    here_api_token: str = None
):
    """Main flow for medical facility accessibility assessment"""
    # Step 1: Load & Modify Data
    await load_and_modify_data(dataset_id, data_dir, table_name_prefix)
    
    # Step 2: Reconciliation (Geocoding)
    await reconcile_geocoding(dataset_id, data_dir, here_api_token)
    
    # Steps 3 & 4: Run in parallel
    pt_task = calculate_distance_pt.submit(dataset_id, data_dir)
    res_task = calculate_distance_residential.submit(dataset_id, data_dir)
    
    # Wait for both to complete
    pt_task.wait()
    res_task.wait()
    
    # Step 5: Save Final Data
    await save_final_data(dataset_id, data_dir)

if __name__ == "__main__":
    # For local execution
    import asyncio
    asyncio.run(medical_facility_accessibility_pipeline())