from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

# Download tasks
@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_noaa():
    logger = get_run_logger()
    logger.info("Downloading NOAA data...")
    # Simulate data download
    data = pd.DataFrame({"timestamp": ["2023-01-01T00:00:00"], "temperature": [10.0], "precipitation": [0.0]})
    data.to_csv("noaa_data.csv", index=False)
    return "noaa_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_ecmwf():
    logger = get_run_logger()
    logger.info("Downloading ECMWF data...")
    # Simulate data download
    data = pd.DataFrame({"timestamp": ["2023-01-01T00:00:00"], "temperature": [15.0], "precipitation": [0.0]})
    data.to_csv("ecmwf_data.csv", index=False)
    return "ecmwf_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_jma():
    logger = get_run_logger()
    logger.info("Downloading JMA data...")
    # Simulate data download
    data = pd.DataFrame({"timestamp": ["2023-01-01T00:00:00"], "temperature": [20.0], "precipitation": [0.0]})
    data.to_csv("jma_data.csv", index=False)
    return "jma_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_metoffice():
    logger = get_run_logger()
    logger.info("Downloading MetOffice data...")
    # Simulate data download
    data = pd.DataFrame({"timestamp": ["2023-01-01T00:00:00"], "temperature": [12.0], "precipitation": [0.0]})
    data.to_csv("metoffice_data.csv", index=False)
    return "metoffice_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_bom():
    logger = get_run_logger()
    logger.info("Downloading BOM data...")
    # Simulate data download
    data = pd.DataFrame({"timestamp": ["2023-01-01T00:00:00"], "temperature": [18.0], "precipitation": [0.0]})
    data.to_csv("bom_data.csv", index=False)
    return "bom_data.csv"

# Normalization tasks
@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(file_path):
    logger = get_run_logger()
    logger.info("Normalizing NOAA data...")
    data = pd.read_csv(file_path)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data.to_csv("normalized_noaa_data.csv", index=False)
    return "normalized_noaa_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(file_path):
    logger = get_run_logger()
    logger.info("Normalizing ECMWF data...")
    data = pd.read_csv(file_path)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data.to_csv("normalized_ecmwf_data.csv", index=False)
    return "normalized_ecmwf_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_jma(file_path):
    logger = get_run_logger()
    logger.info("Normalizing JMA data...")
    data = pd.read_csv(file_path)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data.to_csv("normalized_jma_data.csv", index=False)
    return "normalized_jma_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(file_path):
    logger = get_run_logger()
    logger.info("Normalizing MetOffice data...")
    data = pd.read_csv(file_path)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data.to_csv("normalized_metoffice_data.csv", index=False)
    return "normalized_metoffice_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_bom(file_path):
    logger = get_run_logger()
    logger.info("Normalizing BOM data...")
    data = pd.read_csv(file_path)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data.to_csv("normalized_bom_data.csv", index=False)
    return "normalized_bom_data.csv"

# Merge task
@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(file_paths):
    logger = get_run_logger()
    logger.info("Merging climate data...")
    data_frames = [pd.read_csv(file_path) for file_path in file_paths]
    merged_data = pd.concat(data_frames, ignore_index=True)
    merged_data.to_parquet("unified_climate_data.parquet", index=False)
    return "unified_climate_data.parquet"

# Flow
@flow(name="Climate Data Fusion Pipeline")
def climate_data_fusion_pipeline():
    logger = get_run_logger()
    logger.info("Starting Climate Data Fusion Pipeline...")

    # Download tasks
    noaa_file = download_noaa.submit()
    ecmwf_file = download_ecmwf.submit()
    jma_file = download_jma.submit()
    metoffice_file = download_metoffice.submit()
    bom_file = download_bom.submit()

    # Normalization tasks
    normalized_noaa = normalize_noaa.submit(noaa_file)
    normalized_ecmwf = normalize_ecmwf.submit(ecmwf_file)
    normalized_jma = normalize_jma.submit(jma_file)
    normalized_metoffice = normalize_metoffice.submit(metoffice_file)
    normalized_bom = normalize_bom.submit(bom_file)

    # Merge task
    file_paths = [normalized_noaa, normalized_ecmwf, normalized_jma, normalized_metoffice, normalized_bom]
    merge_climate_data(file_paths)

if __name__ == "__main__":
    climate_data_fusion_pipeline()

# Optional: Deployment/schedule configuration
# - Daily execution schedule
# - No catch-up for missed runs
# - 3 retry attempts with 5-minute delays
# - Email notifications on task failures
# - Linear topology with parallel execution branches
# - Maximum parallel width of 5 tasks during download and normalization phases