from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

# Download tasks
@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_noaa():
    logger = get_run_logger()
    logger.info("Downloading NOAA data...")
    # Placeholder for actual download logic
    return "noaa_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_ecmwf():
    logger = get_run_logger()
    logger.info("Downloading ECMWF data...")
    # Placeholder for actual download logic
    return "ecmwf_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_jma():
    logger = get_run_logger()
    logger.info("Downloading JMA data...")
    # Placeholder for actual download logic
    return "jma_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_metoffice():
    logger = get_run_logger()
    logger.info("Downloading MetOffice data...")
    # Placeholder for actual download logic
    return "metoffice_data.csv"

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_bom():
    logger = get_run_logger()
    logger.info("Downloading BOM data...")
    # Placeholder for actual download logic
    return "bom_data.csv"

# Normalization tasks
@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(file_path):
    logger = get_run_logger()
    logger.info("Normalizing NOAA data...")
    # Placeholder for actual normalization logic
    df = pd.read_csv(file_path)
    # Example normalization
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(lambda x: (x - 32) * 5/9)  # Convert to Celsius
    df['elevation'] = df['elevation'] * 0.3048  # Convert to meters
    df.to_csv("normalized_noaa_data.csv", index=False)
    return "normalized_noaa_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(file_path):
    logger = get_run_logger()
    logger.info("Normalizing ECMWF data...")
    # Placeholder for actual normalization logic
    df = pd.read_csv(file_path)
    # Example normalization
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(lambda x: (x - 32) * 5/9)  # Convert to Celsius
    df['elevation'] = df['elevation'] * 0.3048  # Convert to meters
    df.to_csv("normalized_ecmwf_data.csv", index=False)
    return "normalized_ecmwf_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_jma(file_path):
    logger = get_run_logger()
    logger.info("Normalizing JMA data...")
    # Placeholder for actual normalization logic
    df = pd.read_csv(file_path)
    # Example normalization
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(lambda x: (x - 32) * 5/9)  # Convert to Celsius
    df['elevation'] = df['elevation'] * 0.3048  # Convert to meters
    df.to_csv("normalized_jma_data.csv", index=False)
    return "normalized_jma_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(file_path):
    logger = get_run_logger()
    logger.info("Normalizing MetOffice data...")
    # Placeholder for actual normalization logic
    df = pd.read_csv(file_path)
    # Example normalization
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(lambda x: (x - 32) * 5/9)  # Convert to Celsius
    df['elevation'] = df['elevation'] * 0.3048  # Convert to meters
    df.to_csv("normalized_metoffice_data.csv", index=False)
    return "normalized_metoffice_data.csv"

@task(retries=3, retry_delay_seconds=300)
def normalize_bom(file_path):
    logger = get_run_logger()
    logger.info("Normalizing BOM data...")
    # Placeholder for actual normalization logic
    df = pd.read_csv(file_path)
    # Example normalization
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['temperature'] = df['temperature'].apply(lambda x: (x - 32) * 5/9)  # Convert to Celsius
    df['elevation'] = df['elevation'] * 0.3048  # Convert to meters
    df.to_csv("normalized_bom_data.csv", index=False)
    return "normalized_bom_data.csv"

# Merge task
@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(file_paths):
    logger = get_run_logger()
    logger.info("Merging climate data...")
    dfs = [pd.read_csv(file_path) for file_path in file_paths]
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.to_parquet("unified_climate_data.parquet", index=False)
    return "unified_climate_data.parquet"

# Flow definition
@flow(name="Climate Data Fusion Pipeline", retries=3, retry_delay_seconds=300)
def climate_data_fusion_pipeline():
    logger = get_run_logger()
    logger.info("Starting Climate Data Fusion Pipeline...")

    # Fan-out: Download tasks
    noaa_file = download_noaa.submit()
    ecmwf_file = download_ecmwf.submit()
    jma_file = download_jma.submit()
    metoffice_file = download_metoffice.submit()
    bom_file = download_bom.submit()

    # Fan-out: Normalization tasks
    noaa_normalized = normalize_noaa.submit(noaa_file)
    ecmwf_normalized = normalize_ecmwf.submit(ecmwf_file)
    jma_normalized = normalize_jma.submit(jma_file)
    metoffice_normalized = normalize_metoffice.submit(metoffice_file)
    bom_normalized = normalize_bom.submit(bom_file)

    # Fan-in: Merge task
    merged_file = merge_climate_data([noaa_normalized, ecmwf_normalized, jma_normalized, metoffice_normalized, bom_normalized])

    logger.info(f"Pipeline completed. Unified climate dataset saved to {merged_file}")

if __name__ == "__main__":
    climate_data_fusion_pipeline()

# Optional: Deployment/schedule configuration
# Deploy this flow with a daily schedule, no catch-up, and email notifications on task failures.
# Example deployment command:
# prefect deployment build climate_data_fusion_pipeline.py:climate_data_fusion_pipeline -n "Daily Climate Data Fusion" --cron "0 0 * * *" --skip-missing-cron-times --no-catchup --notify-on-failure --notify-on-crash