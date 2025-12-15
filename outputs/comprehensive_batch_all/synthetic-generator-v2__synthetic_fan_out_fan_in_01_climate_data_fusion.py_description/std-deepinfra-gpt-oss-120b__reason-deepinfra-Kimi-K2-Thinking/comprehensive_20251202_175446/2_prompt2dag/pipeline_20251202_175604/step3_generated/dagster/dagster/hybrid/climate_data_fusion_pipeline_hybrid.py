from dagster import (
    op,
    job,
    schedule,
    multiprocess_executor,
    fs_io_manager,
    ResourceDefinition,
)

# Pre-Generated Task Definitions

@op(
    name='download_bom',
    description='Download BOM Weather Data',
)
def download_bom(context):
    """Op: Download BOM Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='download_ecmwf',
    description='Download ECMWF Weather Data',
)
def download_ecmwf(context):
    """Op: Download ECMWF Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='download_jma',
    description='Download JMA Weather Data',
)
def download_jma(context):
    """Op: Download JMA Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='download_metoffice',
    description='Download MetOffice Weather Data',
)
def download_metoffice(context):
    """Op: Download MetOffice Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='download_noaa',
    description='Download NOAA Weather Data',
)
def download_noaa(context):
    """Op: Download NOAA Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='normalize_bom',
    description='Normalize BOM Weather Data',
)
def normalize_bom(context):
    """Op: Normalize BOM Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='normalize_ecmwf',
    description='Normalize ECMWF Weather Data',
)
def normalize_ecmwf(context):
    """Op: Normalize ECMWF Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='normalize_jma',
    description='Normalize JMA Weather Data',
)
def normalize_jma(context):
    """Op: Normalize JMA Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='normalize_metoffice',
    description='Normalize MetOffice Weather Data',
)
def normalize_metoffice(context):
    """Op: Normalize MetOffice Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='normalize_noaa',
    description='Normalize NOAA Weather Data',
)
def normalize_noaa(context):
    """Op: Normalize NOAA Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='merge_climate_data',
    description='Merge Climate Datasets',
)
def merge_climate_data(context):
    """Op: Merge Climate Datasets"""
    # Docker execution
    # Image: python:3.9
    pass


# Resource definitions (placeholders)
resource_defs = {
    "io_manager": fs_io_manager,
    "https_metoffice": ResourceDefinition.hardcoded_resource(None),
    "ftp_noaa": ResourceDefinition.hardcoded_resource(None),
    "https_ecmwf": ResourceDefinition.hardcoded_resource(None),
    "https_jma": ResourceDefinition.hardcoded_resource(None),
    "https_bom": ResourceDefinition.hardcoded_resource(None),
}


@job(
    name="climate_data_fusion_pipeline",
    description="Implements a climate data fusion workflow that downloads weather station data from five meteorological agencies, normalizes each dataset, and merges them into a unified climate dataset.",
    executor_def=multiprocess_executor,
    resource_defs=resource_defs,
)
def climate_data_fusion_pipeline():
    # Entry point download ops
    noaa_download = download_noaa()
    ecmwf_download = download_ecmwf()
    jma_download = download_jma()
    metoffice_download = download_metoffice()
    bom_download = download_bom()

    # Normalization ops
    noaa_normalized = normalize_noaa()
    ecmwf_normalized = normalize_ecmwf()
    jma_normalized = normalize_jma()
    metoffice_normalized = normalize_metoffice()
    bom_normalized = normalize_bom()

    # Set fan‑in dependencies for normalization
    noaa_download >> noaa_normalized
    ecmwf_download >> ecmwf_normalized
    jma_download >> jma_normalized
    metoffice_download >> metoffice_normalized
    bom_download >> bom_normalized

    # Merge op with fan‑in from all normalized datasets
    merged = merge_climate_data()
    noaa_normalized >> merged
    ecmwf_normalized >> merged
    jma_normalized >> merged
    metoffice_normalized >> merged
    bom_normalized >> merged


@schedule(
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    job=climate_data_fusion_pipeline,
    execution_timezone="UTC",
    description="Daily execution of the climate data fusion pipeline.",
)
def climate_data_fusion_schedule():
    return {}