from dagster import job, op, multiprocess_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='download_bom',
    description='Download BOM Data',
)
def download_bom(context):
    """Op: Download BOM Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_ecmwf',
    description='Download ECMWF Data',
)
def download_ecmwf(context):
    """Op: Download ECMWF Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_jma',
    description='Download JMA Data',
)
def download_jma(context):
    """Op: Download JMA Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_metoffice',
    description='Download MetOffice Data',
)
def download_metoffice(context):
    """Op: Download MetOffice Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_noaa',
    description='Download NOAA Data',
)
def download_noaa(context):
    """Op: Download NOAA Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_bom',
    description='Normalize BOM Data',
)
def normalize_bom(context):
    """Op: Normalize BOM Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_ecmwf',
    description='Normalize ECMWF Data',
)
def normalize_ecmwf(context):
    """Op: Normalize ECMWF Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_jma',
    description='Normalize JMA Data',
)
def normalize_jma(context):
    """Op: Normalize JMA Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_metoffice',
    description='Normalize MetOffice Data',
)
def normalize_metoffice(context):
    """Op: Normalize MetOffice Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_noaa',
    description='Normalize NOAA Data',
)
def normalize_noaa(context):
    """Op: Normalize NOAA Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='merge_climate_data',
    description='Merge Climate Data',
)
def merge_climate_data(context):
    """Op: Merge Climate Data"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def bom_https(context):
    pass

@resource
def jma_https(context):
    pass

@resource
def noaa_ftp(context):
    pass

@resource
def metoffice_https(context):
    pass

@resource
def ecmwf_https(context):
    pass

# Job Definition
@job(
    name='download_noaa_pipeline',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        'bom_https': bom_https,
        'jma_https': jma_https,
        'noaa_ftp': noaa_ftp,
        'metoffice_https': metoffice_https,
        'ecmwf_https': ecmwf_https,
        'io_manager': fs_io_manager
    }
)
def download_noaa_pipeline():
    # Entry points
    noaa_data = download_noaa()
    ecmwf_data = download_ecmwf()
    jma_data = download_jma()
    metoffice_data = download_metoffice()
    bom_data = download_bom()

    # Normalization
    normalized_noaa = normalize_noaa(noaa_data)
    normalized_ecmwf = normalize_ecmwf(ecmwf_data)
    normalized_jma = normalize_jma(jma_data)
    normalized_metoffice = normalize_metoffice(metoffice_data)
    normalized_bom = normalize_bom(bom_data)

    # Merge
    merge_climate_data(
        normalized_noaa,
        normalized_ecmwf,
        normalized_jma,
        normalized_metoffice,
        normalized_bom
    )