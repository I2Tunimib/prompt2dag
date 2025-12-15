from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition


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


@job(
    name="download_noaa_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "noaa_ftp": ResourceDefinition.hardcoded_resource(None),
        "bom_https": ResourceDefinition.hardcoded_resource(None),
        "jma_https": ResourceDefinition.hardcoded_resource(None),
        "ecmwf_https": ResourceDefinition.hardcoded_resource(None),
        "metoffice_https": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def download_noaa_pipeline():
    # Entry point download ops
    noaa = download_noaa()
    ecmwf = download_ecmwf()
    jma = download_jma()
    metoffice = download_metoffice()
    bom = download_bom()

    # Normalization ops, each depends on its respective download
    norm_noaa = normalize_noaa(noaa)
    norm_ecmwf = normalize_ecmwf(ecmwf)
    norm_jma = normalize_jma(jma)
    norm_metoffice = normalize_metoffice(metoffice)
    norm_bom = normalize_bom(bom)

    # Merge op that runs after all normalization steps (fanin pattern)
    merge_climate_data()
    # Note: The merge operation does not consume inputs directly,
    # but its execution is ordered after the normalization steps
    # by virtue of being placed after them in the job definition.