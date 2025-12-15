from dagster import op, job, Field, String, RetryPolicy, In, Out, graph

# Resources and Configs
class WeatherDataSource:
    def __init__(self, url: str):
        self.url = url

# Download Ops
@op(
    config_schema={"url": Field(String, default_value="ftp://noaa.gov/data.csv")},
    out=Out(String, description="Path to downloaded file")
)
def download_noaa(context):
    """Download NOAA weather station data."""
    url = context.op_config["url"]
    # Simulate download
    file_path = "noaa_data.csv"
    context.log.info(f"Downloaded from {url} to {file_path}")
    return file_path

@op(
    config_schema={"url": Field(String, default_value="https://ecmwf.eu/data.csv")},
    out=Out(String, description="Path to downloaded file")
)
def download_ecmwf(context):
    """Download ECMWF weather station data."""
    url = context.op_config["url"]
    # Simulate download
    file_path = "ecmwf_data.csv"
    context.log.info(f"Downloaded from {url} to {file_path}")
    return file_path

@op(
    config_schema={"url": Field(String, default_value="https://jma.go.jp/data.csv")},
    out=Out(String, description="Path to downloaded file")
)
def download_jma(context):
    """Download JMA weather station data."""
    url = context.op_config["url"]
    # Simulate download
    file_path = "jma_data.csv"
    context.log.info(f"Downloaded from {url} to {file_path}")
    return file_path

@op(
    config_schema={"url": Field(String, default_value="https://metoffice.gov.uk/data.csv")},
    out=Out(String, description="Path to downloaded file")
)
def download_metoffice(context):
    """Download MetOffice weather station data."""
    url = context.op_config["url"]
    # Simulate download
    file_path = "metoffice_data.csv"
    context.log.info(f"Downloaded from {url} to {file_path}")
    return file_path

@op(
    config_schema={"url": Field(String, default_value="https://bom.gov.au/data.csv")},
    out=Out(String, description="Path to downloaded file")
)
def download_bom(context):
    """Download BOM weather station data."""
    url = context.op_config["url"]
    # Simulate download
    file_path = "bom_data.csv"
    context.log.info(f"Downloaded from {url} to {file_path}")
    return file_path

# Normalize Ops
@op(
    ins={"file_path": In(String, description="Path to downloaded file")},
    out=Out(String, description="Path to normalized file")
)
def normalize_noaa(context, file_path):
    """Normalize NOAA data to standard format."""
    # Simulate normalization
    normalized_file_path = "noaa_normalized.parquet"
    context.log.info(f"Normalized {file_path} to {normalized_file_path}")
    return normalized_file_path

@op(
    ins={"file_path": In(String, description="Path to downloaded file")},
    out=Out(String, description="Path to normalized file")
)
def normalize_ecmwf(context, file_path):
    """Normalize ECMWF data to standard format."""
    # Simulate normalization
    normalized_file_path = "ecmwf_normalized.parquet"
    context.log.info(f"Normalized {file_path} to {normalized_file_path}")
    return normalized_file_path

@op(
    ins={"file_path": In(String, description="Path to downloaded file")},
    out=Out(String, description="Path to normalized file")
)
def normalize_jma(context, file_path):
    """Normalize JMA data to standard format."""
    # Simulate normalization
    normalized_file_path = "jma_normalized.parquet"
    context.log.info(f"Normalized {file_path} to {normalized_file_path}")
    return normalized_file_path

@op(
    ins={"file_path": In(String, description="Path to downloaded file")},
    out=Out(String, description="Path to normalized file")
)
def normalize_metoffice(context, file_path):
    """Normalize MetOffice data to standard format."""
    # Simulate normalization
    normalized_file_path = "metoffice_normalized.parquet"
    context.log.info(f"Normalized {file_path} to {normalized_file_path}")
    return normalized_file_path

@op(
    ins={"file_path": In(String, description="Path to downloaded file")},
    out=Out(String, description="Path to normalized file")
)
def normalize_bom(context, file_path):
    """Normalize BOM data to standard format."""
    # Simulate normalization
    normalized_file_path = "bom_normalized.parquet"
    context.log.info(f"Normalized {file_path} to {normalized_file_path}")
    return normalized_file_path

# Merge Op
@op(
    ins={
        "noaa_file": In(String, description="Path to normalized NOAA file"),
        "ecmwf_file": In(String, description="Path to normalized ECMWF file"),
        "jma_file": In(String, description="Path to normalized JMA file"),
        "metoffice_file": In(String, description="Path to normalized MetOffice file"),
        "bom_file": In(String, description="Path to normalized BOM file")
    },
    out=Out(String, description="Path to merged climate dataset")
)
def merge_climate_data(context, noaa_file, ecmwf_file, jma_file, metoffice_file, bom_file):
    """Merge all normalized datasets into a unified climate dataset."""
    # Simulate merging
    merged_file_path = "merged_climate_data.parquet"
    context.log.info(f"Merged {noaa_file}, {ecmwf_file}, {jma_file}, {metoffice_file}, {bom_file} into {merged_file_path}")
    return merged_file_path

# Job Definition
@job(
    resource_defs={"weather_data_source": WeatherDataSource},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Climate data fusion pipeline that downloads, normalizes, and merges weather station data from multiple agencies."
)
def climate_data_fusion_job():
    noaa_file = download_noaa()
    ecmwf_file = download_ecmwf()
    jma_file = download_jma()
    metoffice_file = download_metoffice()
    bom_file = download_bom()

    noaa_normalized = normalize_noaa(noaa_file)
    ecmwf_normalized = normalize_ecmwf(ecmwf_file)
    jma_normalized = normalize_jma(jma_file)
    metoffice_normalized = normalize_metoffice(metoffice_file)
    bom_normalized = normalize_bom(bom_file)

    merge_climate_data(
        noaa_file=noaa_normalized,
        ecmwf_file=ecmwf_normalized,
        jma_file=jma_normalized,
        metoffice_file=metoffice_normalized,
        bom_file=bom_normalized
    )

if __name__ == '__main__':
    result = climate_data_fusion_job.execute_in_process()