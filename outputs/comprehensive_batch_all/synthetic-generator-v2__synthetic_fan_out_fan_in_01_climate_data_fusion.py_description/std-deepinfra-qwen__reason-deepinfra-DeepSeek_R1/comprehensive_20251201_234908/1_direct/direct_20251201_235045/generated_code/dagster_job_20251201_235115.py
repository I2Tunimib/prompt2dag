from dagster import op, job, In, Out, ResourceDefinition, String, RetryPolicy, Failure, success_hook, failure_hook

# Resources
class WeatherDataSource:
    def fetch_data(self) -> str:
        """Fetch data from the source and return a file path."""
        raise NotImplementedError

class NoaaDataSource(WeatherDataSource):
    def fetch_data(self) -> str:
        # Fetch data from NOAA FTP server
        return "path/to/noaa_data.csv"

class EcmwfDataSource(WeatherDataSource):
    def fetch_data(self) -> str:
        # Fetch data from ECMWF HTTPS endpoint
        return "path/to/ecmwf_data.csv"

class JmaDataSource(WeatherDataSource):
    def fetch_data(self) -> str:
        # Fetch data from JMA HTTPS endpoint
        return "path/to/jma_data.csv"

class MetOfficeDataSource(WeatherDataSource):
    def fetch_data(self) -> str:
        # Fetch data from MetOffice HTTPS endpoint
        return "path/to/metoffice_data.csv"

class BomDataSource(WeatherDataSource):
    def fetch_data(self) -> str:
        # Fetch data from BOM HTTPS endpoint
        return "path/to/bom_data.csv"

# Download Ops
@op(out=Out(String))
def download_noaa(context, noaa_data_source: WeatherDataSource) -> str:
    """Download NOAA weather station data."""
    return noaa_data_source.fetch_data()

@op(out=Out(String))
def download_ecmwf(context, ecmwf_data_source: WeatherDataSource) -> str:
    """Download ECMWF weather station data."""
    return ecmwf_data_source.fetch_data()

@op(out=Out(String))
def download_jma(context, jma_data_source: WeatherDataSource) -> str:
    """Download JMA weather station data."""
    return jma_data_source.fetch_data()

@op(out=Out(String))
def download_metoffice(context, metoffice_data_source: WeatherDataSource) -> str:
    """Download MetOffice weather station data."""
    return metoffice_data_source.fetch_data()

@op(out=Out(String))
def download_bom(context, bom_data_source: WeatherDataSource) -> str:
    """Download BOM weather station data."""
    return bom_data_source.fetch_data()

# Normalize Ops
@op(out=Out(String))
def normalize_noaa(context, file_path: str) -> str:
    """Normalize NOAA data to standard format."""
    # Normalize data and save to new file
    return "path/to/normalized_noaa_data.parquet"

@op(out=Out(String))
def normalize_ecmwf(context, file_path: str) -> str:
    """Normalize ECMWF data to standard format."""
    # Normalize data and save to new file
    return "path/to/normalized_ecmwf_data.parquet"

@op(out=Out(String))
def normalize_jma(context, file_path: str) -> str:
    """Normalize JMA data to standard format."""
    # Normalize data and save to new file
    return "path/to/normalized_jma_data.parquet"

@op(out=Out(String))
def normalize_metoffice(context, file_path: str) -> str:
    """Normalize MetOffice data to standard format."""
    # Normalize data and save to new file
    return "path/to/normalized_metoffice_data.parquet"

@op(out=Out(String))
def normalize_bom(context, file_path: str) -> str:
    """Normalize BOM data to standard format."""
    # Normalize data and save to new file
    return "path/to/normalized_bom_data.parquet"

# Merge Op
@op(out=Out(String))
def merge_climate_data(context, noaa_path: str, ecmwf_path: str, jma_path: str, metoffice_path: str, bom_path: str) -> str:
    """Merge all normalized datasets into a unified climate dataset."""
    # Merge data and save to new file
    return "path/to/unified_climate_data.parquet"

# Hooks
@success_hook
def notify_success(context):
    context.log.info("Pipeline completed successfully.")

@failure_hook
def notify_failure(context):
    context.log.error("Pipeline failed.")

# Job
@job(
    resource_defs={
        "noaa_data_source": ResourceDefinition.hardcoded_resource(NoaaDataSource()),
        "ecmwf_data_source": ResourceDefinition.hardcoded_resource(EcmwfDataSource()),
        "jma_data_source": ResourceDefinition.hardcoded_resource(JmaDataSource()),
        "metoffice_data_source": ResourceDefinition.hardcoded_resource(MetOfficeDataSource()),
        "bom_data_source": ResourceDefinition.hardcoded_resource(BomDataSource()),
    },
    hooks={notify_success, notify_failure},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
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
        noaa_normalized,
        ecmwf_normalized,
        jma_normalized,
        metoffice_normalized,
        bom_normalized,
    )

if __name__ == "__main__":
    result = climate_data_fusion_job.execute_in_process()