from dagster import op, job, In, Out, ResourceDefinition, RetryPolicy, Failure, success_hook, failure_hook

# Resources
class WeatherDataSource:
    def fetch_data(self):
        # Fetch data from the source
        pass

noaa_resource = ResourceDefinition.hardcoded_resource(WeatherDataSource(), "NOAA FTP server")
ecmwf_resource = ResourceDefinition.hardcoded_resource(WeatherDataSource(), "ECMWF HTTPS endpoint")
jma_resource = ResourceDefinition.hardcoded_resource(WeatherDataSource(), "JMA HTTPS endpoint")
metoffice_resource = ResourceDefinition.hardcoded_resource(WeatherDataSource(), "MetOffice HTTPS endpoint")
bom_resource = ResourceDefinition.hardcoded_resource(WeatherDataSource(), "BOM HTTPS endpoint")

# Download Ops
@op(required_resource_keys={"data_source"}, out=Out(str))
def download_noaa(context):
    """Fetches NOAA weather station CSV data"""
    data_path = context.resources.data_source.fetch_data()
    return data_path

@op(required_resource_keys={"data_source"}, out=Out(str))
def download_ecmwf(context):
    """Fetches ECMWF weather station CSV data"""
    data_path = context.resources.data_source.fetch_data()
    return data_path

@op(required_resource_keys={"data_source"}, out=Out(str))
def download_jma(context):
    """Fetches JMA weather station CSV data"""
    data_path = context.resources.data_source.fetch_data()
    return data_path

@op(required_resource_keys={"data_source"}, out=Out(str))
def download_metoffice(context):
    """Fetches MetOffice weather station CSV data"""
    data_path = context.resources.data_source.fetch_data()
    return data_path

@op(required_resource_keys={"data_source"}, out=Out(str))
def download_bom(context):
    """Fetches BOM weather station CSV data"""
    data_path = context.resources.data_source.fetch_data()
    return data_path

# Normalize Ops
@op(ins={"data_path": In(str)}, out=Out(str))
def normalize_noaa(context, data_path):
    """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)"""
    # Normalize data and save to new path
    normalized_path = f"normalized_{data_path}"
    return normalized_path

@op(ins={"data_path": In(str)}, out=Out(str))
def normalize_ecmwf(context, data_path):
    """Converts ECMWF data to standard format (ISO timestamp, Celsius, meters)"""
    # Normalize data and save to new path
    normalized_path = f"normalized_{data_path}"
    return normalized_path

@op(ins={"data_path": In(str)}, out=Out(str))
def normalize_jma(context, data_path):
    """Converts JMA data to standard format (ISO timestamp, Celsius, meters)"""
    # Normalize data and save to new path
    normalized_path = f"normalized_{data_path}"
    return normalized_path

@op(ins={"data_path": In(str)}, out=Out(str))
def normalize_metoffice(context, data_path):
    """Converts MetOffice data to standard format (ISO timestamp, Celsius, meters)"""
    # Normalize data and save to new path
    normalized_path = f"normalized_{data_path}"
    return normalized_path

@op(ins={"data_path": In(str)}, out=Out(str))
def normalize_bom(context, data_path):
    """Converts BOM data to standard format (ISO timestamp, Celsius, meters)"""
    # Normalize data and save to new path
    normalized_path = f"normalized_{data_path}"
    return normalized_path

# Merge Op
@op(ins={"noaa_data": In(str), "ecmwf_data": In(str), "jma_data": In(str), "metoffice_data": In(str), "bom_data": In(str)}, out=Out(str))
def merge_climate_data(context, noaa_data, ecmwf_data, jma_data, metoffice_data, bom_data):
    """Combines all five normalized datasets into unified climate dataset"""
    # Merge data and save to new path
    merged_path = "merged_climate_data.parquet"
    return merged_path

# Hooks
@success_hook
def notify_success(context):
    context.log.info("Pipeline completed successfully")

@failure_hook
def notify_failure(context):
    context.log.error("Pipeline failed")

# Job
@job(
    resource_defs={
        "noaa_data_source": noaa_resource,
        "ecmwf_data_source": ecmwf_resource,
        "jma_data_source": jma_resource,
        "metoffice_data_source": metoffice_resource,
        "bom_data_source": bom_resource,
    },
    hooks=[notify_success, notify_failure],
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def climate_data_fusion_job():
    noaa_data = download_noaa.alias("download_noaa")()
    ecmwf_data = download_ecmwf.alias("download_ecmwf")()
    jma_data = download_jma.alias("download_jma")()
    metoffice_data = download_metoffice.alias("download_metoffice")()
    bom_data = download_bom.alias("download_bom")()

    noaa_normalized = normalize_noaa.alias("normalize_noaa")(data_path=noaa_data)
    ecmwf_normalized = normalize_ecmwf.alias("normalize_ecmwf")(data_path=ecmwf_data)
    jma_normalized = normalize_jma.alias("normalize_jma")(data_path=jma_data)
    metoffice_normalized = normalize_metoffice.alias("normalize_metoffice")(data_path=metoffice_data)
    bom_normalized = normalize_bom.alias("normalize_bom")(data_path=bom_data)

    merge_climate_data(
        noaa_data=noaa_normalized,
        ecmwf_data=ecmwf_normalized,
        jma_data=jma_normalized,
        metoffice_data=metoffice_normalized,
        bom_data=bom_normalized,
    )

if __name__ == "__main__":
    result = climate_data_fusion_job.execute_in_process()