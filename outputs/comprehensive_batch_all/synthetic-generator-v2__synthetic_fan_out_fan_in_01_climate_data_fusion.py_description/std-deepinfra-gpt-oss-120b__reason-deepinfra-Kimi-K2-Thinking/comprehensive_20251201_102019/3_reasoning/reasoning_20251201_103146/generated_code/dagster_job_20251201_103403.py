from dagster import (
    op,
    job,
    ScheduleDefinition,
    DefaultScheduleStatus,
    RetryPolicy,
    Out,
    In,
)
import time
import os


class DataSourceClient:
    """Stub client for downloading data from meteorological agencies."""
    
    def __init__(self, base_path: str = "/tmp/climate_data"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)
    
    def download(self, agency: str) -> str:
        """Simulate downloading data from agency."""
        filepath = os.path.join(self.base_path, f"{agency}_raw.csv")
        time.sleep(1)
        return filepath


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "download", "agency": "noaa"},
)
def download_noaa(context) -> str:
    """Fetches NOAA weather station CSV data from FTP server."""
    client = DataSourceClient()
    filepath = client.download("noaa")
    context.log.info(f"Downloaded NOAA data to {filepath}")
    return filepath


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "download", "agency": "ecmwf"},
)
def download_ecmwf(context) -> str:
    """Fetches ECMWF weather station CSV data from HTTPS endpoint."""
    client = DataSourceClient()
    filepath = client.download("ecmwf")
    context.log.info(f"Downloaded ECMWF data to {filepath}")
    return filepath


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "download", "agency": "jma"},
)
def download_jma(context) -> str:
    """Fetches JMA weather station CSV data from HTTPS endpoint."""
    client = DataSourceClient()
    filepath = client.download("jma")
    context.log.info(f"Downloaded JMA data to {filepath}")
    return filepath


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "download", "agency": "metoffice"},
)
def download_metoffice(context) -> str:
    """Fetches MetOffice weather station CSV data from HTTPS endpoint."""
    client = DataSourceClient()
    filepath = client.download("metoffice")
    context.log.info(f"Downloaded MetOffice data to {filepath}")
    return filepath


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "download", "agency": "bom"},
)
def download_bom(context) -> str:
    """Fetches BOM weather station CSV data from HTTPS endpoint."""
    client = DataSourceClient()
    filepath = client.download("bom")
    context.log.info(f"Downloaded BOM data to {filepath}")
    return filepath


@op(
    ins={"input_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "normalize", "agency": "noaa"},
)
def normalize_noaa(context, input_path: str) -> str:
    """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)."""
    output_path = input_path.replace("_raw.csv", "_normalized.csv")
    time.sleep(0.5)
    context.log.info(f"Normalized NOAA data: {input_path} -> {output_path}")
    return output_path


@op(
    ins={"input_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "normalize", "agency": "ecmwf"},
)
def normalize_ecmwf(context, input_path: str) -> str:
    """Converts ECMWF data to standard format (ISO timestamp, Celsius, meters)."""
    output_path = input_path.replace("_raw.csv", "_normalized.csv")
    time.sleep(0.5)
    context.log.info(f"Normalized ECMWF data: {input_path} -> {output_path}")
    return output_path


@op(
    ins={"input_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "normalize", "agency": "jma"},
)
def normalize_jma(context, input_path: str) -> str:
    """Converts JMA data to standard format (ISO timestamp, Celsius, meters)."""
    output_path = input_path.replace("_raw.csv", "_normalized.csv")
    time.sleep(0.5)
    context.log.info(f"Normalized JMA data: {input_path} -> {output_path}")
    return output_path


@op(
    ins={"input_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "normalize", "agency": "metoffice"},
)
def normalize_metoffice(context, input_path: str) -> str:
    """Converts MetOffice data to standard format (ISO timestamp, Celsius, meters)."""
    output_path = input_path.replace("_raw.csv", "_normalized.csv")
    time.sleep(0.5)
    context.log.info(f"Normalized MetOffice data: {input_path} -> {output_path}")
    return output_path


@op(
    ins={"input_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "normalize", "agency": "bom"},
)
def normalize_bom(context, input_path: str) -> str:
    """Converts BOM data to standard format (ISO timestamp, Celsius, meters)."""
    output_path = input_path.replace("_raw.csv", "_normalized.csv")
    time.sleep(0.5)
    context.log.info(f"Normalized BOM data: {input_path} -> {output_path}")
    return output_path


@op(
    ins={
        "noaa_path": In(str),
        "ecmwf_path": In(str),
        "jma_path": In(str),
        "metoffice_path": In(str),
        "bom_path": In(str),
    },
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    tags={"task": "merge"},
)
def merge_climate_data(
    context,
    noaa_path: str,
    ecmwf_path: str,
    jma_path: str,
    metoffice_path: str,
    bom_path: str,
) -> str:
    """Combines all five normalized datasets into unified climate dataset (Parquet)."""
    output_path = "/tmp/climate_data/unified_climate.parquet"
    time.sleep(2)
    context.log.info(f"Merged all datasets into: {output_path}")
    return output_path


@job(
    tags={"pipeline": "climate_data_fusion"},
    description="Climate data fusion pipeline with fan-out/fan-in pattern",
)
def climate_data_fusion_job():
    noaa_raw = download_noaa()
    ecmwf_raw = download_ecmwf()
    jma_raw = download_jma()
    metoffice_raw = download_metoffice()
    bom_raw = download_bom()
    
    noaa_norm = normalize_noaa(noaa_raw)
    ecmwf_norm = normalize_ecmwf(ecmwf_raw)
    jma_norm = normalize_jma(jma_raw)
    metoffice_norm = normalize_metoffice(metoffice_raw)
    bom_norm = normalize_bom(bom_raw)
    
    merge_climate_data(
        noaa_path=noaa_norm,
        ecmwf_path=ecmwf_norm,
        jma_path=jma_norm,
        metoffice_path=metoffice_norm,
        bom_path=bom_norm,
    )


climate_data_fusion_schedule = ScheduleDefinition(
    job=climate_data_fusion_job,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)


# Email notifications via run failure sensor:
# from dagster import make_email_on_run_failure_sensor
# email_on_failure = make_email_on_run_failure_sensor(
#     email_from="dagster@example.com",
#     email_password=os.getenv("EMAIL_PASSWORD"),
#     email_to=["data-team@example.com"],
# )


if __name__ == "__main__":
    result = climate_data_fusion_job.execute_in_process()
    assert result.success