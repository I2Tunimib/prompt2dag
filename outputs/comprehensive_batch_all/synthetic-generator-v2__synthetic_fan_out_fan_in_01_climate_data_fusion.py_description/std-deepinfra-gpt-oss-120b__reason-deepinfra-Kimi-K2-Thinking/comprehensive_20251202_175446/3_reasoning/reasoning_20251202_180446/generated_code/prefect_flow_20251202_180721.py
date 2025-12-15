from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import pandas as pd
import tempfile
import os


@task(retries=3, retry_delay_seconds=300)
def download_noaa() -> str:
    """Download NOAA weather station CSV data from FTP server."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    df = pd.DataFrame({
        'timestamp': ['2023-01-01 00:00', '2023-01-01 01:00'],
        'temp_f': [32.0, 33.5],
        'precip_in': [0.1, 0.2],
        'wind_mph': [10, 15]
    })
    df.to_csv(temp_file.name, index=False)
    return temp_file.name


@task(retries=3, retry_delay_seconds=300)
def download_ecmwf() -> str:
    """Download ECMWF weather station CSV data from HTTPS endpoint."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    df = pd.DataFrame({
        'time': ['2023-01-01T00:00:00Z', '2023-01-01T01:00:00Z'],
        'temperature_k': [273.15, 274.15],
        'rainfall_mm': [2.5, 5.0],
        'wind_speed_ms': [5, 7]
    })
    df.to_csv(temp_file.name, index=False)
    return temp_file.name


@task(retries=3, retry_delay_seconds=300)
def download_jma() -> str:
    """Download JMA weather station CSV data from HTTPS endpoint."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    df = pd.DataFrame({
        'datetime': ['2023-01-01 00:00:00', '2023-01-01 01:00:00'],
        'temp_c': [0, 1],
        'precipitation_mm': [2, 4],
        'wind_speed_kmh': [18, 25]
    })
    df.to_csv(temp_file.name, index=False)
    return temp_file.name


@task(retries=3, retry_delay_seconds=300)
def download_metoffice() -> str:
    """Download MetOffice weather station CSV data from HTTPS endpoint."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    df = pd.DataFrame({
        'observation_time': ['2023-01-01 00:00', '2023-01-01 01:00'],
        'air_temp_c': [0.5, 1.5],
        'precipitation_amount_mm': [3, 6],
        'wind_speed_knots': [10, 14]
    })
    df.to_csv(temp_file.name, index=False)
    return temp_file.name


@task(retries=3, retry_delay_seconds=300)
def download_bom() -> str:
    """Download BOM weather station CSV data from HTTPS endpoint."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    df = pd.DataFrame({
        'aifstime_utc': ['202301010000', '202301010100'],
        'apparent_t_c': [0.2, 1.2],
        'rain_trace': [0.2, 0.4],