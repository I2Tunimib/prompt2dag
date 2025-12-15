<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the structure. The pipeline has download tasks in parallel, followed by normalization tasks also in parallel, and then a merge task that depends on all normalization steps.

First, each download and normalization step is an op. Since they need to run in parallel, their dependencies should be set up so that they don't depend on each other. The download ops (like download_noaa) will each feed into their respective normalization op (normalize_noaa). Then all normalization ops feed into the merge_climate_data op.

So, for the Dagster job, I'll define each download as an @op, each normalization as another @op, and the merge as a final @op. The dependencies are: each normalization op depends on its download op, and the merge op depends on all normalization ops.

I need to make sure that the dependencies are correctly set. For example, normalize_noaa requires download_noaa, and similarly for the others. Then merge_climate_data requires all five normalization ops.

Now, considering resources and config. The problem mentions FTP and HTTPS endpoints, but the user wants simplified examples. Maybe add a config_schema for each download op with the endpoint, but perhaps just leave them as placeholders with comments. Alternatively, use a resource for API keys or connections, but since the user said minimal stubs, maybe skip that and just have the download ops return dummy data for now.

The merge task outputs a Parquet file, so maybe the normalize ops return DataFrames, and the merge op concatenates them. But since the user wants executable code, perhaps simulate that with pandas DataFrames.

Also, the pipeline has scheduling and retries. But in Dagster, schedules and retries are configured in the job's definition. The job should include the retry policy. So, in the @job decorator, add a config for retries with 3 attempts and 5-minute delay. However, the user's example code might not include the schedule since that's in a separate definition. The main code should have the job with the retry policy.

Wait, the user's constraints mention including a minimal launch pattern, like the if __name__ block. So the code should have that.

Putting it all together:

Define each download op with a function that returns some data (maybe a dummy DataFrame). Each normalize op takes the respective download op's output and processes it. The merge op takes all five normalized outputs and combines them.

For dependencies, in the job definition, use .alias to avoid name conflicts if necessary, but since each op has a unique name, it's okay. Then, in the job, set the dependencies: each normalization op depends on a download op, and the merge depends on all normalization ops.

Testing if the code is PEP 8 compliant, function names are snake_case, proper spacing, etc.

Now, code outline:

from dagster import op, job, RetryPolicy
import pandas as pd

# Define download ops
@op
def download_noaa():
    # Simulate NOAA data download
    return pd.DataFrame({'station': ['NOAA1'], 'temp_f': [75.2]})

@op
def download_ecmwf():
    # Simulate ECMWF data download
    return pd.DataFrame({'station': ['ECMWF1'], 'temp_c': [24.0]})

# Repeat for JMA, MetOffice, BOM...

# Normalization ops
@op
def normalize_noaa(context, data):
    # Convert to Celsius, ISO timestamp, etc.
    data['temp_c'] = (data['temp_f'] - 32) * 5/9
    data['timestamp'] = pd.Timestamp.now().isoformat()
    return data[['station', 'timestamp', 'temp_c']]

# Similarly for other normalize functions...

@op
def merge_climate_data(noaa, ecmwf, jma, metoffice, bom):
    # Combine all DataFrames
    combined = pd.concat([noaa, ecmwf, jma, metoffice, bom])
    # Convert to Parquet (simulated)
    combined.to_parquet('unified_climate_data.parquet')
    return combined

@job(retry_policy=RetryPolicy(max_retries=3, delay=300))
def climate_data_fusion_job():
    noaa_data = download_noaa()
    ecmwf_data = download_ecmwf()
    jma_data = download_jma()
    metoffice_data = download_metoffice()
    bom_data = download_bom()

    normalized_noaa = normalize_noaa(noaa_data)
    normalized_ecmwf = normalize_ecmwf(ecmwf_data)
    normalized_jma = normalize_jma(jma_data)
    normalized_metoffice = normalize_metoffice(metoffice_data)
    normalized_bom = normalize_bom(bom_data)

    merge_climate_data(
        normalized_noaa,
        normalized_ecmwf,
        normalized_jma,
        normalized_metoffice,
        normalized_bom
    )

if __name__ == '__main__':
    climate_data_fusion_job.execute_in_process()

Wait, but the normalize_ecmwf function would need to handle its data. For example, ECMWF's data might already be in Celsius, so maybe just add timestamp. Similarly for others. The code can have minimal processing for demonstration.

Also, the RetryPolicy is in the job decorator. The email notifications on failure are not handled here since that would require resources, which the user said to provide minimal stubs. So perhaps a comment indicating that email resources would be configured elsewhere.

This code should be executable with Dagster and pandas installed. Each op returns a DataFrame, and the merge combines them. The main block runs the job in process.

I need to make sure all five download and normalization ops are included. Also, the dependencies are correctly set in the job function by calling each normalize function with the respective download output.

Yes, this should satisfy the constraints. The code is PEP 8 compliant, uses explicit dependencies, and includes the retry policy. The launch pattern is included. Resources and config are commented or stubbed as needed.
</think>

from dagster import job, op, RetryPolicy
import pandas as pd
from datetime import datetime

# --- Download Operations ---
@op
def download_noaa():
    """Fetch NOAA weather station data from FTP server"""
    # Example: pd.read_csv('ftp://noaa.gov/data.csv')
    return pd.DataFrame({
        'station_id': ['NOAA001'],
        'timestamp': [datetime.now().isoformat()],
        'temp_f': [75.3]
    })

@op
def download_ecmwf():
    """Fetch ECMWF weather station data from HTTPS endpoint"""
    return pd.DataFrame({
        'station_code': ['ECMWF001'],
        'time': [datetime.now()],
        'temp_c': [24.1]
    })

@op
def download_jma():
    """Fetch JMA weather station data from HTTPS endpoint"""
    return pd.DataFrame({
        'id': ['JMA001'],
        'datetime': [datetime.now()],
        'temperature': [25.2]
    })

@op
def download_metoffice():
    """Fetch MetOffice weather station data from HTTPS endpoint"""
    return pd.DataFrame({
        'station': ['MET001'],
        'ts': [datetime.now().timestamp()],
        'temp': [18.7]
    })

@op
def download_bom():
    """Fetch BOM weather station data from HTTPS endpoint"""
    return pd.DataFrame({
        'bom_id': ['BOM001'],
        'time_utc': [datetime.utcnow()],
        'temp': [22.4]
    })

# --- Normalization Operations ---
@op
def normalize_noaa(_context, df):
    """Convert NOAA data to standard format"""
    df['temp_c'] = (df['temp_f'] - 32) * 5/9
    return df[['station_id', 'timestamp', 'temp_c']].rename(
        columns={'station_id': 'station_id'}
    )

@op
def normalize_ecmwf(_context, df):
    """Convert ECMWF data to standard format"""
    df['timestamp'] = df['time'].apply(lambda x: x.isoformat())
    return df[['station_code', 'timestamp', 'temp_c']].rename(
        columns={'station_code': 'station_id'}
    )

@op
def normalize_jma(_context, df):
    """Convert JMA data to standard format"""
    df['timestamp'] = df['datetime'].apply(lambda x: x.isoformat())
    return df[['id', 'timestamp', 'temperature']].rename(
        columns={'id': 'station_id', 'temperature': 'temp_c'}
    )

@op
def normalize_metoffice(_context, df):
    """Convert MetOffice data to standard format"""
    df['timestamp'] = df['ts'].apply(
        lambda x: datetime.fromtimestamp(x).isoformat()
    )
    return df[['station', 'timestamp', 'temp']].rename(
        columns={'station': 'station_id', 'temp': 'temp_c'}
    )

@op
def normalize_bom(_context, df):
    """Convert BOM data to standard format"""
    df['timestamp'] = df['time_utc'].apply(lambda x: x.isoformat())
    return df[['bom_id', 'timestamp', 'temp']].rename(
        columns={'bom_id': 'station_id', 'temp': 'temp_c'}
    )

# --- Merge Operation ---
@op
def merge_climate_data(
    noaa, ecmwf, jma, metoffice, bom
):
    """Merge all normalized datasets into unified Parquet format"""
    combined = pd.concat([noaa, ecmwf, jma, metoffice, bom])
    combined.to_parquet('/tmp/unified_climate_data.parquet')
    return combined

@job(retry_policy=RetryPolicy(max_retries=3, delay=300))
def climate_data_fusion_job():
    noaa = normalize_noaa(download_noaa())
    ecmwf = normalize_ecmwf(download_ecmwf())
    jma = normalize_jma(download_jma())
    metoffice = normalize_metoffice(download_metoffice())
    bom = normalize_bom(download_bom())
    
    merge_climate_data(noaa, ecmwf, jma, metoffice, bom)

if __name__ == '__main__':
    climate_data_fusion_job.execute_in_process()