<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the structure. The pipeline has download tasks in parallel, then normalization tasks each depending on their respective download, and finally a merge task that depends on all normalizations.

First, I'll create op functions for each step. The download ops are five in total, each fetching data from different sources. Since they run in parallel, they don't depend on each other. Then each normalization op depends on its corresponding download op. The merge op depends on all five normalization ops.

I'll use the @op decorator for each function. The dependencies are set by specifying the op's inputs. For the merge op, all five normalization outputs need to be inputs.

Now, considering resources and configurations. The user mentioned FTP and HTTPS endpoints, so maybe each download op needs some config like URLs or API keys. But the problem says to provide simplified resource or config examples as comments or stubs. So I can add a config_schema for each download op with placeholders, maybe with a url parameter.

For the email notifications and retries, those are part of Dagster's run configuration and op retries. The user wants 3 retry attempts with 5-minute delays. So I can add @op with retry_policy set. Also, the email notifications would require a resource, but since the user said to provide minimal stubs, maybe just a comment.

The schedule is daily with no catch-up. But the code example doesn't include the schedule definition, just the job. So perhaps the main code is just the job, and the schedule is separate. The user's constraints say to include a minimal launch pattern, so the __main__ block with execute_in_process.

Data passing between ops is via XCom, which in Dagster is handled by returning values from ops and passing them as inputs to downstream ops. So each download op returns some data (maybe a file path), which is then passed to the normalization op, which returns normalized data, and all are passed to merge.

Naming the ops: download_agency, normalize_agency, merge_climate_data.

Now, putting it all together. Each download op is an op with no inputs. Each normalize op takes the corresponding download op's output. The merge op takes all five normalize outputs.

For the retry policy, I'll add retry_policy=RetryPolicy(max_retries=3, delay=300) in each op's decorator.

Email notifications would require a resource, like an email client, but since it's a stub, maybe just a commented resource.

The output is Parquet, so the merge op might write to a file and return the path. But for simplicity, the functions can just return dummy data, like returning a string indicating the path.

Testing if the dependencies are correctly set. All download ops are independent. Each normalize depends on one download. Merge depends on all normalizes.

Now, writing the code:

- Import necessary modules: dagster's op, job, RetryPolicy.
- Define each download op with config_schema for url.
- Each normalize op takes the download's output.
- Merge op takes all five normalize outputs.
- Define the job with the ops and dependencies.
- Add the main block to execute.

Make sure the code is PEP8 compliant, uses proper indentation, and has minimal docstrings.

I think that's the plan. Now code it step by step.
</think>

from dagster import op, job, RetryPolicy

@op(
    config_schema={"url": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def download_noaa(context):
    """Download NOAA weather station data from FTP server"""
    # Example implementation:
    # ftp_client.get(context.op_config["url"])
    return f"noaa_data_{context.run_id}.csv"

@op(
    config_schema={"url": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def download_ecmwf(context):
    """Download ECMWF weather station data from HTTPS endpoint"""
    return f"ecmwf_data_{context.run_id}.csv"

@op(
    config_schema={"url": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def download_jma(context):
    """Download JMA weather station data from HTTPS endpoint"""
    return f"jma_data_{context.run_id}.csv"

@op(
    config_schema={"url": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def download_metoffice(context):
    """Download MetOffice weather station data from HTTPS endpoint"""
    return f"metoffice_data_{context.run_id}.csv"

@op(
    config_schema={"url": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def download_bom(context):
    """Download BOM weather station data from HTTPS endpoint"""
    return f"bom_data_{context.run_id}.csv"

@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def normalize_noaa(noaa_data):
    """Convert NOAA data to standard format"""
    return f"normalized_{noaa_data}"

@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def normalize_ecmwf(ecmwf_data):
    """Convert ECMWF data to standard format"""
    return f"normalized_{ecmwf_data}"

@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def normalize_jma(jma_data):
    """Convert JMA data to standard format"""
    return f"normalized_{jma_data}"

@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def normalize_metoffice(metoffice_data):
    """Convert MetOffice data to standard format"""
    return f"normalized_{metoffice_data}"

@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def normalize_bom(bom_data):
    """Convert BOM data to standard format"""
    return f"normalized_{bom_data}"

@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def merge_climate_data(
    noaa_norm, ecmwf_norm, jma_norm, metoffice_norm, bom_norm
):
    """Merge all normalized datasets into unified Parquet file"""
    merged_path = f"climate_data_merged_{noaa_norm[-10:]}.parquet"
    # Implementation would combine all inputs here
    return merged_path

@job
def climate_data_fusion_job():
    noaa = normalize_noaa(download_noaa())
    ecmwf = normalize_ecmwf(download_ecmwf())
    jma = normalize_jma(download_jma())
    metoffice = normalize_metoffice(download_metoffice())
    bom = normalize_bom(download_bom())
    
    merge_climate_data(noaa, ecmwf, jma, metoffice, bom)

if __name__ == "__main__":
    # Example execution with minimal config
    result = climate_data_fusion_job.execute_in_process(run_config={
        "ops": {
            "download_noaa": {"config": {"url": "ftp://noaa.example.com/data.csv"}},
            "download_ecmwf": {"config": {"url": "https://ecmwf.example.org/data.csv"}},
            "download_jma": {"config": {"url": "https://jma.go.jp/data.csv"}},
            "download_metoffice": {"config": {"url": "https://metoffice.gov.uk/data.csv"}},
            "download_bom": {"config": {"url": "https://bom.gov.au/data.csv"}},
        }
    })