<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the structure. The pipeline has five download tasks that run in parallel, each followed by their respective normalization tasks. Then all normalization tasks feed into a merge task.

First, I'll create ops for each download and normalization step. Since the downloads are parallel, each download op doesn't depend on others. Each normalization op depends on its corresponding download op. The merge op depends on all normalization ops.

I'll use the @op decorator for each function. The download functions can return some dummy data for simplicity, maybe a string representing the file path. The normalization functions will take the downloaded data as input and return normalized data. The merge function will collect all normalized data and combine them.

Dependencies are set in the job definition. The download ops are all roots, then each normalization op depends on a download. The merge op depends on all five normalization ops.

For resources and config, the user mentioned to provide stubs as comments. So maybe add a config schema comment for each download op if they need different endpoints or parameters. But since the example is minimal, perhaps skip that unless necessary.

Retries and email notifications are part of the job configuration. In Dagster, retries can be set with the @op(config_schema) or in the job's configuration. But since the user wants minimal stubs, maybe add a comment about retry configuration. Similarly, email notifications would require a resource, so maybe a commented resource example.

The output is Parquet, but the merge function here can just return a combined string for simplicity. Actual Parquet writing would need libraries like pandas, but the user wants executable code, so maybe avoid external dependencies unless necessary.

Testing the code structure: each download and normalization op is defined, then the job sets the dependencies correctly. The main block executes the job in process.

I should also make sure that all function names match the steps, like download_noaa, normalize_noaa, etc. The job function uses the ops in the correct order.

Wait, the merge task depends on all five normalization tasks. So in the job, the merge op's inputs are all the normalization ops' outputs. So in the job definition, merge_climate_data will have all five normalization ops as upstream dependencies.

Let me draft the code:

Start with imports: from dagster import op, job.

Define each download op with a dummy implementation. Then each normalize op takes the download's output. Merge takes all normalize outputs.

In the job, the dependencies are set so that each normalize step depends on its download, and merge depends on all normalizes.

The main block runs the job in process.

Check for PEP8 compliance, like function names in snake_case, proper spacing, etc.

Add minimal docstrings if needed, but the user said to include them where helpful. Maybe a one-liner for each op.

Resources and config: Since the user mentioned to provide stubs, maybe add a config comment for each download op, like # config_schema={"api_endpoint": str} etc., but as comments.

Retries and email: The user's constraints mention 3 retries with 5-minute delays. In Dagster, op retries can be configured via the @op with retry_policy. So add a retry_policy with max_retries=3 and delay=300 seconds. But since the code needs to be executable, I need to import RetryPolicy from dagster and set it in the op decorator. Wait, but the user said if something is unknown, add minimal safe defaults. So perhaps include the retry policy.

Wait, the user's constraints say to include minimal launch pattern. So the main block should have the job execution.

Email notifications on failure would require a resource, like an email client. Since that's an integration, maybe add a commented resource example but not implement it fully.

Putting it all together:

- Imports: from dagster import op, job, RetryPolicy

Each op has a retry_policy with max_retries=3, delay=300.

But wait, RetryPolicy is part of the op's configuration. So the op decorator would include retry_policy=RetryPolicy(...). Let me check the syntax.

Yes, in Dagster, you can define a RetryPolicy and attach it to an op. So for each op, perhaps add the retry policy. But since all ops have the same retry policy, maybe apply it to all. Alternatively, the user's pipeline has retries for the entire job. But the question says 3 retry attempts with 5-minute delays. So perhaps set the retry policy at the job level or per op.

But the user's code needs to be executable. So including RetryPolicy in the op decorators.

Wait, the user's pipeline has maximum parallel width of 5 during download and normalization. So Dagster will handle parallelism based on dependencies. Since all download ops are independent, they can run in parallel.

So code outline:

imports

define download ops with retry policy

define normalize ops with retry policy

define merge op with retry policy

define the job with dependencies

main block.

Testing each step:

download_noaa -> normalize_noaa

Similarly for others. Merge depends on all normalize ops.

Yes.

Now code:

from dagster import op, job, RetryPolicy

retry_policy = RetryPolicy(max_retries=3, delay=300)

@op(retry_policy=retry_policy)
def download_noaa():
    # Example: Fetch from NOAA FTP
    return "noaa_data.csv"

Similarly for other download ops.

Each normalize op takes the download output.

@op(retry_policy=retry_policy)
def normalize_noaa(download_noaa_output):
    # Convert to standard format
    return f"normalized_{download_noaa_output}"

Then merge:

@op(retry_policy=retry_policy)
def merge_climate_data(
    normalize_noaa_output,
    normalize_ecmwf_output,
    normalize_jma_output,
    normalize_metoffice_output,
    normalize_bom_output
):
    # Combine all datasets and save as Parquet
    combined = ", ".join([
        normalize_noaa_output,
        normalize_ecmwf_output,
        normalize_jma_output,
        normalize_metoffice_output,
        normalize_bom_output
    ])
    return f"merged_data.parquet: {combined}"

Then the job:

@job
def climate_data_fusion_job():
    noaa = download_noaa()
    ecmwf = download_ecmwf()
    jma = download_jma()
    metoffice = download_metoffice()
    bom = download_bom()

    normalized_noaa = normalize_noaa(noaa)
    normalized_ecmwf = normalize_ecmwf(ecmwf)
    normalized_jma = normalize_jma(jma)
    normalized_metoffice = normalize_metoffice(metoffice)
    normalized_bom = normalize_bom(bom)

    merge_climate_data(
        normalized_noaa,
        normalized_ecmwf,
        normalized_jma,
        normalized_metoffice,
        normalized_bom
    )

Main block:

if __name__ == "__main__":
    result = climate_data_fusion_job.execute_in_process()
    print(result)

This should satisfy all constraints. Each op has retries, dependencies are set correctly, parallelism is handled by Dagster's execution. The code is executable, uses minimal stubs, and follows PEP8.
</think>

from dagster import op, job, RetryPolicy

retry_policy = RetryPolicy(max_retries=3, delay=300)

@op(retry_policy=retry_policy)
def download_noaa():
    """Fetch NOAA weather station CSV data from FTP server"""
    # Example: return pd.read_csv(ftp://noaa.gov/data.csv)
    return "noaa_data.csv"

@op(retry_policy=retry_policy)
def download_ecmwf():
    """Fetch ECMWF weather station CSV data from HTTPS endpoint"""
    return "ecmwf_data.csv"

@op(retry_policy=retry_policy)
def download_jma():
    """Fetch JMA weather station CSV data from HTTPS endpoint"""
    return "jma_data.csv"

@op(retry_policy=retry_policy)
def download_metoffice():
    """Fetch MetOffice weather station CSV data from HTTPS endpoint"""
    return "metoffice_data.csv"

@op(retry_policy=retry_policy)
def download_bom():
    """Fetch BOM weather station CSV data from HTTPS endpoint"""
    return "bom_data.csv"

@op(retry_policy=retry_policy)
def normalize_noaa(download_noaa_output):
    """Convert NOAA data to standard format (ISO timestamp, Celsius, meters)"""
    return f"normalized_{download_noaa_output}"

@op(retry_policy=retry_policy)
def normalize_ecmwf(download_ecmwf_output):
    """Convert ECMWF data to standard format"""
    return f"normalized_{download_ecmwf_output}"

@op(retry_policy=retry_policy)
def normalize_jma(download_jma_output):
    """Convert JMA data to standard format"""
    return f"normalized_{download_jma_output}"

@op(retry_policy=retry_policy)
def normalize_metoffice(download_metoffice_output):
    """Convert MetOffice data to standard format"""
    return f"normalized_{download_metoffice_output}"

@op(retry_policy=retry_policy)
def normalize_bom(download_bom_output):
    """Convert BOM data to standard format"""
    return f"normalized_{download_bom_output}"

@op(retry_policy=retry_policy)
def merge_climate_data(
    normalize_noaa_output,
    normalize_ecmwf_output,
    normalize_jma_output,
    normalize_metoffice_output,
    normalize_bom_output
):
    """Combine all normalized datasets into unified Parquet format"""
    combined = ", ".join([
        normalize_noaa_output,
        normalize_ecmwf_output,
        normalize_jma_output,
        normalize_metoffice_output,
        normalize_bom_output
    ])
    return f"merged_data.parquet: {combined}"

@job
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

if __name__ == "__main__":
    result = climate_data_fusion_job.execute_in_process()
    print(result)