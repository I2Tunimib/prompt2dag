<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements. The pipeline has download tasks that run in parallel, followed by normalization tasks that also run in parallel, each depending on their respective download. Then a merge task that depends on all normalizations.

First, I'll create a @flow for the main orchestration. Each step (download, normalize, merge) will be @task decorated functions. For parallel execution, I'll use .submit() on the tasks. Since the downloads are parallel, each download task should be submitted asynchronously. Then each normalization task depends on its download, so they should wait for their specific download to complete. The merge task needs all normalization tasks to finish, so I'll collect their results and pass them to merge.

Wait, how do I handle the dependencies? Each normalization task must run after its download. So for example, normalize_noaa depends on download_noaa. So in the flow, after submitting all downloads, I need to submit each normalization with the corresponding download future. Then wait for all normalization futures to complete before merging.

Prefect 2's task.submit() returns a future, so I can pass those futures into the normalization tasks. But tasks can't take futures as arguments directly unless they're waiting for them. Hmm, maybe I should call .result() on the download futures inside the normalization tasks. Or perhaps the normalization tasks should be called with the download task's future, and within the normalization task, retrieve the result.

Wait, no. The tasks are functions, so when you submit a download task, it returns a future. Then, when you submit a normalization task, you pass that future as an argument. Prefect will handle the dependency automatically because the normalization task can't run until the download future is resolved. So in the flow function, after submitting all downloads, I submit each normalization task with the corresponding download future. Then collect all normalization futures and pass them to the merge task.

So the flow would look like:

@flow
def climate_data_fusion_pipeline():
    noaa_future = download_noaa.submit()
    ecmwf_future = download_ecmwf.submit()
    jma_future = download_jma.submit()
    metoffice_future = download_metoffice.submit()
    bom_future = download_bom.submit()

    noaa_norm_future = normalize_noaa.submit(noaa_future)
    ecmwf_norm_future = normalize_ecmwf.submit(ecmwf_future)
    jma_norm_future = normalize_jma.submit(jma_future)
    metoffice_norm_future = normalize_metoffice.submit(metoffice_future)
    bom_norm_future = normalize_bom.submit(bom_future)

    # Wait for all normalization tasks to complete
    all_norm = [noaa_norm_future, ecmwf_norm_future, jma_norm_future, metoffice_norm_future, bom_norm_future]
    merge_climate_data.submit(all_norm)

Wait, but the merge task needs the results of all normalization tasks. So maybe the merge task should be called normally (not submitted) after waiting for all normalization futures. Because if you submit merge, it might not wait. Alternatively, collect all the results first.

Alternatively, use wait=True when submitting the merge task. Or perhaps, since the merge task depends on all normalization tasks, we can gather all the normalization futures and pass them to merge.submit(), but Prefect will handle the dependencies.

Wait, but in Prefect, when you submit a task, it returns a future. If another task is submitted and takes that future as an argument, Prefect knows to wait for the future to complete before running the dependent task. So in this case, each normalization task is submitted with the download future, so Prefect will wait for the download to finish before starting the normalization. Then, the merge task is submitted with all normalization futures, so Prefect will wait for all normalization tasks to finish before starting the merge.

But in the code above, when we call merge_climate_data.submit(all_norm), the all_norm is a list of futures. The merge task would receive these futures as arguments. However, the merge task's function expects actual data, not futures. So inside the merge_climate_data task function, we need to retrieve the results from each future. But wait, when you pass a future to a task, Prefect automatically resolves it by calling .result() on it. So in the task function, the parameters will be the results of the previous tasks.

Wait, no. When using .submit(), the arguments are passed as futures, but when the task runs, Prefect will resolve them. So if a task function is called with a future as an argument, Prefect will wait for that future to complete and pass the result to the task. So in the normalize tasks, their functions take the output of the download tasks. So when you submit normalize_noaa with the download_noaa future, Prefect will wait for download_noaa to finish, get its result, and pass it to normalize_noaa.

Similarly, the merge task takes the results of all normalization tasks. So when you pass the list of normalization futures to merge_climate_data.submit(), Prefect will wait for all those futures to complete, collect their results into a list, and pass that list to the merge task.

So the code structure would be:

In the flow:

download futures are submitted.

Then normalization futures are submitted, each taking the corresponding download future.

Then merge is submitted, taking the list of normalization futures.

But wait, when you submit merge_climate_data.submit(all_norm), all_norm is a list of futures. Prefect will handle each element in the list as a dependency. So the merge task will wait for all normalization tasks to complete, collect their results into a list, and pass that list to the merge task.

So the merge_climate_data task function would receive a list of the normalized data from each normalization task.

But according to the problem statement, the data is passed between tasks via XCom. So each task's output is stored in XCom, and the next task retrieves it. So when using .submit(), Prefect handles XCom automatically.

Now, the task functions need to return the data that the next tasks require. For example, download_noaa returns the data (or a reference to it), which is passed to normalize_noaa, which returns normalized data, passed to merge.

But in the code, the actual implementation of the tasks is left as placeholders. For example, the download tasks might return a file path or a DataFrame, and the normalize tasks process that and return another DataFrame, etc.

So the code will have stubs for each task function, with appropriate parameters and returns.

Now, considering retries and notifications: The user mentioned 3 retry attempts with 5-minute delays, email notifications on failure. These can be set with task decorator parameters like retries, retry_delay_seconds, and possibly on_failure hooks. But since the code needs to be executable, and email setup might require Prefect server or cloud, perhaps adding the retry parameters but leaving notifications as comments.

So for each task, add @task(retries=3, retry_delay_seconds=300). But wait, the pipeline steps mention these settings apply to the entire flow. Wait, the user says "3 retry attempts with 5-minute delays" — are these per task or per flow? Probably per task. So each task should have retries=3 and retry_delay_seconds=5*60=300.

But adding that to each task would be repetitive. Alternatively, could create a decorator with those parameters, but since the code needs to be explicit, perhaps adding them to each @task decorator.

But the problem says "No catch-up for missed runs" — that's a schedule setting, which would be in the deployment configuration. So in the code, perhaps a comment indicating the schedule.

The user also mentioned a daily schedule, so in the code, include a comment about deployment configuration with a schedule.

Putting it all together:

The code will start with importing flow and task from prefect.

Then define each download task as a function with @task(retries=3, retry_delay_seconds=300). Each download function would have a docstring and a placeholder implementation, perhaps raising NotImplementedError, but the user said to avoid placeholders and use safe defaults. So maybe the download functions return a dummy path or data, just to make the code executable.

Similarly for normalization tasks and merge.

The flow function will orchestrate the tasks with .submit() for parallel steps.

The merge task is called after all normalization tasks complete. Since all normalization tasks are submitted, their futures are collected into a list and passed to merge_climate_data.submit().

Wait, but when you pass a list of futures to a task, Prefect will wait for all of them and pass their results as a list. So in the merge task, the parameter would be a list of the normalized data from each agency.

So the code outline is:

from prefect import flow, task

@task(retries=3, retry_delay_seconds=300)
def download_noaa():
    # implementation returning data

Similarly for other download tasks.

@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(data):
    # process data
    return normalized_data

Same for other normalize tasks.

@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_data_sets):
    # merge and save as parquet
    return output_path

@flow
def climate_data_fusion_pipeline():
    # submit all downloads
    d1 = download_noaa.submit()
    d2 = download_ecmwf.submit()
    d3 = download_jma.submit()
    d4 = download_metoffice.submit()
    d5 = download_bom.submit()

    # submit all normalizations with respective download futures
    n1 = normalize_noaa.submit(d1)
    n2 = normalize_ecmwf.submit(d2)
    n3 = normalize_jma.submit(d3)
    n4 = normalize_metoffice.submit(d4)
    n5 = normalize_bom.submit(d5)

    # submit merge with all normalization futures
    merge_task = merge_climate_data.submit([n1, n2, n3, n4, n5])

    return merge_task

if __name__ == "__main__":
    climate_data_fusion_pipeline()

Wait, but when you pass a list of futures to merge_climate_data.submit(), Prefect will resolve each future in the list and pass their results as a list to the merge task. So the merge task's function should expect a list of the normalized datasets.

But in the code above, the merge_climate_data task is defined to take 'normalized_data_sets' as a parameter, which would be a list of the five normalized datasets.

Now, implementing the task functions with minimal code to make it executable. For example, the download tasks could return a dummy string representing the data, and the normalize tasks process that string into another format. The merge task combines them.

But the user said to avoid placeholders like <TODO>, so add minimal, safe defaults. For example, the download tasks could return a dictionary with some sample data, and the normalize tasks convert it, etc.

So for download_noaa:

@task(retries=3, retry_delay_seconds=300)
def download_noaa():
    """Fetches NOAA weather station CSV data from FTP server."""
    # Example implementation (simulated)
    return {"data": "NOAA raw data", "format": "CSV"}

Similarly for other download tasks, changing the data string.

Normalize tasks:

@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(raw_data):
    """Converts NOAA data to standard format."""
    # Example normalization
    return {
        "timestamp": "ISO",
        "temperature": "Celsius",
        "precipitation": "meters",
        "source": "NOAA",
        "data": raw_data["data"] + " normalized"
    }

Merge task:

@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_data_sets):
    """Combines all normalized datasets into unified Parquet format."""
    # Example merge implementation
    merged = {"sources": [d["source"] for d in normalized_data_sets]}
    # Simulate writing to Parquet
    output_path = "/data/climate/unified_dataset.parquet"
    print(f"Merged data saved to {output_path}")
    return output_path

Putting it all together, the code would be structured with all tasks and the flow. Also, the if __name__ guard to run the flow.

Regarding scheduling: The user mentioned daily execution, so add a comment about deployment configuration. For example:

# To deploy with a daily schedule:
# prefect deployment build ./module.py:climate_data_fusion_pipeline --name climate_daily --schedule "0 0 * * *" --cron

But the user said to include a brief comment if schedules are mentioned. So adding a comment in the code.

Also, the email notifications on task failures would require Prefect notifications setup, which might be beyond the code here. So perhaps add a comment in the task decorator about notifications, but since the code needs to be executable, maybe omit it or mention it in a comment.

Wait, the user's constraints say: "If schedules are mentioned, include a brief comment indicating deployment/schedule configuration (optional)." So add a comment.

So in the code, after the flow definition, perhaps:

# To configure daily schedule:
# Use `prefect deployment build` with cron schedule "0 0 * * *"

But the exact comment can be placed near the top or near the if __name__ block.

Putting all together, here's the code.
</think>

from prefect import flow, task

@task(retries=3, retry_delay_seconds=300)
def download_noaa():
    """Fetches NOAA weather station CSV data from FTP server."""
    return {"data": "NOAA raw data", "format": "CSV"}

@task(retries=3, retry_delay_seconds=300)
def download_ecmwf():
    """Fetches ECMWF weather station CSV data from HTTPS endpoint."""
    return {"data": "ECMWF raw data", "format": "CSV"}

@task(retries=3, retry_delay_seconds=300)
def download_jma():
    """Fetches JMA weather station CSV data from HTTPS endpoint."""
    return {"data": "JMA raw data", "format": "CSV"}

@task(retries=3, retry_delay_seconds=300)
def download_metoffice():
    """Fetches MetOffice weather station CSV data from HTTPS endpoint."""
    return {"data": "MetOffice raw data", "format": "CSV"}

@task(retries=3, retry_delay_seconds=300)
def download_bom():
    """Fetches BOM weather station CSV data from HTTPS endpoint."""
    return {"data": "BOM raw data", "format": "CSV"}

@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(raw_data):
    """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)."""
    return {
        "timestamp": "ISO",
        "temperature": "Celsius",
        "precipitation": "meters",
        "source": "NOAA",
        "data": raw_data["data"] + " normalized"
    }

@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(raw_data):
    """Converts ECMWF data to standard format."""
    return {
        "timestamp": "ISO",
        "temperature": "Celsius",
        "precipitation": "meters",
        "source": "ECMWF",
        "data": raw_data["data"] + " normalized"
    }

@task(retries=3, retry_delay_seconds=300)
def normalize_jma(raw_data):
    """Converts JMA data to standard format."""
    return {
        "timestamp": "ISO",
        "temperature": "Celsius",
        "precipitation": "meters",
        "source": "JMA",
        "data": raw_data["data"] + " normalized"
    }

@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(raw_data):
    """Converts MetOffice data to standard format."""
    return {
        "timestamp": "ISO",
        "temperature": "Celsius",
        "precipitation": "meters",
        "source": "MetOffice",
        "data": raw_data["data"] + " normalized"
    }

@task(retries=3, retry_delay_seconds=300)
def normalize_bom(raw_data):
    """Converts BOM data to standard format."""
    return {
        "timestamp": "ISO",
        "temperature": "Celsius",
        "precipitation": "meters",
        "source": "BOM",
        "data": raw_data["data"] + " normalized"
    }

@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_data_sets):
    """Combines all normalized datasets into unified Parquet format."""
    merged = {"sources": [d["source"] for d in normalized_data_sets]}
    output_path = "/data/climate/unified_dataset.parquet"
    print(f"Merged data from {len(normalized_data_sets)} sources saved to {output_path}")
    return output_path

@flow
def climate_data_fusion_pipeline():
    # Deployment configuration: Use cron schedule "0 0 * * *" for daily execution
    noaa_data = download_noaa.submit()
    ecmwf_data = download_ecmwf.submit()
    jma_data = download_jma.submit()
    metoffice_data = download_metoffice.submit()
    bom_data = download_bom.submit()

    noaa_norm = normalize_noaa.submit(noaa_data)
    ecmwf_norm = normalize_ecmwf.submit(ecmwf_data)
    jma_norm = normalize_jma.submit(jma_data)
    metoffice_norm = normalize_metoffice.submit(metoffice_data)
    bom_norm = normalize_bom.submit(bom_data)

    merge_climate_data.submit([noaa_norm, ecmwf_norm, jma_norm, metoffice_norm, bom_norm])

if __name__ == "__main__":
    climate_data_fusion_pipeline()