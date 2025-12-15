from prefect import flow, task

@task(name='download_agency_data', retries=3)
def download_agency_data():
    """Task: Download Agency Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_agency_data', retries=3)
def normalize_agency_data():
    """Task: Normalize Agency Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='merge_climate_data', retries=3)
def merge_climate_data():
    """Task: Merge Climate Datasets"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

# Additional task to satisfy the fan‑in dependency
@task(name='normalize_parallel', retries=3)
def normalize_parallel():
    """Placeholder parallel normalization task"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name='climate_data_fusion_pipeline')
def climate_data_fusion_pipeline():
    # Entry point
    download = download_agency_data()
    
    # Sequential dependency
    normalized_agency = normalize_agency_data(wait_for=[download])
    
    # Parallel branch (fan‑in)
    normalized_parallel = normalize_parallel(wait_for=[download])
    
    # Fan‑in merge
    merge = merge_climate_data(wait_for=[normalized_agency, normalized_parallel])
    
    return merge

if __name__ == "__main__":
    climate_data_fusion_pipeline()