from prefect import flow, task

@task(name='download_bom', retries=3)
def download_bom():
    """Task: Download BOM Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_ecmwf', retries=3)
def download_ecmwf():
    """Task: Download ECMWF Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_jma', retries=3)
def download_jma():
    """Task: Download JMA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_metoffice', retries=3)
def download_metoffice():
    """Task: Download MetOffice Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_noaa', retries=3)
def download_noaa():
    """Task: Download NOAA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_bom', retries=3)
def normalize_bom():
    """Task: Normalize BOM Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_ecmwf', retries=3)
def normalize_ecmwf():
    """Task: Normalize ECMWF Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_jma', retries=3)
def normalize_jma():
    """Task: Normalize JMA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_metoffice', retries=3)
def normalize_metoffice():
    """Task: Normalize MetOffice Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_noaa', retries=3)
def normalize_noaa():
    """Task: Normalize NOAA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='merge_climate_data', retries=3)
def merge_climate_data():
    """Task: Merge Climate Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="download_noaa_pipeline")
def download_noaa_pipeline():
    # Entry point download tasks (can run in parallel)
    bom_download = download_bom()
    ecmwf_download = download_ecmwf()
    jma_download = download_jma()
    metoffice_download = download_metoffice()
    noaa_download = download_noaa()

    # Normalization tasks, each dependent on its respective download
    bom_normalized = normalize_bom(wait_for=[bom_download])
    ecmwf_normalized = normalize_ecmwf(wait_for=[ecmwf_download])
    jma_normalized = normalize_jma(wait_for=[jma_download])
    metoffice_normalized = normalize_metoffice(wait_for=[metoffice_download])
    noaa_normalized = normalize_noaa(wait_for=[noaa_download])

    # Merge task, fan‑in from all normalization tasks
    merged = merge_climate_data(
        wait_for=[
            bom_normalized,
            ecmwf_normalized,
            jma_normalized,
            metoffice_normalized,
            noaa_normalized,
        ]
    )
    return merged

if __name__ == "__main__":
    download_noaa_pipeline()