from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='start_pipeline', retries=0)
def start_pipeline():
    """Task: Start Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='ingest_apac', retries=2)
def ingest_apac():
    """Task: Ingest APAC Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='ingest_eu', retries=2)
def ingest_eu():
    """Task: Ingest EU Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='ingest_us_east', retries=2)
def ingest_us_east():
    """Task: Ingest US‑East Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='ingest_us_west', retries=2)
def ingest_us_west():
    """Task: Ingest US‑West Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='convert_currency_apac', retries=2)
def convert_currency_apac():
    """Task: Convert APAC Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='convert_currency_eu', retries=2)
def convert_currency_eu():
    """Task: Convert EU Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='convert_currency_us_east', retries=2)
def convert_currency_us_east():
    """Task: Convert US‑East Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='convert_currency_us_west', retries=2)
def convert_currency_us_west():
    """Task: Convert US‑West Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='aggregate_global_revenue', retries=2)
def aggregate_global_revenue():
    """Task: Aggregate Global Revenue"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='end_pipeline', retries=0)
def end_pipeline():
    """Task: End Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="start_pipeline_pipeline", task_runner=ConcurrentTaskRunner())
def start_pipeline_pipeline():
    # Entry point
    start = start_pipeline.submit()

    # Fan‑out: ingest tasks
    ingest_us_east_fut = ingest_us_east.submit(wait_for=[start])
    ingest_us_west_fut = ingest_us_west.submit(wait_for=[start])
    ingest_eu_fut = ingest_eu.submit(wait_for=[start])
    ingest_apac_fut = ingest_apac.submit(wait_for=[start])

    # Fan‑out: conversion tasks (each depends on its ingest)
    convert_us_east_fut = convert_currency_us_east.submit(wait_for=[ingest_us_east_fut])
    convert_us_west_fut = convert_currency_us_west.submit(wait_for=[ingest_us_west_fut])
    convert_eu_fut = convert_currency_eu.submit(wait_for=[ingest_eu_fut])
    convert_apac_fut = convert_currency_apac.submit(wait_for=[ingest_apac_fut])

    # Fan‑in: aggregate global revenue
    aggregate_fut = aggregate_global_revenue.submit(
        wait_for=[convert_us_east_fut, convert_us_west_fut, convert_eu_fut, convert_apac_fut]
    )

    # End of pipeline
    end_pipeline.submit(wait_for=[aggregate_fut])


# If this script is executed directly, run the flow
if __name__ == "__main__":
    start_pipeline_pipeline()