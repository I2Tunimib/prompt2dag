from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='ingest_csv', retries=1)
def ingest_csv():
    """Task: Ingest Customer CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='quality_check', retries=1)
def quality_check():
    """Task: Assess Data Quality"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='production_load', retries=1)
def production_load():
    """Task: Load High-Quality Data to Production"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='quarantine_and_alert', retries=1)
def quarantine_and_alert():
    """Task: Quarantine Low-Quality Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='send_alert_email', retries=1)
def send_alert_email():
    """Task: Send Quality Alert Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='cleanup', retries=1)
def cleanup():
    """Task: Cleanup Temporary Resources"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="data_quality_gate", task_runner=ConcurrentTaskRunner())
def data_quality_gate():
    """Main flow implementing the data quality gate with fanout/fanin pattern."""
    # Entry point
    ingest = ingest_csv()

    # Fan‑out after quality check
    qc = quality_check.wait_for(ingest)()
    prod = production_load.wait_for(qc)()
    quarantine = quarantine_and_alert.wait_for(qc)()

    # Chain from quarantine to alert
    alert = send_alert_email.wait_for(quarantine)()

    # Fan‑in: cleanup after both production load and alert
    cleanup.wait_for(prod, alert)()


if __name__ == "__main__":
    data_quality_gate()