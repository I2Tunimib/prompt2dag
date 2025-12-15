from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule


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


@task(name='cleanup', retries=1)
def cleanup():
    """Task: Final Cleanup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='production_load', retries=1)
def production_load():
    """Task: Load High‑Quality Data to Production"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='send_alert_email', retries=1)
def send_alert_email():
    """Task: Send Quality Alert Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='quarantine_and_alert', retries=1)
def quarantine_and_alert():
    """Task: Quarantine Low‑Quality Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="data_quality_gate")
def data_quality_gate():
    """Main pipeline flow implementing fanout-fanin pattern."""
    # Entry point
    ingest = ingest_csv()

    # Quality check depends on ingest
    qc = quality_check(wait_for=[ingest])

    # Fan‑out: two parallel branches after quality check
    prod = production_load(wait_for=[qc])
    quarantine = quarantine_and_alert(wait_for=[qc])

    # Alert after quarantine
    alert = send_alert_email(wait_for=[quarantine])

    # Fan‑in: cleanup after both production load and alert
    cleanup(wait_for=[prod, alert])


# Deployment configuration
DeploymentSpec(
    name="data_quality_gate_deployment",
    flow=data_quality_gate,
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
    work_pool_name="default-agent-pool",
    task_runner=ConcurrentTaskRunner(),
)