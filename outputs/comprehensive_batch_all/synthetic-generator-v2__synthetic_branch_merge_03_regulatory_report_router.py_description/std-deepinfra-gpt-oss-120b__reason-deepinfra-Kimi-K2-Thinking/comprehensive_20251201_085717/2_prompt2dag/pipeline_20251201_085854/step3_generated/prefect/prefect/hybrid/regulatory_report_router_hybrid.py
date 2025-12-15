from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='extract_transaction_csv', retries=2)
def extract_transaction_csv():
    """Task: Extract Transaction CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='determine_account_routing', retries=2)
def determine_account_routing():
    """Task: Determine Account Routing"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_irs_report', retries=2)
def generate_irs_report():
    """Task: Generate IRS Report"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='archive_regulatory_reports', retries=2)
def archive_regulatory_reports():
    """Task: Archive Regulatory Reports"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_fatca_report', retries=2)
def generate_fatca_report():
    """Task: Generate FATCA Report"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="regulatory_report_router", task_runner=ConcurrentTaskRunner())
def regulatory_report_router():
    """
    Regulatory reporting pipeline that extracts transaction data,
    determines routing, runs parallel reporting workflows for IRS and FATCA,
    and archives the combined results.
    """
    # Entry point
    csv_data = extract_transaction_csv()

    # Branching: determine routing based on extracted CSV
    routing_info = determine_account_routing(csv_data)

    # Fan‑out: run both reporting tasks in parallel
    fatca_report = generate_fatca_report(routing_info)
    irs_report = generate_irs_report(routing_info)

    # Fan‑in: archive after both reports are completed
    archive = archive_regulatory_reports(fatca_report, irs_report)

    return archive


if __name__ == "__main__":
    regulatory_report_router()