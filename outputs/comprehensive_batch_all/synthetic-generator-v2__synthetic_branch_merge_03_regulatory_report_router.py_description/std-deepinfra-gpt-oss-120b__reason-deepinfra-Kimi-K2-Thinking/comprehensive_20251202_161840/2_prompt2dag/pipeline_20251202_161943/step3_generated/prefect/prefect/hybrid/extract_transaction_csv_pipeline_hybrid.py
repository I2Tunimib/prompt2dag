from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='extract_transaction_csv', retries=2)
def extract_transaction_csv():
    """Task: Extract Transaction CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='branch_account_type_check', retries=2)
def branch_account_type_check():
    """Task: Account Type Branch Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_fatca_report', retries=2)
def generate_fatca_report():
    """Task: Generate FATCA Report"""
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


@flow(
    name="extract_transaction_csv_pipeline",
    task_runner=ConcurrentTaskRunner(),
)
def extract_transaction_csv_pipeline():
    # Entry point
    extract_future = extract_transaction_csv.submit()

    # Fan‑out: branch after extraction
    branch_future = branch_account_type_check.submit(wait_for=[extract_future])

    # Parallel generation of reports
    fatca_future = generate_fatca_report.submit(wait_for=[branch_future])
    irs_future = generate_irs_report.submit(wait_for=[branch_future])

    # Fan‑in: archive after both reports are ready
    archive_future = archive_regulatory_reports.submit(
        wait_for=[fatca_future, irs_future]
    )

    return archive_future


if __name__ == "__main__":
    extract_transaction_csv_pipeline()