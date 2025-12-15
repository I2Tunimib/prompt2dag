from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition

# -------------------------------------------------------------------------
# Task Definitions (provided exactly as given)
# -------------------------------------------------------------------------

@op(
    name='extract_transaction_csv',
    description='Extract Transaction CSV',
)
def extract_transaction_csv(context):
    """Op: Extract Transaction CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='branch_account_type_check',
    description='Account Type Branch Check',
)
def branch_account_type_check(context):
    """Op: Account Type Branch Check"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='generate_fatca_report',
    description='Generate FATCA Report',
)
def generate_fatca_report(context):
    """Op: Generate FATCA Report"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='generate_irs_report',
    description='Generate IRS Report',
)
def generate_irs_report(context):
    """Op: Generate IRS Report"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='archive_regulatory_reports',
    description='Archive Regulatory Reports',
)
def archive_regulatory_reports(context):
    """Op: Archive Regulatory Reports"""
    # Docker execution
    # Image: python:3.9
    pass


# -------------------------------------------------------------------------
# Job Definition with fanout/fanin pattern
# -------------------------------------------------------------------------

@job(
    name="extract_transaction_csv_pipeline",
    description="Comprehensive Pipeline Description",
    executor_def=multiprocess_executor,
    resource_defs={
        "conn_secure_archive": ResourceDefinition.hardcoded_resource(None),
        "conn_local_fs": ResourceDefinition.hardcoded_resource(None),
        "conn_fatca_system": ResourceDefinition.hardcoded_resource(None),
        "conn_irs_system": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def extract_transaction_csv_pipeline():
    # Entry point
    extract = extract_transaction_csv()

    # Fan‑out: branch after extraction
    branch = branch_account_type_check()
    extract >> branch

    # Fan‑out from branch to two parallel reports
    fatca_report = generate_fatca_report()
    irs_report = generate_irs_report()
    branch >> [fatca_report, irs_report]

    # Fan‑in: archive after both reports are ready
    archive = archive_regulatory_reports()
    [fatca_report, irs_report] >> archive

# End of file