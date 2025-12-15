from dagster import op, job, In, Out, RetryPolicy, Field, String, execute_in_process

@op(
    config_schema={"csv_path": Field(String, is_required=True)},
    out=Out(dagster_type=list, description="List of dictionaries representing CSV rows")
)
def read_csv(context):
    """Reads financial transaction CSV file."""
    import csv
    csv_path = context.op_config["csv_path"]
    with open(csv_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data

@op(
    ins={"data": In(dagster_type=list, description="List of dictionaries representing CSV rows")},
    out=Out(dagster_type=dict, description="Dictionary with lists of domestic and international accounts")
)
def account_check(data):
    """Analyzes account types and determines routing path."""
    domestic_accounts = []
    international_accounts = []
    for row in data:
        if row['account_type'] == 'domestic':
            domestic_accounts.append(row)
        elif row['account_type'] == 'international':
            international_accounts.append(row)
    return {"domestic": domestic_accounts, "international": international_accounts}

@op(
    ins={"data": In(dagster_type=list, description="List of dictionaries representing international accounts")},
    out=Out(dagster_type=list, description="List of generated FATCA XML reports")
)
def route_to_fatca(data):
    """Processes international accounts through FATCA regulatory reporting system."""
    reports = []
    for row in data:
        report = f"<FATCAReport><Account>{row['account_number']}</Account></FATCAReport>"
        reports.append(report)
    return reports

@op(
    ins={"data": In(dagster_type=list, description="List of dictionaries representing domestic accounts")},
    out=Out(dagster_type=list, description="List of generated IRS Form 1099 data")
)
def route_to_irs(data):
    """Processes domestic accounts through IRS regulatory reporting system."""
    reports = []
    for row in data:
        report = f"<IRSReport><Account>{row['account_number']}</Account></IRSReport>"
        reports.append(report)
    return reports

@op(
    ins={
        "fatca_reports": In(dagster_type=list, description="List of generated FATCA XML reports"),
        "irs_reports": In(dagster_type=list, description="List of generated IRS Form 1099 data")
    },
    out=Out(dagster_type=str, description="Path to the archived reports")
)
def archive_reports(fatca_reports, irs_reports):
    """Merges outputs from both reporting branches, compresses all regulatory reports, and stores them in secure archive location."""
    import zipfile
    import os
    archive_path = "reports_archive.zip"
    with zipfile.ZipFile(archive_path, 'w') as archive:
        for i, report in enumerate(fatca_reports):
            archive.writestr(f"fatca_report_{i}.xml", report)
        for i, report in enumerate(irs_reports):
            archive.writestr(f"irs_report_{i}.xml", report)
    return archive_path

@job(
    resource_defs={},
    config={
        "ops": {
            "read_csv": {
                "config": {
                    "csv_path": "transactions.csv"
                }
            }
        }
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def regulatory_report_router():
    data = read_csv()
    account_types = account_check(data)
    fatca_reports = route_to_fatca(account_types["international"])
    irs_reports = route_to_irs(account_types["domestic"])
    archive_reports(fatca_reports, irs_reports)

if __name__ == '__main__':
    result = regulatory_report_router.execute_in_process()