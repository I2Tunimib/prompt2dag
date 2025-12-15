from dagster import op, job, In, Out, RetryPolicy, Field, String, execute_in_process

@op(
    config_schema={"csv_path": Field(String, is_required=True)},
    out=Out(dagster_type=list, description="List of transaction dictionaries")
)
def read_csv(context):
    """Reads financial transaction CSV file and returns a list of transaction dictionaries."""
    import csv
    csv_path = context.op_config["csv_path"]
    with open(csv_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        transactions = [row for row in reader]
    return transactions

@op(
    ins={"transactions": In(dagster_type=list, description="List of transaction dictionaries")},
    out={
        "domestic_transactions": Out(dagster_type=list, description="List of domestic transactions"),
        "international_transactions": Out(dagster_type=list, description="List of international transactions")
    }
)
def account_check(transactions):
    """Analyzes account types and routes transactions to appropriate reporting systems."""
    domestic_transactions = []
    international_transactions = []
    for transaction in transactions:
        if transaction["account_type"] == "domestic":
            domestic_transactions.append(transaction)
        elif transaction["account_type"] == "international":
            international_transactions.append(transaction)
    return {"domestic_transactions": domestic_transactions, "international_transactions": international_transactions}

@op(
    ins={"transactions": In(dagster_type=list, description="List of international transactions")},
    out=Out(dagster_type=list, description="List of FATCA XML reports")
)
def route_to_fatca(transactions):
    """Processes international accounts through FATCA regulatory reporting system."""
    # Simplified FATCA report generation
    fatca_reports = [f"FATCA Report for {transaction['account_number']}" for transaction in transactions]
    return fatca_reports

@op(
    ins={"transactions": In(dagster_type=list, description="List of domestic transactions")},
    out=Out(dagster_type=list, description="List of IRS Form 1099 reports")
)
def route_to_irs(transactions):
    """Processes domestic accounts through IRS regulatory reporting system."""
    # Simplified IRS report generation
    irs_reports = [f"IRS Form 1099 for {transaction['account_number']}" for transaction in transactions]
    return irs_reports

@op(
    ins={
        "fatca_reports": In(dagster_type=list, description="List of FATCA XML reports"),
        "irs_reports": In(dagster_type=list, description="List of IRS Form 1099 reports")
    },
    out=Out(dagster_type=str, description="Path to the archived reports")
)
def archive_reports(fatca_reports, irs_reports):
    """Merges and archives all regulatory reports."""
    import zipfile
    import os
    archive_path = "regulatory_reports.zip"
    with zipfile.ZipFile(archive_path, 'w') as archive:
        for report in fatca_reports + irs_reports:
            archive.writestr(f"{report}.xml", report)
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
    transactions = read_csv()
    account_check_result = account_check(transactions)
    fatca_reports = route_to_fatca(account_check_result["international_transactions"])
    irs_reports = route_to_irs(account_check_result["domestic_transactions"])
    archive_reports(fatca_reports, irs_reports)

if __name__ == '__main__':
    result = regulatory_report_router.execute_in_process()