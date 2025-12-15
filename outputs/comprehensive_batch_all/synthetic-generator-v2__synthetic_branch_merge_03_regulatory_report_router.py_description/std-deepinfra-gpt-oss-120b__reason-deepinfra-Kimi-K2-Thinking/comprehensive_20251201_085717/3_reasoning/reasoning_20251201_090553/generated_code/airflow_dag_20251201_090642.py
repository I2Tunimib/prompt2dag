from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


default_args = {
    'owner': 'financial_regulatory_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def read_csv(**context):
    """
    Reads financial transaction CSV file and returns data.
    Default path: /data/transactions.csv
    """
    import csv
    import os
    
    csv_path = '/data/transactions.csv'
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at {csv_path}")
    
    transactions = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            transactions.append(row)
    
    return transactions


def account_check(**context):
    """
    Analyzes account types and determines routing path.
    Returns task_id(s) for downstream branching.
    """
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='read_csv')
    
    if not transactions:
        raise ValueError("No transaction data available")
    
    account_types = set()
    for txn in transactions:
        acc_type = txn.get('account_type', '').lower()
        if 'international' in acc_type:
            account_types.add('international')
        elif 'domestic' in acc_type:
            account_types.add('domestic')
    
    if not account_types:
        raise ValueError("No valid account types found in transaction data")
    
    if account_types == {'international'}:
        return 'route_to_fatca'
    elif account_types == {'domestic'}:
        return 'route_to_irs'
    else:
        return ['route_to_fatca', 'route_to_irs']


def route_to_fatca(**context):
    """
    Processes international accounts through FATCA reporting system.
    Generates XML reports for international transactions.
    """
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='read_csv')
    
    international_txns = [
        txn for txn in transactions 
        if 'international' in txn.get('account_type', '').lower()
    ]
    
    if not international_txns:
        return "no_international_accounts"
    
    # Simulate FATCA XML report generation
    report_data = {
        'report_type': 'FATCA',
        'record_count': len(international_txns),
        'accounts_processed': [txn.get('account_id') for txn in international_txns],
        'status': 'generated'
    }
    
    return report_data


def route_to_irs(**context):
    """
    Processes domestic accounts through IRS reporting system.
    Generates Form 1099 data for domestic transactions.
    """
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='read_csv')
    
    domestic_txns = [
        txn for txn in transactions 
        if 'domestic' in txn.get('account_type', '').lower()
    ]
    
    if not domestic_txns:
        return "no_domestic_accounts"
    
    # Simulate IRS Form 1099 generation
    report_data = {
        'report_type': 'IRS_1099',
        'record_count': len(domestic_txns),
        'accounts_processed': [txn.get('account_id') for txn in domestic_txns],
        'status': 'generated'
    }
    
    return report_data


def archive_reports(**context):
    """
    Merges outputs from both reporting branches and archives them.
    Compresses reports and stores in secure location.
    """
    ti = context['ti']
    
    # Pull results from both reporting tasks
    fatca_result = ti.xcom_pull(task_ids='route_to_fatca')
    irs_result = ti.xcom_pull(task_ids='route_to_irs')
    
    # Merge reports
    merged_reports = []
    if fatca_result and isinstance(fatca_result, dict):
        merged_reports.append(fatca_result)
    if irs_result and isinstance(irs_result, dict):
        merged_reports.append(irs_result)
    
    if not merged_reports:
        raise ValueError("No reports available for archiving")
    
    # Simulate archiving process
    archive_summary = {
        'archive_timestamp': datetime.utcnow().isoformat(),
        'reports_archived': len(merged_reports),
        'archive_location': '/secure/archive/regulatory_reports/',
        'status': 'archived'
    }
    
    return archive_summary


with DAG(
    'regulatory_report_router',
    default_args=default_args,
    description='Routes financial transactions to appropriate regulatory reporting systems',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['regulatory', 'reporting', 'financial', 'fatca', 'irs'],
) as dag:
    
    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
    )
    
    account_check_task = BranchPythonOperator(
        task_id='account_check',
        python_callable=account_check,
    )
    
    route_to_fatca_task = PythonOperator(
        task_id='route_to_fatca',
        python_callable=route_to_fatca,
    )
    
    route_to_irs_task = PythonOperator(
        task_id='route_to_irs',
        python_callable=route_to_irs,
    )
    
    archive_reports_task = PythonOperator(
        task_id='archive_reports',
        python_callable=archive_reports,
        trigger_rule='none_failed_min_one_success',
    )
    
    # Define task dependencies
    read_csv_task >> account_check_task
    account_check_task >> [route_to_fatca_task, route_to_irs_task]
    [route_to_fatca_task, route_to_irs_task] >> archive_reports_task