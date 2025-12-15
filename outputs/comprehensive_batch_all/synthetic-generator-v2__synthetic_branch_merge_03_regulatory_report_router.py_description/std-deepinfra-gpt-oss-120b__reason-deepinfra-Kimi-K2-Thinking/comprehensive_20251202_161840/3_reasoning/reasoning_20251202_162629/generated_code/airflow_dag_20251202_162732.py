from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'financial-compliance',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def read_csv(**context):
    """Read financial transaction CSV and push to XCom."""
    transactions = [
        {'account_id': 'INT001', 'account_type': 'international', 'amount': 50000},
        {'account_id': 'DOM001', 'account_type': 'domestic', 'amount': 25000},
        {'account_id': 'INT002', 'account_type': 'international', 'amount': 75000},
    ]
    context['ti'].xcom_push(key='transactions', value=transactions)
    return 'CSV data read successfully'

def account_check(**context):
    """Analyze account types and determine routing path."""
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='read_csv', key='transactions') or []
    
    has_international = any(t.get('account_type') == 'international' for t in transactions)
    has_domestic = any(t.get('account_type') == 'domestic' for t in transactions)
    
    if has_international and has_domestic:
        return ['route_to_fatca', 'route_to_irs']
    elif has_international:
        return 'route_to_fatca'
    elif has_domestic:
        return 'route_to_irs'
    return 'archive_reports'

def route_to_fatca(**context):
    """Process international accounts through FATCA reporting."""
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='read_csv', key='transactions') or []
    international_txns = [t for t in transactions if t.get('account_type') == 'international']
    
    report_path = f"/tmp/fatca_report_{context['ds']}.xml"
    ti.xcom_push(key='fatca_report', value=report_path)
    return f'FATCA report generated for {len(international_txns)} transactions'

def route_to_irs(**context):
    """Process domestic accounts through IRS reporting."""
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='read_csv', key='transactions') or []
    domestic_txns = [t for t in transactions if t.get('account_type') == 'domestic']
    
    report_path = f"/tmp/irs_report_{context['ds']}.txt"
    ti.xcom_push(key='irs_report', value=report_path)
    return f'IRS report generated for {len(domestic_txns)} transactions'

def archive_reports(**context):
    """Merge and archive all regulatory reports."""
    ti = context['ti']
    fatca_report = ti.xcom_pull(task_ids='route_to_fatca', key='fatca_report')
    irs_report = ti.xcom_pull(task_ids='route_to_irs', key='irs_report')
    
    reports = [r for r in [fatca_report, irs_report] if r]
    if not reports:
        return 'No reports to archive'
    
    archive_path = f"/tmp/archive_{context['ds']}.tar.gz"
    return f'Reports archived to {archive_path}: {reports}'

with DAG(
    'regulatory_report_router',
    default_args=default_args,
    description='Financial transaction regulatory report router with branch-merge pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['regulatory', 'compliance', 'financial'],
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
    
    read_csv_task >> account_check_task
    account_check_task >> [route_to_fatca_task, route_to_irs_task]
    route_to_fatca_task >> archive_reports_task
    route_to_irs_task >> archive_reports_task