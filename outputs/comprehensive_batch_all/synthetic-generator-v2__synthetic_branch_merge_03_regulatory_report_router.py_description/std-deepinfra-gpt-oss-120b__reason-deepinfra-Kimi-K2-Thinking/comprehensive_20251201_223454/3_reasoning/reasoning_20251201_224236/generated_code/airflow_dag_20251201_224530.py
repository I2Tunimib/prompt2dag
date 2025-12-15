from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


default_args = {
    'owner': 'financial_compliance',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def read_csv(**context):
    """Reads financial transaction CSV and pushes data to XCom."""
    transactions = [
        {'id': 1, 'account_type': 'international', 'amount': 1000.00, 'currency': 'USD'},
        {'id': 2, 'account_type': 'domestic', 'amount': 500.00, 'currency': 'USD'},
        {'id': 3, 'account_type': 'international', 'amount': 2000.00, 'currency': 'EUR'},
        {'id': 4, 'account_type': 'domestic', 'amount': 750.00, 'currency': 'USD'},
    ]
    
    context['task_instance'].xcom_push(key='transactions', value=transactions)
    return f"Successfully read {len(transactions)} transactions"


def account_check(**context):
    """Analyzes account types and determines routing path for branching."""
    transactions = context['task_instance'].xcom_pull(
        task_ids='read_csv', 
        key='transactions'
    )
    
    if not transactions:
        return 'archive_reports'
    
    has_international = any(t.get('account_type') == 'international' for t in transactions)
    has_domestic = any(t.get('account_type') == 'domestic' for t in transactions)
    
    if has_international and has_domestic:
        return ['route_to_fatca', 'route_to_irs']
    elif has_international:
        return 'route_to_fatca'
    elif has_domestic:
        return 'route_to_irs'
    else:
        return 'archive_reports'


def route_to_fatca(**context):
    """Processes international accounts through FATCA reporting system."""
    transactions = context['task_instance'].xcom_pull(
        task_ids='read_csv', 
        key='transactions'
    )
    
    international_txns = [
        t for t in transactions 
        if t.get('account_type') == 'international'
    ]
    
    fatca_reports = [
        {
            'transaction_id': t['id'],
            'report_type': 'FATCA_XML',
            'amount': t['amount'],
            'currency': t['currency']
        } 
        for t in international_txns
    ]
    
    context['task_instance'].xcom_push(key='fatca_reports', value=fatca_reports)
    return f"Generated {len(fatca_reports)} FATCA reports"


def route_to_irs(**context):
    """Processes domestic accounts through IRS reporting system."""
    transactions = context['task_instance'].xcom_pull(
        task_ids='read_csv', 
        key='transactions'
    )
    
    domestic_txns = [
        t for t in transactions 
        if t.get('account_type') == 'domestic'
    ]
    
    irs_reports = [
        {
            'transaction_id': t['id'],
            'form_type': '1099',
            'amount': t['amount'],
            'currency': t['currency']
        } 
        for t in domestic_txns
    ]
    
    context['task_instance'].xcom_push(key='irs_reports', value=irs_reports)
    return f"Generated {len(irs_reports)} IRS reports"


def archive_reports(**context):
    """Merges and archives reports from both regulatory branches."""
    fatca_reports = context['task_instance'].xcom_pull(
        task_ids='route_to_fatca', 
        key='fatca_reports'
    ) or []
    
    irs_reports = context['task_instance'].xcom_pull(
        task_ids='route_to_irs', 
        key='irs_reports'
    ) or []
    
    # Handle potential None values from skipped branches
    fatca_reports = fatca_reports if isinstance(fatca_reports, list) else []
    irs_reports = irs_reports if isinstance(irs_reports, list) else []
    
    all_reports = fatca_reports + irs_reports
    
    print(f"Archiving {len(all_reports)} total regulatory reports")
    
    return f"Archived {len(all_reports)} reports"


with DAG(
    dag_id='regulatory_report_router',
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
    
    # Linear extraction -> conditional branching -> parallel transformation -> synchronized merge
    read_csv_task >> account_check_task
    account_check_task >> [route_to_fatca_task, route_to_irs_task]
    route_to_fatca_task >> archive_reports_task
    route_to_irs_task >> archive_reports_task