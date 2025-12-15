from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def read_csv(**kwargs):
    """Reads the financial transaction CSV file and returns the DataFrame."""
    file_path = '/path/to/financial_transactions.csv'
    df = pd.read_csv(file_path)
    kwargs['ti'].xcom_push(key='transaction_data', value=df.to_dict())
    return df

def account_check(**kwargs):
    """Analyzes account types and determines the routing path."""
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(key='transaction_data'))
    international_accounts = df[df['account_type'] == 'international']
    domestic_accounts = df[df['account_type'] == 'domestic']
    
    if not international_accounts.empty and not domestic_accounts.empty:
        return ['route_to_fatca', 'route_to_irs']
    elif not international_accounts.empty:
        return 'route_to_fatca'
    elif not domestic_accounts.empty:
        return 'route_to_irs'
    else:
        return 'no_accounts_to_process'

def route_to_fatca(**kwargs):
    """Processes international accounts through FATCA regulatory reporting system."""
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(key='transaction_data'))
    international_accounts = df[df['account_type'] == 'international']
    # Process international accounts and generate XML reports
    # For demonstration, we'll just print the account count
    print(f"Processing {len(international_accounts)} international accounts through FATCA.")
    ti.xcom_push(key='fatca_reports', value=international_accounts.to_dict())

def route_to_irs(**kwargs):
    """Processes domestic accounts through IRS regulatory reporting system."""
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(key='transaction_data'))
    domestic_accounts = df[df['account_type'] == 'domestic']
    # Process domestic accounts and generate Form 1099 data
    # For demonstration, we'll just print the account count
    print(f"Processing {len(domestic_accounts)} domestic accounts through IRS.")
    ti.xcom_push(key='irs_reports', value=domestic_accounts.to_dict())

def archive_reports(**kwargs):
    """Merges outputs from both reporting branches, compresses all regulatory reports, and stores them in a secure archive location."""
    ti = kwargs['ti']
    fatca_reports = pd.DataFrame(ti.xcom_pull(key='fatca_reports'))
    irs_reports = pd.DataFrame(ti.xcom_pull(key='irs_reports'))
    merged_reports = pd.concat([fatca_reports, irs_reports])
    # Archive the merged reports
    # For demonstration, we'll just print the merged report count
    print(f"Archiving {len(merged_reports)} merged regulatory reports.")
    # Save to a secure archive location
    merged_reports.to_csv('/path/to/secure_archive/merged_reports.csv', index=False)

def no_accounts_to_process(**kwargs):
    """Handles the case where no accounts are found to process."""
    print("No accounts to process.")

with DAG(
    dag_id='regulatory_report_router',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        provide_context=True
    )

    account_check_task = BranchPythonOperator(
        task_id='account_check',
        python_callable=account_check,
        provide_context=True
    )

    route_to_fatca_task = PythonOperator(
        task_id='route_to_fatca',
        python_callable=route_to_fatca,
        provide_context=True
    )

    route_to_irs_task = PythonOperator(
        task_id='route_to_irs',
        python_callable=route_to_irs,
        provide_context=True
    )

    no_accounts_to_process_task = PythonOperator(
        task_id='no_accounts_to_process',
        python_callable=no_accounts_to_process,
        provide_context=True
    )

    archive_reports_task = PythonOperator(
        task_id='archive_reports',
        python_callable=archive_reports,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    start >> read_csv_task >> account_check_task
    account_check_task >> [route_to_fatca_task, route_to_irs_task, no_accounts_to_process_task]
    [route_to_fatca_task, route_to_irs_task] >> archive_reports_task
    archive_reports_task >> end