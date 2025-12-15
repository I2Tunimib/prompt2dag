from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.text import MIMEText

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def ingest_vendor_a():
    # Simulate ingestion of Vendor A's CSV data
    data = pd.DataFrame({
        'sku': ['A123', 'A456', 'A789'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [100, 200, 300]
    })
    return data.to_dict(orient='records')

def ingest_vendor_b():
    # Simulate ingestion of Vendor B's CSV data
    data = pd.DataFrame({
        'sku': ['B123', 'B456', 'B789'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [150, 250, 350]
    })
    return data.to_dict(orient='records')

def ingest_vendor_c():
    # Simulate ingestion of Vendor C's CSV data
    data = pd.DataFrame({
        'sku': ['C123', 'C456', 'C789'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [200, 300, 400]
    })
    return data.to_dict(orient='records')

def cleanse_data(**context):
    # Retrieve data from XCom
    vendor_a_data = context['ti'].xcom_pull(task_ids='ingest_vendor_a')
    vendor_b_data = context['ti'].xcom_pull(task_ids='ingest_vendor_b')
    vendor_c_data = context['ti'].xcom_pull(task_ids='ingest_vendor_c')
    
    # Combine data from all vendors
    combined_data = vendor_a_data + vendor_b_data + vendor_c_data
    
    # Convert to DataFrame for processing
    df = pd.DataFrame(combined_data)
    
    # Normalize SKU formats
    df['sku'] = df['sku'].str.upper()
    
    # Validate shipment dates and filter invalid records
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    
    # Enrich data with location information from reference tables
    # (Assuming a reference table is available)
    reference_data = pd.DataFrame({
        'sku': ['A123', 'B456', 'C789'],
        'location': ['Warehouse A', 'Warehouse B', 'Warehouse C']
    })
    df = df.merge(reference_data, on='sku', how='left')
    
    # Return cleansed data
    return df.to_dict(orient='records')

def load_to_db(**context):
    # Retrieve cleansed data from XCom
    cleansed_data = context['ti'].xcom_pull(task_ids='cleanse_data')
    
    # Simulate loading data to PostgreSQL database
    # (Assuming a connection to the database is available)
    # Example: df.to_sql('inventory_shipments', con=engine, if_exists='append', index=False)
    print(f"Loading {len(cleansed_data)} records to database")

def send_summary_email(**context):
    # Retrieve cleansed data from XCom
    cleansed_data = context['ti'].xcom_pull(task_ids='cleanse_data')
    
    # Generate summary email content
    record_count = len(cleansed_data)
    processing_date = datetime.now().strftime('%Y-%m-%d')
    email_content = f"Processing Date: {processing_date}\nRecord Count: {record_count}\nData Quality Metrics: N/A"
    
    # Send email
    msg = MIMEText(email_content)
    msg['Subject'] = 'Supply Chain Shipment Data Processing Summary'
    msg['From'] = 'airflow@company.com'
    msg['To'] = 'supply-chain-team@company.com'
    
    with smtplib.SMTP('smtp.company.com', 587) as server:
        server.starttls()
        server.login('airflow@company.com', 'password')
        server.sendmail('airflow@company.com', 'supply-chain-team@company.com', msg.as_string())

with DAG(
    'supply_chain_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
) as dag:

    with TaskGroup('extract') as extract:
        ingest_vendor_a_task = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a,
        )
        
        ingest_vendor_b_task = PythonOperator(
            task_id='ingest_vendor_b',
            python_callable=ingest_vendor_b,
        )
        
        ingest_vendor_c_task = PythonOperator(
            task_id='ingest_vendor_c',
            python_callable=ingest_vendor_c,
        )
    
    with TaskGroup('transform') as transform:
        cleanse_data_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
            provide_context=True,
        )
    
    with TaskGroup('load') as load:
        load_to_db_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
            provide_context=True,
        )
        
        send_summary_email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
            provide_context=True,
        )
    
    # Define task dependencies
    extract >> cleanse_data_task >> load_to_db_task >> send_summary_email_task