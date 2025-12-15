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
    """Ingest raw shipment CSV data from Vendor A."""
    df = pd.read_csv('path/to/vendor_a.csv')
    return df.to_dict(orient='records')

def ingest_vendor_b():
    """Ingest raw shipment CSV data from Vendor B."""
    df = pd.read_csv('path/to/vendor_b.csv')
    return df.to_dict(orient='records')

def ingest_vendor_c():
    """Ingest raw shipment CSV data from Vendor C."""
    df = pd.read_csv('path/to/vendor_c.csv')
    return df.to_dict(orient='records')

def cleanse_data(**context):
    """Process and cleanse data from all vendors."""
    vendor_a_data = context['ti'].xcom_pull(task_ids='ingest_vendor_a')
    vendor_b_data = context['ti'].xcom_pull(task_ids='ingest_vendor_b')
    vendor_c_data = context['ti'].xcom_pull(task_ids='ingest_vendor_c')
    
    df_a = pd.DataFrame(vendor_a_data)
    df_b = pd.DataFrame(vendor_b_data)
    df_c = pd.DataFrame(vendor_c_data)
    
    df = pd.concat([df_a, df_b, df_c])
    
    # Normalize SKU formats
    df['sku'] = df['sku'].str.upper()
    
    # Validate shipment dates and filter invalid records
    df['shipment_date'] = pd.to_datetime(df['shipment_date'], errors='coerce')
    df = df.dropna(subset=['shipment_date'])
    
    # Enrich data with location information
    location_ref = pd.read_csv('path/to/location_reference.csv')
    df = pd.merge(df, location_ref, on='location_id', how='left')
    
    return df.to_dict(orient='records')

def load_to_db(**context):
    """Load cleansed data to PostgreSQL inventory database."""
    data = context['ti'].xcom_pull(task_ids='cleanse_data')
    df = pd.DataFrame(data)
    
    # Assuming a connection to PostgreSQL is established
    engine = create_engine('postgresql://user:password@host:port/dbname')
    df.to_sql('inventory_shipments', engine, if_exists='append', index=False)

def send_summary_email(**context):
    """Send summary email notification to the supply chain team."""
    data = context['ti'].xcom_pull(task_ids='cleanse_data')
    df = pd.DataFrame(data)
    
    processing_date = datetime.now().strftime('%Y-%m-%d')
    record_count = len(df)
    data_quality_metrics = df.describe().to_string()
    
    message = MIMEText(f"""
    Processing Date: {processing_date}
    Record Count: {record_count}
    Data Quality Metrics:
    {data_quality_metrics}
    """)
    message['Subject'] = 'Supply Chain Shipment Data Processing Summary'
    message['From'] = 'airflow@company.com'
    message['To'] = 'supply-chain-team@company.com'
    
    with smtplib.SMTP('smtp.company.com', 587) as server:
        server.starttls()
        server.login('airflow@company.com', 'password')
        server.sendmail('airflow@company.com', 'supply-chain-team@company.com', message.as_string())

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
    
    extract >> cleanse_data_task >> load_to_db_task >> send_summary_email_task