from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def ingest_vendor_a():
    """Ingest raw shipment CSV data from Vendor A."""
    df = pd.read_csv('/path/to/vendor_a.csv')
    return df.to_dict(orient='records')

def ingest_vendor_b():
    """Ingest raw shipment CSV data from Vendor B."""
    df = pd.read_csv('/path/to/vendor_b.csv')
    return df.to_dict(orient='records')

def ingest_vendor_c():
    """Ingest raw shipment CSV data from Vendor C."""
    df = pd.read_csv('/path/to/vendor_c.csv')
    return df.to_dict(orient='records')

def cleanse_data(**context):
    """Process and cleanse data from all vendors."""
    vendor_a_data = context['ti'].xcom_pull(task_ids='ingest_vendor_a')
    vendor_b_data = context['ti'].xcom_pull(task_ids='ingest_vendor_b')
    vendor_c_data = context['ti'].xcom_pull(task_ids='ingest_vendor_c')
    
    df_a = pd.DataFrame(vendor_a_data)
    df_b = pd.DataFrame(vendor_b_data)
    df_c = pd.DataFrame(vendor_c_data)
    
    combined_df = pd.concat([df_a, df_b, df_c])
    
    # Normalize SKU formats
    combined_df['sku'] = combined_df['sku'].str.upper()
    
    # Validate shipment dates and filter invalid records
    combined_df['shipment_date'] = pd.to_datetime(combined_df['shipment_date'], errors='coerce')
    combined_df = combined_df.dropna(subset=['shipment_date'])
    
    # Enrich data with location information
    location_df = pd.read_csv('/path/to/location_reference.csv')
    combined_df = pd.merge(combined_df, location_df, on='location_id', how='left')
    
    return combined_df.to_dict(orient='records')

def load_to_db(**context):
    """Load cleansed data to PostgreSQL inventory database."""
    cleansed_data = context['ti'].xcom_pull(task_ids='cleanse_data')
    df = pd.DataFrame(cleansed_data)
    
    # Assuming a PostgreSQL connection is configured in Airflow
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://user:password@host:port/dbname')
    
    df.to_sql('inventory_shipments', engine, if_exists='append', index=False)

def send_summary_email(**context):
    """Send summary email notification to the supply chain team."""
    from airflow.operators.email import EmailOperator
    
    ti = context['ti']
    processing_date = ti.execution_date
    record_count = len(ti.xcom_pull(task_ids='cleanse_data'))
    
    email_body = f"""
    Processing Date: {processing_date}
    Total Records Processed: {record_count}
    Data Quality Metrics: [Add metrics here]
    """
    
    EmailOperator(
        task_id='send_summary_email',
        to='supply-chain-team@company.com',
        subject='Supply Chain Shipment Data Processing Summary',
        html_content=email_body
    ).execute(context)

with DAG(
    'supply_chain_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
) as dag:

    with TaskGroup('extract') as extract:
        ingest_vendor_a_task = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a
        )
        
        ingest_vendor_b_task = PythonOperator(
            task_id='ingest_vendor_b',
            python_callable=ingest_vendor_b
        )
        
        ingest_vendor_c_task = PythonOperator(
            task_id='ingest_vendor_c',
            python_callable=ingest_vendor_c
        )
    
    with TaskGroup('transform') as transform:
        cleanse_data_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
            provide_context=True
        )
    
    with TaskGroup('load') as load:
        load_to_db_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
            provide_context=True
        )
        
        send_summary_email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
            provide_context=True
        )
    
    extract >> transform >> load
    cleanse_data_task >> load_to_db_task >> send_summary_email_task