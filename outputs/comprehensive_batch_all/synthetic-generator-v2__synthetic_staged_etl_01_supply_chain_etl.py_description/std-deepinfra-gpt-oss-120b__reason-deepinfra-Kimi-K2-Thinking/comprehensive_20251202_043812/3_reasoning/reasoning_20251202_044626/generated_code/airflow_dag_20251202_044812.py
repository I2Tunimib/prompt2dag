from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'supply-chain-team',
    'depends_on_past': False,
    'email': ['supply-chain-team@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ingest_vendor_a(**context):
    """Simulate ingestion of raw shipment CSV data from Vendor A."""
    vendor_a_data = [
        {
            "shipment_id": f"A_{i}",
            "sku": f"SKU-A-{i:04d}",
            "shipment_date": "2024-01-01",
            "quantity": 10,
            "vendor": "A"
        }
        for i in range(1250)
    ]
    context['task_instance'].xcom_push(key='vendor_a_data', value=vendor_a_data)
    return f"Ingested {len(vendor_a_data)} records from Vendor A"


def ingest_vendor_b(**context):
    """Simulate ingestion of raw shipment CSV data from Vendor B."""
    vendor_b_data = [
        {
            "shipment_id": f"B_{i}",
            "sku": f"SKU-B-{i:04d}",
            "shipment_date": "2024-01-01",
            "quantity": 20,
            "vendor": "B"
        }
        for i in range(980)
    ]
    context['task_instance'].xcom_push(key='vendor_b_data', value=vendor_b_data)
    return f"Ingested {len(vendor_b_data)} records from Vendor B"


def ingest_vendor_c(**context):
    """Simulate ingestion of raw shipment CSV data from Vendor C."""
    vendor_c_data = [
        {
            "shipment_id": f"C_{i}",
            "sku": f"SKU-C-{i:04d}",
            "shipment_date": "2024-01-01",
            "quantity": 30,
            "vendor": "C"
        }
        for i in range(1750)
    ]
    context['task_instance'].xcom_push(key='vendor_c_data', value=vendor_c_data)
    return f"Ingested {len(vendor_c_data)} records from Vendor C"


def cleanse_data(**context):
    """
    Process all vendor datasets together.
    Normalizes SKU formats, validates shipment dates, filters invalid records,
    and enriches data with location information.
    """
    ti = context['task_instance']
    
    vendor_a_data = ti.xcom_pull(
        task_ids='extract_stage.ingest_vendor_a',
        key='vendor_a_data'
    ) or []
    vendor_b_data = ti.xcom_pull(
        task_ids='extract_stage.ingest_vendor_b',
        key='vendor_b_data'
    ) or []
    vendor_c_data = ti.xcom_pull(
        task_ids='extract_stage.ingest_vendor_c',
        key='vendor_c_data'
    ) or []
    
    all_data = vendor_a_data + vendor_b_data + vendor_c_data
    
    cleansed_data = []
    for record in all_data:
        if not record.get('shipment_date'):
            continue
        
        normalized_record = record.copy()
        normalized_record['sku_normalized'] = record['sku'].replace('-', '_')
        normalized_record['location'] = 'WAREHOUSE_01'
        cleansed_data.append(normalized_record)
    
    ti.xcom_push(key='cleansed_data', value=cleansed_data)
    return f"Cleansed {len(cleansed_data)} records"


def load_to_db(**context):
    """Load cleansed data to PostgreSQL inventory database."""
    ti = context['task_instance']
    cleansed_data = ti.xcom_pull(
        task_ids='transform_stage.cleanse_data',
        key='cleansed_data'
    ) or []
    
    record_count = len(cleansed_data)
    print(f"Upserting {record_count} records to inventory_shipments table")
    
    ti.xcom_push(key='load_result', value={
        'records_loaded': record_count,
        'table': 'inventory_shipments',
        'status': 'success'
    })
    return f"Loaded {record_count} records to database"


def send_summary_email(**context):
    """Send email notification with processing summary."""
    ti = context['task_instance']
    load_result = ti.xcom_pull(
        task_ids='load_stage.load_to_db',
        key='load_result'
    )
    
    processing_date = context['ds']
    records_loaded = load_result.get('records_loaded', 0) if load_result else 0
    
    email_subject = f"Supply Chain Shipment Processing Summary - {processing_date}"
    email_body = f"""
    Supply Chain Shipment Processing Summary
    
    Processing Date: {processing_date}
    Records Processed: {records_loaded}
    Data Quality Metrics: All records validated and cleansed
    Status: Completed Successfully
    """
    
    print(f"To: supply-chain-team@company.com")
    print(f"Subject: {email_subject}")
    print(f"Body: {email_body}")
    
    return "Email notification sent"


with DAG(
    'supply_chain_shipment_etl',
    default_args=default_args,
    description='Three-stage ETL pipeline for supply chain shipment data processing',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['supply_chain', 'etl'],
) as dag:
    
    with TaskGroup('extract_stage', tooltip='Parallel vendor data ingestion') as extract_stage:
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
    
    with TaskGroup('transform_stage', tooltip='Data cleansing and normalization') as transform_stage:
        cleanse_data_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
        )
    
    with TaskGroup('load_stage', tooltip='Database loading and notification') as load_stage:
        load_to_db_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
        )
        
        send_summary_email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
        )
        
        load_to_db_task >> send_summary_email_task
    
    extract_stage >> transform_stage >> load_stage