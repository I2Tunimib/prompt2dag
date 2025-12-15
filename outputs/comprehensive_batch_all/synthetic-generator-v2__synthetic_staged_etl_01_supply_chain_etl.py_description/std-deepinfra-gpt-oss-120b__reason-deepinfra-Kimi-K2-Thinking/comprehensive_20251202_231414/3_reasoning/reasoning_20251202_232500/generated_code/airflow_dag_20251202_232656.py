from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from typing import Any, Dict, List

# Default arguments for the DAG
default_args = {
    'owner': 'supply-chain-team',
    'depends_on_past': False,
    'email_on_failure': False,  # Failure email notifications disabled
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ingest_vendor_a(**kwargs: Any) -> str:
    """Ingests raw shipment CSV data from Vendor A (1,250 records)"""
    ti = kwargs['ti']
    data = [{"vendor": "A", "record_id": i, "data": f"raw_data_a_{i}"} for i in range(1250)]
    ti.xcom_push(key='vendor_a_data', value=data)
    return "Vendor A data ingested successfully"


def ingest_vendor_b(**kwargs: Any) -> str:
    """Ingests raw shipment CSV data from Vendor B (980 records)"""
    ti = kwargs['ti']
    data = [{"vendor": "B", "record_id": i, "data": f"raw_data_b_{i}"} for i in range(980)]
    ti.xcom_push(key='vendor_b_data', value=data)
    return "Vendor B data ingested successfully"


def ingest_vendor_c(**kwargs: Any) -> str:
    """Ingests raw shipment CSV data from Vendor C (1,750 records)"""
    ti = kwargs['ti']
    data = [{"vendor": "C", "record_id": i, "data": f"raw_data_c_{i}"} for i in range(1750)]
    ti.xcom_push(key='vendor_c_data', value=data)
    return "Vendor C data ingested successfully"


def cleanse_data(**kwargs: Any) -> str:
    """Processes all three vendor datasets together"""
    ti = kwargs['ti']
    vendor_a_data = ti.xcom_pull(task_ids='extract.ingest_vendor_a', key='vendor_a_data')
    vendor_b_data = ti.xcom_pull(task_ids='extract.ingest_vendor_b', key='vendor_b_data')
    vendor_c_data = ti.xcom_pull(task_ids='extract.ingest_vendor_c', key='vendor_c_data')

    all_data = vendor_a_data + vendor_b_data + vendor_c_data

    cleansed_data = []
    for record in all_data:
        cleansed_record = {
            "vendor": record["vendor"],
            "record_id": record["record_id"],
            "sku": f"SKU-{record['vendor']}-{record['record_id']}",
            "shipment_date": "2024-01-01",
            "location": f"Location-{record['vendor']}",
            "status": "valid"
        }
        cleansed_data.append(cleansed_record)

    ti.xcom_push(key='cleansed_data', value=cleansed_data)
    return f"Data cleansed successfully. Processed {len(cleansed_data)} records"


def load_to_db(**kwargs: Any) -> str:
    """Loads cleansed data to PostgreSQL inventory database"""
    ti = kwargs['ti']
    cleansed_data = ti.xcom_pull(task_ids='transform.cleanse_data', key='cleansed_data')
    records_loaded = len(cleansed_data) if cleansed_data else 0

    ti.xcom_push(key='load_metrics', value={
        'records_loaded': records_loaded,
        'table': 'inventory_shipments'
    })

    return f"Loaded {records_loaded} records to inventory_shipments table"


def send_summary_email(**kwargs: Any) -> str:
    """Sends email notification to supply chain team"""
    ti = kwargs['ti']
    load_metrics = ti.xcom_pull(task_ids='load.load_to_db', key='load_metrics')
    processing_date = kwargs['ds']

    email_content = f"""
    Supply Chain ETL Pipeline Summary
    
    Processing Date: {processing_date}
    Records Loaded: {load_metrics['records_loaded']}
    Destination Table: {load_metrics['table']}
    Data Quality: All records validated and enriched
    
    Status: SUCCESS
    """

    return f"Email sent to supply-chain-team@company.com with summary for {processing_date}"


# DAG definition
with DAG(
    dag_id='supply_chain_etl_pipeline',
    default_args=default_args,
    description='Three-stage ETL pipeline for supply chain shipment data processing',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['supply_chain', 'etl'],
) as dag:

    # Stage 1: Extract - Parallel vendor data ingestion
    with TaskGroup('extract', tooltip='Stage 1: Extract - Parallel vendor data ingestion') as extract_group:
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

    # Stage 2: Transform - Data cleansing and normalization
    with TaskGroup('transform', tooltip='Stage 2: Transform - Data cleansing and normalization') as transform_group:
        cleanse_data_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
        )

    # Stage 3: Load - Database loading and notification
    with TaskGroup('load', tooltip='Stage 3: Load - Database loading and notification') as load_group:
        load_to_db_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
        )

        send_summary_email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
        )

        # Sequential execution within load group
        load_to_db_task >> send_summary_email_task

    # Define dependencies between stages
    extract_group >> transform_group >> load_group