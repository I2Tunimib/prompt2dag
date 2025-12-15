from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


default_args = {
    'owner': 'supply-chain-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'supply_chain_shipment_etl',
    default_args=default_args,
    description='Three-stage ETL pipeline for supply chain shipment data processing',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['supply-chain', 'etl'],
) as dag:

    with TaskGroup('extract_stage') as extract_stage:
        
        def ingest_vendor_a(**context):
            data = [
                {'shipment_id': f'A_{i}', 'sku': f'SKU-A-{i:04d}', 
                 'shipment_date': '2024-01-01', 'quantity': 10}
                for i in range(1250)
            ]
            ti = context['task_instance']
            ti.xcom_push(key='vendor_a_data', value=data)
            ti.xcom_push(key='vendor_a_record_count', value=len(data))
            return f"Ingested {len(data)} records from Vendor A"

        ingest_vendor_a_task = PythonOperator(
            task_id='ingest_vendor_a',
            python_callable=ingest_vendor_a,
        )

        def ingest_vendor_b(**context):
            data = [
                {'shipment_id': f'B_{i}', 'sku': f'SKU-B-{i:04d}', 
                 'shipment_date': '2024-01-01', 'quantity': 20}
                for i in range(980)
            ]
            ti = context['task_instance']
            ti.xcom_push(key='vendor_b_data', value=data)
            ti.xcom_push(key='vendor_b_record_count', value=len(data))
            return f"Ingested {len(data)} records from Vendor B"

        ingest_vendor_b_task = PythonOperator(
            task_id='ingest_vendor_b',
            python_callable=ingest_vendor_b,
        )

        def ingest_vendor_c(**context):
            data = [
                {'shipment_id': f'C_{i}', 'sku': f'SKU-C-{i:04d}', 
                 'shipment_date': '2024-01-01', 'quantity': 30}
                for i in range(1750)
            ]
            ti = context['task_instance']
            ti.xcom_push(key='vendor_c_data', value=data)
            ti.xcom_push(key='vendor_c_record_count', value=len(data))
            return f"Ingested {len(data)} records from Vendor C"

        ingest_vendor_c_task = PythonOperator(
            task_id='ingest_vendor_c',
            python_callable=ingest_vendor_c,
        )

    with TaskGroup('transform_stage') as transform_stage:
        
        def cleanse_data(**context):
            ti = context['task_instance']
            vendor_a_data = ti.xcom_pull(task_ids='extract_stage.ingest_vendor_a', key='vendor_a_data')
            vendor_b_data = ti.xcom_pull(task_ids='extract_stage.ingest_vendor_b', key='vendor_b_data')
            vendor_c_data = ti.xcom_pull(task_ids='extract_stage.ingest_vendor_c', key='vendor_c_data')
            
            all_data = (vendor_a_data or []) + (vendor_b_data or []) + (vendor_c_data or [])
            cleansed_data = []
            invalid_records = 0
            
            for record in all_data:
                record['sku_normalized'] = record['sku'].upper().replace('-', '')
                try:
                    datetime.strptime(record['shipment_date'], '%Y-%m-%d')
                    record['warehouse_location'] = 'WH-US-01'
                    cleansed_data.append(record)
                except (ValueError, TypeError):
                    invalid_records += 1
            
            ti.xcom_push(key='cleansed_data', value=cleansed_data)
            ti.xcom_push(key='total_records_before_cleansing', value=len(all_data))
            ti.xcom_push(key='total_records_after_cleansing', value=len(cleansed_data))
            ti.xcom_push(key='invalid_records', value=invalid_records)
            return f"Cleansed {len(cleansed_data)} records, filtered {invalid_records} invalid records"

        cleanse_data_task = PythonOperator(
            task_id='cleanse_data',
            python_callable=cleanse_data,
        )

    with TaskGroup('load_stage') as load_stage:
        
        def load_to_db(**context):
            ti = context['task_instance']
            cleansed_data = ti.xcom_pull(task_ids='transform_stage.cleanse_data', key='cleansed_data')
            
            if not cleansed_data:
                raise ValueError("No cleansed data available for loading")
            
            records_loaded = len(cleansed_data)
            ti.xcom_push(key='records_loaded', value=records_loaded)
            ti.xcom_push(key='load_status', value='success')
            return f"Successfully loaded {records_loaded} records to inventory_shipments table"

        load_to_db_task = PythonOperator(
            task_id='load_to_db',
            python_callable=load_to_db,
        )

        def send_summary_email(**context):
            ti = context['task_instance']
            processing_date = context['ds']
            
            vendor_a_count = ti.xcom_pull(task_ids='extract_stage.ingest_vendor_a', key='vendor_a_record_count')
            vendor_b_count = ti.xcom_pull(task_ids='extract_stage.ingest_vendor_b', key='vendor_b_record_count')
            vendor_c_count = ti.xcom_pull(task_ids='extract_stage.ingest_vendor_c', key='vendor_c_record_count')
            
            total_before = ti.xcom_pull(task_ids='transform_stage.cleanse_data', key='total_records_before_cleansing')
            total_after = ti.xcom_pull(task_ids='transform_stage.cleanse_data', key='total_records_after_cleansing')
            invalid_records = ti.xcom_pull(task_ids='transform_stage.cleanse_data', key='invalid_records')
            
            records_loaded = ti.xcom_pull(task_ids='load_stage.load_to_db', key='records_loaded')
            
            email_subject = f"Supply Chain ETL Summary - {processing_date}"
            email_body = f"""
            Supply Chain Shipment ETL Processing Summary

            Processing Date: {processing_date}

            Stage 1 - Extraction:
            - Vendor A: {vendor_a_count} records
            - Vendor B: {vendor_b_count} records
            - Vendor C: {vendor_c_count} records
            - Total Extracted: {vendor_a_count + vendor_b_count + vendor_c_count} records

            Stage 2 - Transformation:
            - Records Before Cleansing: {total_before}
            - Records After Cleansing: {total_after}
            - Invalid Records Filtered: {invalid_records}

            Stage 3 - Loading:
            - Records Loaded to Database: {records_loaded}

            Data Quality Metrics:
            - Cleansing Success Rate: {((total_after / total_before) * 100):.2f}%
            """
            
            print(f"Sending email to: supply-chain-team@company.com")
            print(f"Subject: {email_subject}")
            print(f"Body: {email_body}")
            
            return "Email notification sent successfully"

        send_summary_email_task = PythonOperator(
            task_id='send_summary_email',
            python_callable=send_summary_email,
        )

        load_to_db_task >> send_summary_email_task

    extract_stage >> transform_stage >> load_stage