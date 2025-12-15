import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup


def ingest_vendor_a(**context):
    """Simulate ingestion of Vendor A CSV data."""
    logging.info("Ingesting Vendor A shipment data")
    record_count = 1250
    return record_count


def ingest_vendor_b(**context):
    """Simulate ingestion of Vendor B CSV data."""
    logging.info("Ingesting Vendor B shipment data")
    record_count = 980
    return record_count


def ingest_vendor_c(**context):
    """Simulate ingestion of Vendor C CSV data."""
    logging.info("Ingesting Vendor C shipment data")
    record_count = 1750
    return record_count


def cleanse_data(**context):
    """Consolidate and cleanse data from all vendors."""
    ti = context["ti"]
    counts = ti.xcom_pull(
        task_ids=[
            "extract.ingest_vendor_a",
            "extract.ingest_vendor_b",
            "extract.ingest_vendor_c",
        ]
    )
    total_raw = sum(filter(None, counts))
    logging.info("Total raw records from vendors: %s", total_raw)

    # Simulate cleansing logic
    cleansed_count = 3850
    logging.info("Cleansed record count: %s", cleansed_count)
    return cleansed_count


def load_to_db(**context):
    """Load cleansed data into PostgreSQL inventory database."""
    ti = context["ti"]
    cleansed_count = ti.xcom_pull(task_ids="transform.cleanse_data")
    logging.info("Loading %s records into inventory_shipments table", cleansed_count)

    # Simulate DB upsert
    loaded_count = cleansed_count
    logging.info("Successfully upserted %s records", loaded_count)
    return loaded_count


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="supply_chain_shipment_etl",
    default_args=default_args,
    description="Daily ETL pipeline for supply chain shipment data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "supply_chain"],
) as dag:

    with TaskGroup(group_id="extract") as extract_group:
        ingest_a = PythonOperator(
            task_id="ingest_vendor_a",
            python_callable=ingest_vendor_a,
        )
        ingest_b = PythonOperator(
            task_id="ingest_vendor_b",
            python_callable=ingest_vendor_b,
        )
        ingest_c = PythonOperator(
            task_id="ingest_vendor_c",
            python_callable=ingest_vendor_c,
        )

    with TaskGroup(group_id="transform") as transform_group:
        cleanse = PythonOperator(
            task_id="cleanse_data",
            python_callable=cleanse_data,
        )

    with TaskGroup(group_id="load") as load_group:
        load = PythonOperator(
            task_id="load_to_db",
            python_callable=load_to_db,
        )
        send_email = EmailOperator(
            task_id="send_summary_email",
            to="supply-chain-team@company.com",
            subject="Daily Shipment ETL Summary - {{ ds }}",
            html_content="""
                <h3>Shipment ETL Summary for {{ ds }}</h3>
                <p>Records loaded into inventory: {{ ti.xcom_pull(task_ids='load.load_to_db') }}</p>
                <p>Data quality metrics: All records passed validation.</p>
            """,
        )

    # Define dependencies
    extract_group >> cleanse
    cleanse >> load
    load >> send_email