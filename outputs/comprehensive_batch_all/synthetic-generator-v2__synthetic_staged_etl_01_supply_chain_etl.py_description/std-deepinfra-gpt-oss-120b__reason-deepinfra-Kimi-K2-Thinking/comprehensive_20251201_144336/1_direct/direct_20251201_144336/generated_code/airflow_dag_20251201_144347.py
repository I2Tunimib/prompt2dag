import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.email.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup


def ingest_vendor_a(**kwargs):
    """Ingest raw shipment CSV data from Vendor A."""
    logging.info("Ingesting data for Vendor A")
    # Placeholder for actual ingestion logic
    return {"vendor": "A", "records": 1250}


def ingest_vendor_b(**kwargs):
    """Ingest raw shipment CSV data from Vendor B."""
    logging.info("Ingesting data for Vendor B")
    # Placeholder for actual ingestion logic
    return {"vendor": "B", "records": 980}


def ingest_vendor_c(**kwargs):
    """Ingest raw shipment CSV data from Vendor C."""
    logging.info("Ingesting data for Vendor C")
    # Placeholder for actual ingestion logic
    return {"vendor": "C", "records": 1750}


def cleanse_data(**kwargs):
    """Cleanses and normalizes data from all vendors."""
    ti = kwargs["ti"]
    data_a = ti.xcom_pull(task_ids="extract.ingest_vendor_a")
    data_b = ti.xcom_pull(task_ids="extract.ingest_vendor_b")
    data_c = ti.xcom_pull(task_ids="extract.ingest_vendor_c")

    logging.info("Received data from vendors: %s, %s, %s", data_a, data_b, data_c)

    # Placeholder for actual cleansing logic
    cleansed = {
        "total_records": 3850,
        "details": [data_a, data_b, data_c],
    }
    logging.info("Cleansed data: %s", cleansed)
    return cleansed


def load_to_db(**kwargs):
    """Loads cleansed data into the PostgreSQL inventory database."""
    ti = kwargs["ti"]
    cleansed = ti.xcom_pull(task_ids="transform.cleanse_data")
    logging.info("Loading data into DB: %s", cleansed)

    hook = PostgresHook(postgres_conn_id="inventory_postgres")
    # Example upsert statement; replace with real logic as needed
    sql = """
    INSERT INTO inventory_shipments (data, processed_at)
    VALUES (%s, NOW())
    ON CONFLICT (data) DO UPDATE SET processed_at = EXCLUDED.processed_at;
    """
    # Using a dummy parameter for illustration
    hook.run(sql, parameters=(str(cleansed),))
    logging.info("Data load completed")
    return "load_success"


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="supply_chain_shipment_etl",
    description="Daily ETL pipeline for shipment data from multiple vendors",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
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
            subject="Shipment ETL Summary - {{ ds }}",
            html_content="""
                <h3>Shipment ETL Run Summary</h3>
                <p>Date: {{ ds }}</p>
                <p>Total records processed: 3,850</p>
                <p>Data quality metrics: All records passed validation.</p>
            """,
        )
        load >> send_email

    # Define dependencies between groups
    extract_group >> transform_group >> load_group

    # Parallel extraction tasks are already independent within the extract group
    # Fan‑in is handled by the downstream cleanse task which depends on all three
    ingest_a >> cleanse
    ingest_b >> cleanse
    ingest_c >> cleanse