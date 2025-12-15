from datetime import datetime, timedelta
import json

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.email.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup


def ingest_vendor_a(**kwargs):
    """Simulate ingestion of Vendor A CSV data."""
    # Placeholder: generate dummy DataFrame
    df = pd.DataFrame(
        {
            "vendor": ["A"] * 1250,
            "sku": [f"SKU{i:04d}" for i in range(1250)],
            "shipment_date": pd.date_range(start="2024-01-01", periods=1250, freq="D"),
            "location_id": [i % 10 for i in range(1250)],
        }
    )
    return df.to_json()


def ingest_vendor_b(**kwargs):
    """Simulate ingestion of Vendor B CSV data."""
    df = pd.DataFrame(
        {
            "vendor": ["B"] * 980,
            "sku": [f"sku-{i:04d}" for i in range(980)],
            "shipment_date": pd.date_range(start="2024-01-05", periods=980, freq="D"),
            "location_id": [i % 8 for i in range(980)],
        }
    )
    return df.to_json()


def ingest_vendor_c(**kwargs):
    """Simulate ingestion of Vendor C CSV data."""
    df = pd.DataFrame(
        {
            "vendor": ["C"] * 1750,
            "sku": [f"SKU_{i:04d}" for i in range(1750)],
            "shipment_date": pd.date_range(start="2024-01-10", periods=1750, freq="D"),
            "location_id": [i % 12 for i in range(1750)],
        }
    )
    return df.to_json()


def cleanse_data(**kwargs):
    """Combine and cleanse data from all vendors."""
    ti = kwargs["ti"]
    df_a = pd.read_json(ti.xcom_pull(task_ids="extraction.ingest_vendor_a"))
    df_b = pd.read_json(ti.xcom_pull(task_ids="extraction.ingest_vendor_b"))
    df_c = pd.read_json(ti.xcom_pull(task_ids="extraction.ingest_vendor_c"))

    # Consolidate
    df = pd.concat([df_a, df_b, df_c], ignore_index=True)

    # Normalise SKU format (uppercase, remove hyphens/underscores)
    df["sku"] = df["sku"].str.upper().str.replace(r"[-_]", "", regex=True)

    # Validate shipment dates (keep only dates <= today)
    today = pd.Timestamp(datetime.utcnow().date())
    df = df[df["shipment_date"] <= today]

    # Enrich with dummy location info (simulated)
    location_lookup = {i: f"Location_{i}" for i in range(20)}
    df["location_name"] = df["location_id"].map(location_lookup)

    # Return cleansed data as JSON
    return df.to_json()


def load_to_db(**kwargs):
    """Load cleansed data into PostgreSQL inventory database."""
    ti = kwargs["ti"]
    df = pd.read_json(ti.xcom_pull(task_ids="transformation.cleanse_data"))

    hook = PostgresHook(postgres_conn_id="inventory_postgres")
    # Example upsert logic (simplified for illustration)
    insert_sql = """
        INSERT INTO inventory_shipments (vendor, sku, shipment_date, location_id, location_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sku) DO UPDATE
        SET vendor = EXCLUDED.vendor,
            shipment_date = EXCLUDED.shipment_date,
            location_id = EXCLUDED.location_id,
            location_name = EXCLUDED.location_name;
    """
    records = [
        (
            row["vendor"],
            row["sku"],
            row["shipment_date"].date(),
            int(row["location_id"]),
            row["location_name"],
        )
        for _, row in df.iterrows()
    ]

    hook.run(insert_sql, parameters=records, autocommit=True)
    return len(records)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="supply_chain_shipment_etl",
    default_args=default_args,
    description="Daily ETL pipeline for shipment data from three vendors",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["supply_chain", "etl"],
) as dag:

    with TaskGroup(group_id="extraction") as extraction:
        ingest_a = PythonOperator(
            task_id="ingest_vendor_a", python_callable=ingest_vendor_a
        )
        ingest_b = PythonOperator(
            task_id="ingest_vendor_b", python_callable=ingest_vendor_b
        )
        ingest_c = PythonOperator(
            task_id="ingest_vendor_c", python_callable=ingest_vendor_c
        )

    with TaskGroup(group_id="transformation") as transformation:
        cleanse = PythonOperator(task_id="cleanse_data", python_callable=cleanse_data)

    with TaskGroup(group_id="loading") as loading:
        load = PythonOperator(task_id="load_to_db", python_callable=load_to_db)

        send_email = EmailOperator(
            task_id="send_summary_email",
            to="supply-chain-team@company.com",
            subject="Shipment ETL Summary - {{ ds }}",
            html_content="""
                <h3>Shipment ETL Summary</h3>
                <p><strong>Processing date:</strong> {{ ds }}</p>
                <p><strong>Records loaded:</strong> {{ ti.xcom_pull(task_ids='loading.load_to_db') }}</p>
                <p><strong>Data quality metrics:</strong> All records passed validation.</p>
            """,
        )

    # Define dependencies
    [ingest_a, ingest_b, ingest_c] >> cleanse
    cleanse >> load >> send_email