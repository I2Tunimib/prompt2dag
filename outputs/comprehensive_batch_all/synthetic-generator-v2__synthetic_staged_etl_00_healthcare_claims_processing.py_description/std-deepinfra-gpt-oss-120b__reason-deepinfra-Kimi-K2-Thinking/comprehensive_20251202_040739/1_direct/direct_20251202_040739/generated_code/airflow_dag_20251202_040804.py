import hashlib
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_claims(**context):
    """
    Extract patient claims data from a CSV file.
    Returns a DataFrame that will be pushed to XCom.
    """
    claims_path = Variable.get("claims_csv_path", default_var="/path/to/claims.csv")
    logging.info("Reading claims data from %s", claims_path)
    df = pd.read_csv(claims_path)
    return df.to_dict(orient="records")


def extract_providers(**context):
    """
    Extract provider data from a CSV file.
    Returns a DataFrame that will be pushed to XCom.
    """
    providers_path = Variable.get(
        "providers_csv_path", default_var="/path/to/providers.csv"
    )
    logging.info("Reading providers data from %s", providers_path)
    df = pd.read_csv(providers_path)
    return df.to_dict(orient="records")


def _hash_identifier(identifier: str) -> str:
    """Hash a string identifier using SHA‑256."""
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def transform_join(**context):
    """
    Join claims and provider data, anonymize patient identifiers,
    and calculate a simple risk score.
    The transformed records are returned for downstream loading.
    """
    ti = context["ti"]
    claims = ti.xcom_pull(task_ids="extract.extract_claims")
    providers = ti.xcom_pull(task_ids="extract.extract_providers")

    if not claims or not providers:
        raise ValueError("Missing input data for transformation.")

    claims_df = pd.DataFrame(claims)
    providers_df = pd.DataFrame(providers)

    # Anonymize patient identifiers
    claims_df["patient_hash"] = claims_df["patient_id"].apply(_hash_identifier)
    claims_df = claims_df.drop(columns=["patient_id"])

    # Join on provider identifier
    merged_df = claims_df.merge(
        providers_df, how="left", left_on="provider_id", right_on="provider_id"
    )

    # Simple risk scoring: amount * 0.01 (placeholder logic)
    merged_df["risk_score"] = merged_df["claim_amount"] * 0.01

    # Keep only needed columns for the warehouse
    result_df = merged_df[
        [
            "claim_id",
            "patient_hash",
            "provider_id",
            "procedure_code",
            "claim_amount",
            "risk_score",
            "provider_name",
            "provider_specialty",
        ]
    ]

    logging.info("Transformation complete, %d records produced.", len(result_df))
    return result_df.to_dict(orient="records")


def load_warehouse(**context):
    """
    Load transformed data into the healthcare analytics warehouse.
    Inserts records into the claims_fact table.
    """
    ti = context["ti"]
    transformed = ti.xcom_pull(task_ids="transform_join")
    if not transformed:
        raise ValueError("No transformed data available for loading.")

    df = pd.DataFrame(transformed)
    hook = PostgresHook(postgres_conn_id="analytics_warehouse")
    # Example insert using COPY could be added; here we use simple INSERT for illustration.
    insert_sql = """
        INSERT INTO claims_fact (
            claim_id,
            patient_hash,
            provider_id,
            procedure_code,
            claim_amount,
            risk_score,
            provider_name,
            provider_specialty
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = df.values.tolist()
    logging.info("Loading %d rows into claims_fact table.", len(rows))
    hook.run(insert_sql, parameters=rows, autocommit=True)


def refresh_bi(**context):
    """
    Trigger refresh of BI dashboards (Power BI, Tableau).
    This implementation logs the action; replace with real API calls as needed.
    """
    logging.info("Refreshing Power BI dashboards...")
    logging.info("Refreshing Tableau dashboards...")
    # Placeholder for actual refresh logic (e.g., REST API calls)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="healthcare_claims_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["healthcare", "etl"],
) as dag:

    with TaskGroup(group_id="extract") as extract_group:
        extract_claims_task = PythonOperator(
            task_id="extract_claims",
            python_callable=extract_claims,
            provide_context=True,
        )
        extract_providers_task = PythonOperator(
            task_id="extract_providers",
            python_callable=extract_providers,
            provide_context=True,
        )
        extract_claims_task >> extract_providers_task

    transform_task = PythonOperator(
        task_id="transform_join",
        python_callable=transform_join,
        provide_context=True,
    )

    with TaskGroup(group_id="load") as load_group:
        load_warehouse_task = PythonOperator(
            task_id="load_warehouse",
            python_callable=load_warehouse,
            provide_context=True,
        )
        refresh_bi_task = PythonOperator(
            task_id="refresh_bi",
            python_callable=refresh_bi,
            provide_context=True,
        )
        load_warehouse_task >> refresh_bi_task

    extract_group >> transform_task >> load_group