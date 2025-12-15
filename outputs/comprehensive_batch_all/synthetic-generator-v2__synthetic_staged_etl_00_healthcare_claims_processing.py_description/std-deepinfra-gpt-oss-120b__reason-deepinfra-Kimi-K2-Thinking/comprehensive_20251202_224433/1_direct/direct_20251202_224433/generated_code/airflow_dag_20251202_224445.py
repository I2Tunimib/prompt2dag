import hashlib
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_claims(**kwargs):
    """
    Extract patient claims from a CSV file and push the data to XCom.
    """
    claims_path = "/path/to/claims.csv"  # Update with the actual path
    df = pd.read_csv(claims_path)
    # Convert DataFrame to a list of dicts for XCom serialization
    records = df.to_dict(orient="records")
    kwargs["ti"].xcom_push(key="claims_data", value=records)


def extract_providers(**kwargs):
    """
    Extract provider information from a CSV file and push the data to XCom.
    """
    providers_path = "/path/to/providers.csv"  # Update with the actual path
    df = pd.read_csv(providers_path)
    records = df.to_dict(orient="records")
    kwargs["ti"].xcom_push(key="providers_data", value=records)


def transform_join(**kwargs):
    """
    Join claims and provider data, anonymize patient identifiers,
    and calculate a simple risk score.
    The transformed data is pushed to XCom for downstream loading.
    """
    ti = kwargs["ti"]
    claims = ti.xcom_pull(key="claims_data", task_ids="extract_stage.extract_claims")
    providers = ti.xcom_pull(key="providers_data", task_ids="extract_stage.extract_providers")

    if not claims or not providers:
        raise ValueError("Missing input data for transformation.")

    claims_df = pd.DataFrame(claims)
    providers_df = pd.DataFrame(providers)

    # Join on provider identifier (assumed column name 'provider_id')
    merged_df = claims_df.merge(providers_df, on="provider_id", how="left", suffixes=("_claim", "_prov"))

    # Anonymize patient identifier
    def hash_patient_id(pid):
        return hashlib.sha256(str(pid).encode()).hexdigest()

    merged_df["patient_id_hashed"] = merged_df["patient_id"].apply(hash_patient_id)
    merged_df.drop(columns=["patient_id"], inplace=True)

    # Simple risk scoring: amount * factor based on procedure code prefix
    def risk_score(row):
        code = str(row["procedure_code"])
        factor = 1.5 if code.startswith("A") else 1.0
        return row["claim_amount"] * factor

    merged_df["risk_score"] = merged_df.apply(risk_score, axis=1)

    # Push transformed records to XCom
    transformed_records = merged_df.to_dict(orient="records")
    ti.xcom_push(key="transformed_data", value=transformed_records)


def load_warehouse(**kwargs):
    """
    Load transformed data into the healthcare analytics warehouse.
    Assumes a Postgres connection defined in Airflow with the ID 'healthcare_warehouse'.
    """
    ti = kwargs["ti"]
    transformed = ti.xcom_pull(key="transformed_data", task_ids="transform_join")
    if not transformed:
        raise ValueError("No transformed data available for loading.")

    df = pd.DataFrame(transformed)

    pg_hook = PostgresHook(postgres_conn_id="healthcare_warehouse")
    engine = pg_hook.get_sqlalchemy_engine()

    # Load into fact table; adjust table name and schema as needed
    df.to_sql(
        name="claims_fact",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )


def refresh_bi(**kwargs):
    """
    Trigger a refresh of BI dashboards.
    This placeholder simply logs the action; replace with real API calls as needed.
    """
    # Example: call Power BI and Tableau refresh endpoints here
    print("BI dashboards refresh triggered.")


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

    with TaskGroup(group_id="extract_stage") as extract_stage:
        extract_claims_task = PythonOperator(
            task_id="extract_claims",
            python_callable=extract_claims,
        )
        extract_providers_task = PythonOperator(
            task_id="extract_providers",
            python_callable=extract_providers,
        )
        extract_claims_task >> extract_providers_task  # ordering not required; kept for clarity

    transform_task = PythonOperator(
        task_id="transform_join",
        python_callable=transform_join,
    )

    with TaskGroup(group_id="load_stage") as load_stage:
        load_warehouse_task = PythonOperator(
            task_id="load_warehouse",
            python_callable=load_warehouse,
        )
        refresh_bi_task = PythonOperator(
            task_id="refresh_bi",
            python_callable=refresh_bi,
        )
        load_warehouse_task >> refresh_bi_task

    # Define dependencies
    extract_stage >> transform_task >> load_stage