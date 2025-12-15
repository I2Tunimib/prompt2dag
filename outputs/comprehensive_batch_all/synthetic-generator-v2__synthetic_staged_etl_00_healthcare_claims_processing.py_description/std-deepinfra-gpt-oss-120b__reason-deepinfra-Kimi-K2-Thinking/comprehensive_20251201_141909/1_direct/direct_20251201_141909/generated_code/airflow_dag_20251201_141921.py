from datetime import datetime, timedelta
import hashlib
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_claims(**context):
    """
    Extract patient claims data from a CSV file.
    The path is hard‑coded for illustration; replace with a variable or connection as needed.
    """
    claims_path = "/opt/airflow/data/claims.csv"
    df_claims = pd.read_csv(claims_path)
    # Push dataframe to XCom (serialized)
    context["ti"].xcom_push(key="claims_df", value=df_claims.to_dict("records"))


def extract_providers(**context):
    """
    Extract provider data from a CSV file.
    """
    providers_path = "/opt/airflow/data/providers.csv"
    df_providers = pd.read_csv(providers_path)
    context["ti"].xcom_push(key="providers_df", value=df_providers.to_dict("records"))


def _hash_identifier(identifier: str) -> str:
    """Return a SHA‑256 hash of the given identifier."""
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def _calculate_risk_score(row: dict) -> float:
    """
    Simple risk scoring based on procedure code and claim amount.
    This is a placeholder; replace with real business logic.
    """
    amount = float(row.get("claim_amount", 0))
    proc_code = row.get("procedure_code", "")
    # Example: higher amount and certain procedure codes increase risk
    score = amount / 1000.0
    if proc_code.startswith("R"):
        score += 2.0
    return round(score, 2)


def transform_join(**context):
    """
    Join claims and provider data, anonymize patient identifiers,
    and calculate a risk score for each claim.
    """
    ti = context["ti"]
    claims_records = ti.xcom_pull(key="claims_df", task_ids="extract_stage.extract_claims")
    providers_records = ti.xcom_pull(key="providers_df", task_ids="extract_stage.extract_providers")

    df_claims = pd.DataFrame(claims_records)
    df_providers = pd.DataFrame(providers_records)

    # Join on provider_id (assumed column name)
    df_joined = pd.merge(df_claims, df_providers, on="provider_id", how="left", suffixes=("_claim", "_provider"))

    # Anonymize patient identifier
    if "patient_id" in df_joined.columns:
        df_joined["patient_hash"] = df_joined["patient_id"].apply(_hash_identifier)
        df_joined.drop(columns=["patient_id"], inplace=True)

    # Calculate risk score
    df_joined["risk_score"] = df_joined.apply(_calculate_risk_score, axis=1)

    # Push transformed data to XCom
    ti.xcom_push(key="transformed_df", value=df_joined.to_dict("records"))


def load_warehouse(**context):
    """
    Load transformed data into the analytics warehouse.
    This example uses PostgresHook; adjust schema/table names as needed.
    """
    ti = context["ti"]
    transformed_records = ti.xcom_pull(key="transformed_df", task_ids="transform_join")
    if not transformed_records:
        raise ValueError("No transformed data found in XCom.")

    df = pd.DataFrame(transformed_records)

    # Example: write to a staging table then upsert to final tables
    pg_hook = PostgresHook(postgres_conn_id="analytics_warehouse")
    # Create temporary table
    create_tmp_sql = """
    CREATE TEMP TABLE tmp_claims AS
    SELECT * FROM claims_fact LIMIT 0;
    """
    pg_hook.run(create_tmp_sql)

    # Use COPY to load data efficiently
    # For simplicity, we use pandas to_sql (requires SQLAlchemy)
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql("tmp_claims", con=engine, if_exists="append", index=False)

    # Upsert logic placeholder
    upsert_sql = """
    INSERT INTO claims_fact (SELECT * FROM tmp_claims)
    ON CONFLICT (claim_id) DO UPDATE SET
        provider_id = EXCLUDED.provider_id,
        claim_amount = EXCLUDED.claim_amount,
        risk_score = EXCLUDED.risk_score,
        patient_hash = EXCLUDED.patient_hash;
    """
    pg_hook.run(upsert_sql)


def refresh_bi(**context):
    """
    Trigger refresh of BI dashboards (Power BI, Tableau).
    In a real environment this could call REST APIs or CLI commands.
    """
    # Placeholder implementation
    print("Refreshing Power BI dashboards...")
    print("Refreshing Tableau dashboards...")
    # Example: call an external script or API here


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="healthcare_claims_etl",
    default_args=default_args,
    description="Daily ETL pipeline for healthcare claims processing",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcare", "etl"],
) as dag:

    with TaskGroup(group_id="extract_stage") as extract_stage:
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
        extract_claims_task >> extract_providers_task  # ordering optional; they run in parallel

    transform_task = PythonOperator(
        task_id="transform_join",
        python_callable=transform_join,
        provide_context=True,
    )

    with TaskGroup(group_id="load_stage") as load_stage:
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
        load_warehouse_task >> refresh_bi_task  # they can also run in parallel; adjust as needed

    # Define overall dependencies
    extract_stage >> transform_task >> load_stage