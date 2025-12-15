import hashlib
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="healthcare_claims_processing_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "etl", "claims"],
) as dag:

    @task
    def extract_claims():
        """Extract patient claims data from CSV file."""
        # Simulate reading from CSV
        claims_data = [
            {"claim_id": "C001", "patient_id": "P123", "provider_id": "PRV001", "procedure_code": "99213", "amount": 150.00},
            {"claim_id": "C002", "patient_id": "P456", "provider_id": "PRV002", "procedure_code": "99214", "amount": 200.00},
        ]
        return json.dumps(claims_data)

    @task
    def extract_providers():
        """Extract provider data from CSV file."""
        # Simulate reading from CSV
        providers_data = [
            {"provider_id": "PRV001", "provider_name": "Dr. Smith", "specialty": "Family Medicine"},
            {"provider_id": "PRV002", "provider_name": "Dr. Jones", "specialty": "Internal Medicine"},
        ]
        return json.dumps(providers_data)

    @task
    def transform_join(claims_json, providers_json):
        """Join claims and provider data, anonymize PII, and calculate risk scores."""
        claims = json.loads(claims_json)
        providers = json.loads(providers_json)
        
        # Create provider lookup
        provider_lookup = {p["provider_id"]: p for p in providers}
        
        transformed_data = []
        for claim in claims:
            # Anonymize patient_id by hashing
            patient_id_hash = hashlib.sha256(claim["patient_id"].encode()).hexdigest()[:16]
            
            # Get provider info
            provider = provider_lookup.get(claim["provider_id"], {})
            
            # Calculate risk score (simplified: based on procedure code and amount)
            risk_score = 0.0
            if claim["procedure_code"] in ["99213", "99214"]:
                risk_score = claim["amount"] / 100.0
            
            transformed_record = {
                "claim_id": claim["claim_id"],
                "patient_id_hash": patient_id_hash,
                "provider_id": claim["provider_id"],
                "provider_name": provider.get("provider_name"),
                "specialty": provider.get("specialty"),
                "procedure_code": claim["procedure_code"],
                "amount": claim["amount"],
                "risk_score": round(risk_score, 2),
            }
            transformed_data.append(transformed_record)
        
        return json.dumps(transformed_data)

    @task
    def load_warehouse(transformed_json):
        """Load transformed data to healthcare analytics warehouse tables."""
        transformed_data = json.loads(transformed_json)
        
        # Simulate loading to Postgres
        # In production, use PostgresHook or similar
        print(f"Loading {len(transformed_data)} records to warehouse...")
        for record in transformed_data:
            print(f"Inserting into claims_fact: {record['claim_id']}")
            print(f"Inserting into providers_dim: {record['provider_id']}")
        
        return f"Successfully loaded {len(transformed_data)} records"

    @task
    def refresh_bi():
        """Trigger refresh of BI dashboard tools including Power BI and Tableau."""
        # Simulate triggering BI refresh
        # In production, use API calls to Power BI and Tableau
        print("Triggering Power BI dataset refresh...")
        print("Triggering Tableau datasource refresh...")
        return "BI refresh triggered"

    # Define task groups for parallel execution stages
    with TaskGroup("extract_stage") as extract_stage:
        extract_claims_task = extract_claims()
        extract_providers_task = extract_providers()

    transform_join_task = transform_join(
        extract_claims_task, 
        extract_providers_task
    )

    with TaskGroup("load_stage") as load_stage:
        load_warehouse_task = load_warehouse(transform_join_task)
        refresh_bi_task = refresh_bi()

    # Define task dependencies
    extract_stage >> transform_join_task >> load_stage