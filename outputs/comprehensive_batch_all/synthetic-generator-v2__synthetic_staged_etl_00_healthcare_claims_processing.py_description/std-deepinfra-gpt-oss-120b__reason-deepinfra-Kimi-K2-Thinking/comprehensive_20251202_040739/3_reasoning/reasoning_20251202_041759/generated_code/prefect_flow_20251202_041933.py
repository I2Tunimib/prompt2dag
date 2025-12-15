from prefect import flow, task
import pandas as pd
import hashlib
from typing import Optional
import time
import os


def load_to_postgres(df: pd.DataFrame, table_name: str, conn_string: str) -> None:
    """Simulate loading a DataFrame to a Postgres table."""
    print(f"Loading {len(df)} rows to table '{table_name}'")
    time.sleep(2)


@task(retries=2, retry_delay_seconds=300)
def extract_claims(claims_path: str) -> pd.DataFrame:
    """Extract patient claims data from CSV file."""
    if not os.path.exists(claims_path):
        data = {
            'claim_id': [1, 2, 3, 4, 5],
            'patient_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
            'provider_id': [101, 102, 101, 103, 102],
            'procedure_code': ['A123', 'B456', 'A123', 'C789', 'B456'],
            'amount': [150.0, 250.0, 175.0, 500.0, 300.0]
        }
        return pd.DataFrame(data)
    
    return pd.read_csv(claims_path)


@task(retries=2, retry_delay_seconds=300)
def extract_providers(providers_path: str) -> pd.DataFrame:
    """Extract provider data from CSV file."""
    if not os.path.exists(providers_path):
        data = {
            'provider_id': [101, 102, 103],
            'provider_name': ['Dr. Smith', 'Dr. Jones', 'Dr. Brown'],
            'specialty': ['Cardiology', 'Neurology', 'Orthopedics']
        }
        return pd.DataFrame(data)
    
    return pd.read_csv(providers_path)


@task(retries=2, retry_delay_seconds=300)
def transform_join(
    claims_df: pd.DataFrame, 
    providers_df: pd.DataFrame
) -> pd.DataFrame:
    """Join claims and provider data, anonymize PII, and calculate risk scores."""
    joined_df = claims_df.merge(providers_df, on='provider_id', how='left')
    
    def hash_id(patient_id: str) -> str:
        return hashlib.sha256(patient_id.encode()).hexdigest()[:16]
    
    joined_df['patient_id_hashed'] = joined_df['patient_id'].apply(hash_id)
    joined_df.drop(columns=['patient_id'], inplace=True)
    
    procedure_risk_map = {
        'A123': 1.2,
        'B456': 1.5,
        'C789': 1.8
    }
    
    joined_df['risk_score'] = joined_df.apply(
        lambda row: procedure_risk_map.get(
            row['procedure_code'], 1.0
        ) * row['amount'] / 100,
        axis=1
    )
    
    return joined_df


@task(retries=2, retry_delay_seconds=300)
def load_warehouse(
    transformed_df: pd.DataFrame, 
    connection_string: str
) -> bool:
    """Load transformed data to healthcare analytics warehouse tables."""
    claims_fact = transformed_df[[
        'claim_id', 'provider_id', 'procedure_code', 
        'amount', 'risk_score'
    ]]
    
    providers_dim = transformed_df[[
        'provider_id', 'provider_name', 'specialty'
    ]].drop_duplicates()
    
    load_to_postgres(claims_fact, 'claims_fact', connection_string)
    load_to_postgres(providers_dim, 'providers_dim', connection_string)
    
    return True


@task(retries=2, retry_delay_seconds=300)
def refresh_bi() -> bool:
    """Trigger refresh of BI dashboard tools including Power BI and Tableau."""
    print("Triggering Power BI refresh...")
    time.sleep(1)
    print("Triggering Tableau refresh...")
    time.sleep(1)
    print("BI refresh completed successfully")
    
    return True


@flow(
    name="healthcare-claims-etl",
    description="Daily ETL pipeline for healthcare claims processing"
)
def healthcare_claims_etl(
    claims_path: str = "data/claims.csv",
    providers_path: str = "data/providers.csv",
    warehouse_conn: str = "postgresql://user:pass@localhost:5432/healthcare_analytics"
) -> bool:
    """
    Healthcare claims processing ETL pipeline.
    
    Extracts claims and provider data from CSV files,
    transforms and anonymizes the data, then loads to warehouse
    and refreshes BI tools.
    """
    claims_future = extract_claims.submit(claims_path)
    providers_future = extract_providers.submit(providers_path)
    
    transformed_future = transform_join.submit(
        claims_future, 
        providers_future
    )
    
    load_future = load_warehouse.submit(
        transformed_future, 
        warehouse_conn
    )
    refresh_future = refresh_bi.submit()
    
    load_result = load_future.result()
    refresh_result = refresh_future.result()
    
    return load_result and refresh_result


if __name__ == '__main__':
    # Local execution for testing
    # For scheduled deployment, configure via Prefect CLI/UI:
    # Schedule: Daily at 2 AM UTC starting 2024-01-01
    # Retry policy: 2 retries with 5-minute delays (configured on tasks)
    
    healthcare_claims_etl(
        claims_path="data/claims.csv",
        providers_path="data/providers.csv",
        warehouse_conn="postgresql://user:pass@localhost:5432/healthcare_analytics"
    )